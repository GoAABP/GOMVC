using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

public class D1_Saldos_Cartera_Controller : Controller
{
    private readonly ILogger<D1_Saldos_Cartera_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

    public D1_Saldos_Cartera_Controller(ILogger<D1_Saldos_Cartera_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
    }

    public async Task D1_ProcessSaldosCartera()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadSaldosCartera.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.");
        _logger.LogInformation("Process started.");

        var files = Directory.GetFiles(_filePath, "SaldosCarteraXConvenio*.csv");
        if (files.Length == 0)
        {
            var errorLog = "File not found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D1_WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }

        var file = files[0];
        logBuilder.AppendLine($"File found: {file}");

        try
        {
            await D1_BulkInsertSaldosCarteraData(file, logBuilder);
            await D1_ExecuteSaldosCarteraInsert(logBuilder, logPath);
            D1_MoveFilesToHistoric(file, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await D1_WriteLog(logBuilder.ToString(), logPath);
            throw;
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.");
        _logger.LogInformation("Process completed successfully.");
        await D1_WriteLog(logBuilder.ToString(), logPath);
    }

    public async Task D1_ProcessHistoricSaldosCartera()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadHistoricSaldosCartera.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process started.");
        _logger.LogInformation("Historic process started.");

        var files = Directory.GetFiles(_filePath, "SaldosCarteraXConvenio_*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No historic files found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D1_WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }

        foreach (var file in files)
        {
            logBuilder.AppendLine($"Processing file: {file}");
            try
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                var match = Regex.Match(fileName, @"SaldosCarteraXConvenio_(\d{2})(\d{2})(\d{4})(Morning|Afternoon|Night)");

                if (!match.Success)
                {
                    var errorLog = $"Invalid file name format: {fileName}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(errorLog);
                    continue;
                }

                var day = int.Parse(match.Groups[1].Value);
                var month = int.Parse(match.Groups[2].Value);
                var year = int.Parse(match.Groups[3].Value);
                var period = match.Groups[4].Value;

                var parsedDate = new DateTime(year, month, day);
                var defaultTime = period switch
                {
                    "Morning" => new TimeSpan(8, 0, 0),
                    "Afternoon" => new TimeSpan(14, 0, 0),
                    "Night" => new TimeSpan(20, 0, 0),
                    _ => throw new InvalidOperationException("Unknown period.")
                };
                var fechaGenerado = parsedDate.Add(defaultTime);

                logBuilder.AppendLine($"Parsed FechaGenerado: {fechaGenerado} for file: {file}");

                await D1_BulkInsertSaldosCarteraData(file, logBuilder);
                await D1_InsertHistoricSaldosCartera(fechaGenerado, logBuilder, logPath);
                D1_MoveFilesToHistoric(file, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}.");
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process completed.");
        _logger.LogInformation("Historic process completed.");
        await D1_WriteLog(logBuilder.ToString(), logPath);
    }

    private async Task D1_BulkInsertSaldosCarteraData(string csvFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D1_Stage_Saldos_Cartera;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D1_Stage_Saldos_Cartera.");

                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{csvFilePath.Replace("\\", "\\\\")}' " +
                                          "INTO TABLE D1_Stage_Saldos_Cartera " +
                                          "FIELDS TERMINATED BY ',' " +
                                          "ENCLOSED BY '\"' " +
                                          "LINES TERMINATED BY '\\n' " +
                                          "IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Bulk inserted data into D1_Stage_Saldos_Cartera.");

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during bulk insert: {ex.Message}");
                    _logger.LogError(ex, "Error during bulk insert.");
                    throw;
                }
            }
        }
    }

    private async Task D1_ExecuteSaldosCarteraInsert(StringBuilder logBuilder, string logPath)
    {
        var sqlInsertCommand = @"
        INSERT INTO D1_Saldos_Cartera (
            Id_Solicitud, Id_Credito, Id_Persona, Referencia, Afiliado, Nombre, Monto, Comision, 
            Intereses_Totales, Monto_Total, Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, 
            IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, Comision_Pagada, 
            Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
            Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, 
            Id_Convenio, Dependencia, Primer_Pago_Teorico, Ultimo_Pago, Tipo_Financiamiento, 
            Capital_Vigente, Capital_Vencido, Intereses_Vencidos, Vencido, Sdo_Insoluto, 
            Sdo_Total_c_ListasCobro, Sdo_Vencido_c_ListCobro, Estatus_Cartera, Estatus, 
            Sucursal, Fecha_Desembolso, Frecuencia, Primer_Pago_Real, Ultimo_Pago_c_ListaCobro, 
            Ultimo_Pago_Aplicado, Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, Periodos_Atraso, 
            Pago, Monto_Ultimo_Pago, Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, 
            Com_Vigente, Com_Vencida, Clabe, Sig_Pago, Monto_Sig_Pago, vFondeador, Valida_Domi, 
            vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm, RFC, vMotiveExt, iPeriodsExt, 
            vCommentExt, nRetencion, nJoPay, iMaxDays, vMaxDate, nLiquidate, nLiqPrin, nLiqInt, 
            nLiqMor, nLiqCha, nLiqPrinTran, nLiqIntTran, nLiqMorTran, nLiqChaTran, nLiqRetTran, 
            vScoreBuro, vCollectStatus, nCAT, vOpTable, FechaGenerado
        )
        SELECT 
            Id_Solicitud, Id_Credito, Id_Persona, Referencia, Afiliado, Nombre, Monto, Comision, 
            Intereses_Totales, Monto_Total, Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, 
            IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, Comision_Pagada, 
            Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
            Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, 
            Id_Convenio, Dependencia, STR_TO_DATE(NULLIF(Primer_Pago_Teorico, ''), '%d/%m/%Y'), 
            STR_TO_DATE(NULLIF(Ultimo_Pago, ''), '%d/%m/%Y'), Tipo_Financiamiento, Capital_Vigente, 
            Capital_Vencido, Intereses_Vencidos, Vencido, Sdo_Insoluto, Sdo_Total_c_ListasCobro, 
            Sdo_Vencido_c_ListCobro, Estatus_Cartera, Estatus, Sucursal, 
            STR_TO_DATE(NULLIF(Fecha_Desembolso, ''), '%d/%m/%Y'), Frecuencia, 
            STR_TO_DATE(NULLIF(Primer_Pago_Real, ''), '%d/%m/%Y'), 
            STR_TO_DATE(NULLIF(Ultimo_Pago_c_ListaCobro, ''), '%d/%m/%Y'), 
            STR_TO_DATE(NULLIF(Ultimo_Pago_Aplicado, ''), '%d/%m/%Y'), 
            Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, Periodos_Atraso, Pago, Monto_Ultimo_Pago, 
            Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, Com_Vigente, Com_Vencida, Clabe, 
            STR_TO_DATE(NULLIF(Sig_Pago, ''), '%d/%m/%Y'), Monto_Sig_Pago, vFondeador, Valida_Domi, 
            vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm, RFC, vMotiveExt, iPeriodsExt, vCommentExt, 
            nRetencion, nJoPay, iMaxDays, STR_TO_DATE(NULLIF(vMaxDate, ''), '%d/%m/%Y'), nLiquidate, 
            nLiqPrin, nLiqInt, nLiqMor, nLiqCha, nLiqPrinTran, nLiqIntTran, nLiqMorTran, nLiqChaTran, 
            nLiqRetTran, vScoreBuro, vCollectStatus, nCAT, vOpTable, NOW() AS FechaGenerado
        FROM D1_Stage_Saldos_Cartera;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Inserted data successfully.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during insert: {ex.Message}");
                    _logger.LogError(ex, "Error during insert.");
                    throw;
                }
            }
        }
    }

    private async Task D1_InsertHistoricSaldosCartera(DateTime fechaGenerado, StringBuilder logBuilder, string logPath)
    {
        var sqlInsertCommand = @"
            INSERT INTO D1_Saldos_Cartera (
                Id_Solicitud, Id_Credito, Id_Persona, Referencia, Afiliado, Nombre, Monto, Comision, 
                Intereses_Totales, Monto_Total, Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, 
                IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, Comision_Pagada, 
                Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
                Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, 
                Id_Convenio, Dependencia, Primer_Pago_Teorico, Ultimo_Pago, Tipo_Financiamiento, 
                Capital_Vigente, Capital_Vencido, Intereses_Vencidos, Vencido, Sdo_Insoluto, 
                Sdo_Total_c_ListasCobro, Sdo_Vencido_c_ListCobro, Estatus_Cartera, Estatus, 
                Sucursal, Fecha_Desembolso, Frecuencia, Primer_Pago_Real, Ultimo_Pago_c_ListaCobro, 
                Ultimo_Pago_Aplicado, Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, Periodos_Atraso, 
                Pago, Monto_Ultimo_Pago, Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, 
                Com_Vigente, Com_Vencida, Clabe, Sig_Pago, Monto_Sig_Pago, vFondeador, Valida_Domi, 
                vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm, RFC, vMotiveExt, iPeriodsExt, 
                vCommentExt, nRetencion, nJoPay, iMaxDays, vMaxDate, nLiquidate, nLiqPrin, nLiqInt, 
                nLiqMor, nLiqCha, nLiqPrinTran, nLiqIntTran, nLiqMorTran, nLiqChaTran, nLiqRetTran, 
                vScoreBuro, vCollectStatus, nCAT, vOpTable, FechaGenerado
            )
            SELECT 
                Id_Solicitud, Id_Credito, Id_Persona, Referencia, Afiliado, Nombre, Monto, Comision, 
                Intereses_Totales, Monto_Total, Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, 
                IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, Comision_Pagada, 
                Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
                Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, 
                Id_Convenio, Dependencia, STR_TO_DATE(NULLIF(Primer_Pago_Teorico, ''), '%d/%m/%Y'), 
                STR_TO_DATE(NULLIF(Ultimo_Pago, ''), '%d/%m/%Y'), Tipo_Financiamiento, Capital_Vigente, 
                Capital_Vencido, Intereses_Vencidos, Vencido, Sdo_Insoluto, Sdo_Total_c_ListasCobro, 
                Sdo_Vencido_c_ListCobro, Estatus_Cartera, Estatus, Sucursal, 
                STR_TO_DATE(NULLIF(Fecha_Desembolso, ''), '%d/%m/%Y'), Frecuencia, 
                STR_TO_DATE(NULLIF(Primer_Pago_Real, ''), '%d/%m/%Y'), 
                STR_TO_DATE(NULLIF(Ultimo_Pago_c_ListaCobro, ''), '%d/%m/%Y'), 
                STR_TO_DATE(NULLIF(Ultimo_Pago_Aplicado, ''), '%d/%m/%Y'), 
                Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, Periodos_Atraso, Pago, Monto_Ultimo_Pago, 
                Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, Com_Vigente, Com_Vencida, Clabe, 
                STR_TO_DATE(NULLIF(Sig_Pago, ''), '%d/%m/%Y'), Monto_Sig_Pago, vFondeador, Valida_Domi, 
                vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm, RFC, vMotiveExt, iPeriodsExt, vCommentExt, 
                nRetencion, nJoPay, iMaxDays, STR_TO_DATE(NULLIF(vMaxDate, ''), '%d/%m/%Y'), nLiquidate, 
                nLiqPrin, nLiqInt, nLiqMor, nLiqCha, nLiqPrinTran, nLiqIntTran, nLiqMorTran, nLiqChaTran, 
                nLiqRetTran, vScoreBuro, vCollectStatus, nCAT, vOpTable, @FechaGenerado
            FROM D1_Stage_Saldos_Cartera;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    command.Parameters.AddWithValue("@FechaGenerado", fechaGenerado);
                    await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Inserted historic data successfully into D1_Saldos_Cartera.");
                    _logger.LogInformation("Inserted historic data successfully into D1_Saldos_Cartera.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during historic insert: {ex.Message}");
                    _logger.LogError(ex, "Error during historic insert.");
                    throw;
                }
            }
        }
    }

    private void D1_MoveFilesToHistoric(string filePath, StringBuilder logBuilder)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");
        var fileName = Path.GetFileNameWithoutExtension(filePath);
        var fileExtension = Path.GetExtension(filePath);
        var newFileName = $"{fileName}_{timestamp}{fileExtension}";
        var historicFilePath = Path.Combine(_historicFilePath, newFileName);

        try
        {
            System.IO.File.Move(filePath, historicFilePath);
            logBuilder.AppendLine($"Moved file to historic: {historicFilePath}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error moving file to historic: {ex.Message}");
            _logger.LogError(ex, "Error moving file to historic.");
            throw;
        }
    }

    private async Task D1_WriteLog(string logContent, string logPath)
    {
        var logDirectory = Path.GetDirectoryName(logPath);
        var historicDirectory = _historicFilePath;

        // Generate a unique log file name based on the current timestamp
        var uniqueLogName = $"D1_Saldos_Cartera_Bulk_{DateTime.Now:yyyy-MM-dd_HH-mm}.log";
        var fullLogPath = Path.Combine(logDirectory!, uniqueLogName);

        // Check for any existing log files starting with "D1_Saldos_Cartera_Bulk"
        var existingLogs = Directory.GetFiles(logDirectory!, "D1_Saldos_Cartera_Bulk*.log");
        foreach (var existingLog in existingLogs)
        {
            var historicLogPath = Path.Combine(historicDirectory, Path.GetFileName(existingLog));
            System.IO.File.Move(existingLog, historicLogPath);
            _logger.LogInformation($"Moved existing log to historic: {historicLogPath}");
        }

        // Write the new log content to the unique log file
        await System.IO.File.WriteAllTextAsync(fullLogPath, logContent);
        _logger.LogInformation($"New log written to: {fullLogPath}");
    }
}
