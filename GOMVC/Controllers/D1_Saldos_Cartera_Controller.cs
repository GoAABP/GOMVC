using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
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
#pragma warning disable CS8601 // Possible null reference assignment.
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
#pragma warning restore CS8601 // Possible null reference assignment.
    }

    public async Task D1_ProcessSaldosCartera()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadSaldosCartera.log";
        var logBuilder = new StringBuilder();
        var startLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.";
        logBuilder.AppendLine(startLog);
        _logger.LogInformation(startLog);

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
            await D1_BulkInsertSaldosCarteraData(file, logBuilder); // Directly process the CSV file
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

        var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.";
        logBuilder.AppendLine(endLog);
        _logger.LogInformation(endLog);
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
                    _logger.LogInformation("Truncated table D1_Stage_Saldos_Cartera.");

                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{csvFilePath.Replace("\\", "\\\\")}' " +
                                          "INTO TABLE D1_Stage_Saldos_Cartera " +
                                          "FIELDS TERMINATED BY ',' " +
                                          "ENCLOSED BY '\"' " +
                                          "LINES TERMINATED BY '\\n' " +
                                          "IGNORE 1 LINES;";

                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Bulk inserted data into D1_Stage_Saldos_Cartera.");
                    _logger.LogInformation("Bulk inserted data into D1_Stage_Saldos_Cartera.");

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error during bulk insert for SaldosCartera: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    throw;
                }
            }
        }
    }
    private async Task D1_ExecuteSaldosCarteraInsert(StringBuilder logBuilder, string logPath)
    {
        var sqlInsertCommand = @"
            INSERT INTO D1_Saldos_Cartera (
            Id_Solicitud, Id_Credito, Id_Persona, Referencia, Afiliado, Nombre, Monto, Comision, Intereses_Totales, Monto_Total, 
            Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, 
            Comision_Pagada, Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
            Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, Id_Convenio, Dependencia, 
            Primer_Pago_Teorico, Ultimo_Pago, Tipo_Financiamiento, Capital_Vigente, Capital_Vencido, Intereses_Vencidos, Vencido, 
            Sdo_Insoluto, Sdo_Total_c_ListasCobro, Sdo_Vencido_c_ListCobro, Estatus_Cartera, Estatus, Sucursal, Fecha_Desembolso, 
            Frecuencia, Primer_Pago_Real, Ultimo_Pago_c_ListaCobro, Ultimo_Pago_Aplicado, Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, 
            Periodos_Atraso, Pago, Monto_Ultimo_Pago, Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, Com_Vigente, Com_Vencida, 
            Clabe, Sig_Pago, Monto_Sig_Pago, vFondeador, Valida_Domi, vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm, RFC, vMotiveExt, 
            iPeriodsExt, vCommentExt, nRetencion, nJoPay, iMaxDays, vMaxDate, nLiquidate, nLiqPrin, nLiqInt, nLiqMor, nLiqCha, 
            nLiqPrinTran, nLiqIntTran, nLiqMorTran, nLiqChaTran, nLiqRetTran, vScoreBuro, vCollectStatus, nCAT, vOpTable, FechaGenerado
        )
        SELECT
            Id_Solicitud, Id_Credito, Id_Persona, Referencia, Afiliado, Nombre, Monto, Comision, Intereses_Totales, Monto_Total, 
            Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, 
            Comision_Pagada, Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
            Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, Id_Convenio, Dependencia, 
            STR_TO_DATE(NULLIF(Primer_Pago_Teorico, ''), '%d/%m/%Y'), 
            STR_TO_DATE(NULLIF(Ultimo_Pago, ''), '%d/%m/%Y'), 
            Tipo_Financiamiento, Capital_Vigente, Capital_Vencido, Intereses_Vencidos, Vencido, Sdo_Insoluto, Sdo_Total_c_ListasCobro, 
            Sdo_Vencido_c_ListCobro, Estatus_Cartera, Estatus, Sucursal, 
            STR_TO_DATE(NULLIF(Fecha_Desembolso, ''), '%d/%m/%Y'), Frecuencia, 
            STR_TO_DATE(NULLIF(Primer_Pago_Real, ''), '%d/%m/%Y'), 
            STR_TO_DATE(NULLIF(Ultimo_Pago_c_ListaCobro, ''), '%d/%m/%Y'), 
            STR_TO_DATE(NULLIF(Ultimo_Pago_Aplicado, ''), '%d/%m/%Y'), Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, Periodos_Atraso, 
            Pago, Monto_Ultimo_Pago, Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, Com_Vigente, Com_Vencida, Clabe, 
            STR_TO_DATE(NULLIF(Sig_Pago, ''), '%d/%m/%Y'), Monto_Sig_Pago, vFondeador, Valida_Domi, vAfiliateIdO, vAfiliateO, 
            Saldo_Retencion_Adm, RFC, vMotiveExt, iPeriodsExt, vCommentExt, nRetencion, nJoPay, iMaxDays, 
            STR_TO_DATE(NULLIF(vMaxDate, ''), '%d/%m/%Y'), nLiquidate, nLiqPrin, nLiqInt, nLiqMor, nLiqCha, nLiqPrinTran, 
            nLiqIntTran, nLiqMorTran, nLiqChaTran, nLiqRetTran, vScoreBuro, vCollectStatus, nCAT, vOpTable, NOW()
        FROM
            D1_Stage_Saldos_Cartera;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Executed SQL insert successfully.");
                    _logger.LogInformation("Executed SQL insert successfully.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error executing SQL insert: {ex.Message}");
                    _logger.LogError(ex, "Error executing SQL insert.");
                    await D1_WriteLog(logBuilder.ToString(), logPath);
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

        System.IO.File.Move(filePath, historicFilePath);
        logBuilder.AppendLine($"Moved file to historic: {historicFilePath}");
        _logger.LogInformation($"Moved file to historic: {historicFilePath}");
    }

    private async Task D1_WriteLog(string logContent, string logPath)
    {
        var historicLogPath = Path.Combine(_historicFilePath, $"BulkLoad_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.log");
        if (System.IO.File.Exists(logPath))
        {
#pragma warning disable CS8604 // Possible null reference argument.
            var uniqueLogPath = Path.Combine(
                Path.GetDirectoryName(logPath),
                $"{Path.GetFileNameWithoutExtension(logPath)}_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}{Path.GetExtension(logPath)}"
            );
#pragma warning restore CS8604 // Possible null reference argument.
            System.IO.File.Move(logPath, uniqueLogPath);
        }

        await System.IO.File.WriteAllTextAsync(logPath, logContent);
    }
}
