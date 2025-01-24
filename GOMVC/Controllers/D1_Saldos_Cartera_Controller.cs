using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;

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
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance); // Handle special characters
    }

    // Process current files
    public async Task<IActionResult> D1_ProcessSaldosCartera()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadSaldosCartera.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.");
        _logger.LogInformation("Process started.");

        var files = Directory.GetFiles(_filePath, "SaldosCarteraXConvenio*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No files matching 'SaldosCarteraXConvenio*.csv' found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D1_WriteLog(logBuilder.ToString(), logPath);
            return NotFound(errorLog);
        }

        foreach (var file in files)
        {
            logBuilder.AppendLine($"Processing file: {file}");

            try
            {
                var convertedFilePath = ConvertToUTF8WithBOM(file);
                logBuilder.AppendLine($"Converted file to UTF-8 with BOM: {convertedFilePath}");

                var sanitizedFilePath = PreprocessCsvFile(convertedFilePath, logBuilder);
                logBuilder.AppendLine($"Sanitized file: {sanitizedFilePath}");

                ValidateFile(sanitizedFilePath, logBuilder);

                await D1_BulkInsertSaldosCarteraData(sanitizedFilePath, logBuilder);

                await D1_ExecuteSaldosCarteraInsert(logBuilder, logPath);

                D1_MoveFilesToHistoric(file, logBuilder);
                D1_MoveFilesToHistoric(sanitizedFilePath, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}.");
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed.");
        _logger.LogInformation("Process completed.");
        await D1_WriteLog(logBuilder.ToString(), logPath);

        return Ok("Files processed successfully.");
    }

    // Process historic files
    public async Task<IActionResult> D1_ProcessHistoricSaldosCartera()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadHistoricSaldosCartera.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process started.");
        _logger.LogInformation("Historic process started.");

        var files = Directory.GetFiles(_filePath, "SaldosCarteraXConvenio_*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No historic files matching 'SaldosCarteraXConvenio_*.csv' found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D1_WriteLog(logBuilder.ToString(), logPath);
            return NotFound(errorLog);
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

                var sanitizedFilePath = PreprocessCsvFile(file, logBuilder);
                await D1_BulkInsertSaldosCarteraData(sanitizedFilePath, logBuilder);
                await D1_InsertHistoricSaldosCartera(fechaGenerado, logBuilder, logPath);
                D1_MoveFilesToHistoric(file, logBuilder);
                D1_MoveFilesToHistoric(sanitizedFilePath, logBuilder);
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

        return Ok("Historic files processed successfully.");
    }

    // Preprocess the CSV to fix malformed rows
    private string PreprocessCsvFile(string inputFilePath, StringBuilder logBuilder)
    {
        var sanitizedFilePath = Path.Combine(
            Path.GetDirectoryName(inputFilePath)!,
            Path.GetFileNameWithoutExtension(inputFilePath) + "_sanitized.csv"
        );

        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            BadDataFound = null, // Ignore malformed rows
            TrimOptions = TrimOptions.Trim, // Trim leading/trailing whitespace
        };

        try
        {
            using (var reader = new StreamReader(inputFilePath))
            using (var csvReader = new CsvReader(reader, config))
            using (var writer = new StreamWriter(sanitizedFilePath))
            using (var csvWriter = new CsvWriter(writer, config))
            {
                var records = csvReader.GetRecords<dynamic>();
                csvWriter.WriteRecords(records);
            }
            logBuilder.AppendLine($"Successfully sanitized file: {sanitizedFilePath}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error sanitizing file: {ex.Message}");
            throw;
        }

        return sanitizedFilePath;
    }

    // Bulk insert into staging table
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

    // Insert data from staging table to final table
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
                vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm, RFC, vMotiveExt, iPeriodsExt, vCommentExt, 
                nRetencion, nJoPay, iMaxDays, vMaxDate, nLiquidate, nLiqPrin, nLiqInt, nLiqMor, 
                nLiqCha, nLiqPrinTran, nLiqIntTran, nLiqMorTran, nLiqChaTran, nLiqRetTran, 
                vScoreBuro, vCollectStatus, nCAT, vOpTable, FechaGenerado
            )
            SELECT 
                Id_Solicitud, Id_Credito, Id_Persona, Referencia, Afiliado, Nombre, Monto, Comision, 
                Intereses_Totales, Monto_Total, Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, 
                IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, Comision_Pagada, 
                Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
                Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, 
                Id_Convenio, Dependencia, 
                STR_TO_DATE(NULLIF(Primer_Pago_Teorico, ''), '%d/%m/%Y') AS Primer_Pago_Teorico,
                STR_TO_DATE(NULLIF(Ultimo_Pago, ''), '%d/%m/%Y') AS Ultimo_Pago,
                Tipo_Financiamiento, Capital_Vigente, Capital_Vencido, Intereses_Vencidos, Vencido, 
                Sdo_Insoluto, Sdo_Total_c_ListasCobro, Sdo_Vencido_c_ListCobro, Estatus_Cartera, 
                Estatus, Sucursal, STR_TO_DATE(NULLIF(Fecha_Desembolso, ''), '%d/%m/%Y') AS Fecha_Desembolso,
                Frecuencia, STR_TO_DATE(NULLIF(Primer_Pago_Real, ''), '%d/%m/%Y') AS Primer_Pago_Real,
                STR_TO_DATE(NULLIF(Ultimo_Pago_c_ListaCobro, ''), '%d/%m/%Y') AS Ultimo_Pago_c_ListaCobro,
                STR_TO_DATE(NULLIF(Ultimo_Pago_Aplicado, ''), '%d/%m/%Y') AS Ultimo_Pago_Aplicado,
                Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, Periodos_Atraso, Pago, 
                Monto_Ultimo_Pago, Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, 
                Com_Vigente, Com_Vencida, Clabe, STR_TO_DATE(NULLIF(Sig_Pago, ''), '%d/%m/%Y') AS Sig_Pago,
                Monto_Sig_Pago, vFondeador, Valida_Domi, vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm,
                RFC, vMotiveExt, iPeriodsExt, vCommentExt, nRetencion, nJoPay, iMaxDays, 
                STR_TO_DATE(NULLIF(vMaxDate, ''), '%d/%m/%Y') AS vMaxDate, nLiquidate, nLiqPrin, 
                nLiqInt, nLiqMor, nLiqCha, nLiqPrinTran, nLiqIntTran, nLiqMorTran, 
                nLiqChaTran, nLiqRetTran, vScoreBuro, vCollectStatus, nCAT, vOpTable, NOW() AS FechaGenerado
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
                    logBuilder.AppendLine("Inserted data successfully into D1_Saldos_Cartera.");
                    _logger.LogInformation("Inserted data successfully into D1_Saldos_Cartera.");
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

    // Insert historic data into final table
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
                Id_Convenio, Dependencia, 
                STR_TO_DATE(NULLIF(Primer_Pago_Teorico, ''), '%d/%m/%Y') AS Primer_Pago_Teorico,
                STR_TO_DATE(NULLIF(Ultimo_Pago, ''), '%d/%m/%Y') AS Ultimo_Pago,
                Tipo_Financiamiento, Capital_Vigente, Capital_Vencido, Intereses_Vencidos, Vencido, 
                Sdo_Insoluto, Sdo_Total_c_ListasCobro, Sdo_Vencido_c_ListCobro, Estatus_Cartera, 
                Estatus, Sucursal, STR_TO_DATE(NULLIF(Fecha_Desembolso, ''), '%d/%m/%Y') AS Fecha_Desembolso,
                Frecuencia, STR_TO_DATE(NULLIF(Primer_Pago_Real, ''), '%d/%m/%Y') AS Primer_Pago_Real,
                STR_TO_DATE(NULLIF(Ultimo_Pago_c_ListaCobro, ''), '%d/%m/%Y') AS Ultimo_Pago_c_ListaCobro,
                STR_TO_DATE(NULLIF(Ultimo_Pago_Aplicado, ''), '%d/%m/%Y') AS Ultimo_Pago_Aplicado,
                Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, Periodos_Atraso, Pago, 
                Monto_Ultimo_Pago, Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, 
                Com_Vigente, Com_Vencida, Clabe, STR_TO_DATE(NULLIF(Sig_Pago, ''), '%d/%m/%Y') AS Sig_Pago,
                Monto_Sig_Pago, vFondeador, Valida_Domi, vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm,
                RFC, vMotiveExt, iPeriodsExt, vCommentExt, nRetencion, nJoPay, iMaxDays, 
                STR_TO_DATE(NULLIF(vMaxDate, ''), '%d/%m/%Y') AS vMaxDate, nLiquidate, nLiqPrin, 
                nLiqInt, nLiqMor, nLiqCha, nLiqPrinTran, nLiqIntTran, nLiqMorTran, 
                nLiqChaTran, nLiqRetTran, vScoreBuro, vCollectStatus, nCAT, vOpTable, @FechaGenerado
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

    // Validate file for invalid characters
    private void ValidateFile(string filePath, StringBuilder logBuilder)
    {
        using (var reader = new StreamReader(filePath, Encoding.UTF8))
        {
            while (!reader.EndOfStream)
            {
                string line = reader.ReadLine();
                if (line.Contains("�"))
                {
                    logBuilder.AppendLine($"Warning: Line contains invalid characters: {line}");
                }
            }
        }
    }

    // Convert file to UTF-8 with BOM
    private string ConvertToUTF8WithBOM(string filePath)
    {
        var newFilePath = Path.Combine(
            Path.GetDirectoryName(filePath)!,
            Path.GetFileNameWithoutExtension(filePath) + "_utf8" + Path.GetExtension(filePath)
        );

        using (var reader = new StreamReader(filePath, Encoding.GetEncoding("Windows-1252")))
        using (var writer = new StreamWriter(newFilePath, false, new UTF8Encoding(true)))
        {
            while (!reader.EndOfStream)
            {
                writer.WriteLine(reader.ReadLine());
            }
        }

        return newFilePath;
    }

    // Move processed files to historic folder
    private void D1_MoveFilesToHistoric(string filePath, StringBuilder logBuilder)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");
        var historicFilePath = Path.Combine(_historicFilePath, $"{Path.GetFileNameWithoutExtension(filePath)}_{timestamp}{Path.GetExtension(filePath)}");

        System.IO.File.Move(filePath, historicFilePath);
        logBuilder.AppendLine($"Moved file to historic: {historicFilePath}");
    }

    // Write logs to file
    private async Task D1_WriteLog(string logContent, string logPath)
    {
        await System.IO.File.WriteAllTextAsync(logPath, logContent);
        _logger.LogInformation($"Log written to: {logPath}");
    }
}
