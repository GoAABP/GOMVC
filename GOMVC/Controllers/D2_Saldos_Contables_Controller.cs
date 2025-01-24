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

public class D2_Saldos_Contables_Controller : Controller
{
    private readonly ILogger<D2_Saldos_Contables_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

    public D2_Saldos_Contables_Controller(ILogger<D2_Saldos_Contables_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance); // Handle special characters
    }

    // Process current files
    public async Task<IActionResult> D2_ProcessSaldosContables()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D2_Saldos_Contables_Bulk.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.");
        _logger.LogInformation("Process started.");

        var files = Directory.GetFiles(_filePath, "SDOSCONT*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No files matching 'SDOSCONT*.csv' found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D2_WriteLog(logBuilder.ToString(), logPath);
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

                await D2_BulkInsertSaldosContablesData(sanitizedFilePath, logBuilder);

                await D2_ExecuteSaldosContablesInsert(logBuilder, logPath);

                D2_MoveFilesToHistoric(file, logBuilder);
                D2_MoveFilesToHistoric(sanitizedFilePath, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}.");
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed.");
        _logger.LogInformation("Process completed.");
        await D2_WriteLog(logBuilder.ToString(), logPath);

        return Ok("Files processed successfully.");
    }

    // Process historic files
    public async Task<IActionResult> D2_ProcessHistoricSaldosContables()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D2_Saldos_Contables_Historic.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process started.");
        _logger.LogInformation("Historic process started.");

        var files = Directory.GetFiles(_filePath, "SDOSCONT_*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No historic files matching 'SDOSCONT_*.csv' found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D2_WriteLog(logBuilder.ToString(), logPath);
            return NotFound(errorLog);
        }

        foreach (var file in files)
        {
            logBuilder.AppendLine($"Processing file: {file}");

            try
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                var match = Regex.Match(fileName, @"SDOSCONT_(\d{2})(\d{2})(\d{4})");

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
                var fechaGenerado = new DateTime(year, month, day);

                logBuilder.AppendLine($"Parsed FechaGenerado: {fechaGenerado} for file: {file}");

                var sanitizedFilePath = PreprocessCsvFile(file, logBuilder);
                await D2_BulkInsertSaldosContablesData(sanitizedFilePath, logBuilder);
                await D2_InsertHistoricSaldosContables(fechaGenerado, logBuilder, logPath);

                D2_MoveFilesToHistoric(file, logBuilder);
                D2_MoveFilesToHistoric(sanitizedFilePath, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}.");
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process completed.");
        _logger.LogInformation("Historic process completed.");
        await D2_WriteLog(logBuilder.ToString(), logPath);

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
    private async Task D2_BulkInsertSaldosContablesData(string csvFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D2_Stage_Saldos_Contables;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D2_Stage_Saldos_Contables.");

                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{csvFilePath.Replace("\\", "\\\\")}' " +
                                          "INTO TABLE D2_Stage_Saldos_Contables " +
                                          "FIELDS TERMINATED BY ',' " +
                                          "ENCLOSED BY '\"' " +
                                          "LINES TERMINATED BY '\\n' " +
                                          "IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Bulk inserted data into D2_Stage_Saldos_Contables.");

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
    private async Task D2_ExecuteSaldosContablesInsert(StringBuilder logBuilder, string logPath)
    {
        var sqlInsertCommand = @"
            INSERT INTO D2_Saldos_Contables (
                Id_Credito, Referencia, Nombre, Id_Sucursal, Sucursal, Id_Convenio, Convenio, Financiamiento, 
                Estatus_Inicial, Estatus_Final, Fecha_Apertura, Fecha_Terminacion, Importe, Dias_Atraso, 
                Cuotas_Atrasadas, Periodos_Atraso, Pagos_Sostenidos, Pago, Frecuencia, Fecha_Ultimo_Pago, 
                Importe_Ultimo_Pago, Saldo_Inicial_Capital, Otorgado, Pagos, Ajuste_Cargo_Capital, Ajuste_Abono_Capital, 
                Saldo_Final_Capital, Calculo, Diferencia, Capital_Vigente, Capital_Vencido, Saldo_Inicial_Interes, 
                Devengamiento, Pagos_Interes, Ajuste_Cargo_Interes, Ajuste_Abono_Interes, Interes_No_Devengado, 
                Saldo_Final_Interes, Calculo_Interes, Diferencia_Interes, Interes_Devengado, IVA_Interes_Devengado, 
                Interes_No_DevengadoB, Fecha_Cartera_Vencida, Saldo_Contable, Saldo_Insoluto, Porc_Provision, 
                Reserva, nCAT, vOpTable, Status, FechaGenerado
            )
            SELECT 
                Id_Credito, Referencia, Nombre, Id_Sucursal, Sucursal, Id_Convenio, Convenio, Financiamiento, 
                Estatus_Inicial, Estatus_Final, 
                STR_TO_DATE(NULLIF(Fecha_Apertura, ''), '%d/%m/%Y') AS Fecha_Apertura, 
                STR_TO_DATE(NULLIF(Fecha_Terminacion, ''), '%d/%m/%Y') AS Fecha_Terminacion, 
                Importe, Dias_Atraso, Cuotas_Atrasadas, Periodos_Atraso, Pagos_Sostenidos, Pago, Frecuencia, 
                STR_TO_DATE(NULLIF(Fecha_Ultimo_Pago, ''), '%d/%m/%Y') AS Fecha_Ultimo_Pago, 
                Importe_Ultimo_Pago, Saldo_Inicial_Capital, Otorgado, Pagos, Ajuste_Cargo_Capital, Ajuste_Abono_Capital, 
                Saldo_Final_Capital, Calculo, Diferencia, Capital_Vigente, Capital_Vencido, Saldo_Inicial_Interes, 
                Devengamiento, Pagos_Interes, Ajuste_Cargo_Interes, Ajuste_Abono_Interes, Interes_No_Devengado, 
                Saldo_Final_Interes, Calculo_Interes, Diferencia_Interes, Interes_Devengado, IVA_Interes_Devengado, 
                Interes_No_DevengadoB, 
                STR_TO_DATE(NULLIF(Fecha_Cartera_Vencida, ''), '%d/%m/%Y') AS Fecha_Cartera_Vencida, 
                Saldo_Contable, Saldo_Insoluto, Porc_Provision, Reserva, nCAT, vOpTable, Status, 
                NOW() AS FechaGenerado
            FROM D2_Stage_Saldos_Contables;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Inserted data successfully into D2_Saldos_Contables.");
                    _logger.LogInformation("Inserted data successfully into D2_Saldos_Contables.");
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
    private async Task D2_InsertHistoricSaldosContables(DateTime fechaGenerado, StringBuilder logBuilder, string logPath)
    {
        var sqlInsertCommand = @"
            INSERT INTO D2_Saldos_Contables (
                Id_Credito, Referencia, Nombre, Id_Sucursal, Sucursal, Id_Convenio, Convenio, Financiamiento, 
                Estatus_Inicial, Estatus_Final, Fecha_Apertura, Fecha_Terminacion, Importe, Dias_Atraso, 
                Cuotas_Atrasadas, Periodos_Atraso, Pagos_Sostenidos, Pago, Frecuencia, Fecha_Ultimo_Pago, 
                Importe_Ultimo_Pago, Saldo_Inicial_Capital, Otorgado, Pagos, Ajuste_Cargo_Capital, Ajuste_Abono_Capital, 
                Saldo_Final_Capital, Calculo, Diferencia, Capital_Vigente, Capital_Vencido, Saldo_Inicial_Interes, 
                Devengamiento, Pagos_Interes, Ajuste_Cargo_Interes, Ajuste_Abono_Interes, Interes_No_Devengado, 
                Saldo_Final_Interes, Calculo_Interes, Diferencia_Interes, Interes_Devengado, IVA_Interes_Devengado, 
                Interes_No_DevengadoB, Fecha_Cartera_Vencida, Saldo_Contable, Saldo_Insoluto, Porc_Provision, 
                Reserva, nCAT, vOpTable, Status, FechaGenerado
            )
            SELECT 
                Id_Credito, Referencia, Nombre, Id_Sucursal, Sucursal, Id_Convenio, Convenio, Financiamiento, 
                Estatus_Inicial, Estatus_Final, 
                STR_TO_DATE(NULLIF(Fecha_Apertura, ''), '%d/%m/%Y') AS Fecha_Apertura, 
                STR_TO_DATE(NULLIF(Fecha_Terminacion, ''), '%d/%m/%Y') AS Fecha_Terminacion, 
                Importe, Dias_Atraso, Cuotas_Atrasadas, Periodos_Atraso, Pagos_Sostenidos, Pago, Frecuencia, 
                STR_TO_DATE(NULLIF(Fecha_Ultimo_Pago, ''), '%d/%m/%Y') AS Fecha_Ultimo_Pago, 
                Importe_Ultimo_Pago, Saldo_Inicial_Capital, Otorgado, Pagos, Ajuste_Cargo_Capital, Ajuste_Abono_Capital, 
                Saldo_Final_Capital, Calculo, Diferencia, Capital_Vigente, Capital_Vencido, Saldo_Inicial_Interes, 
                Devengamiento, Pagos_Interes, Ajuste_Cargo_Interes, Ajuste_Abono_Interes, Interes_No_Devengado, 
                Saldo_Final_Interes, Calculo_Interes, Diferencia_Interes, Interes_Devengado, IVA_Interes_Devengado, 
                Interes_No_DevengadoB, 
                STR_TO_DATE(NULLIF(Fecha_Cartera_Vencida, ''), '%d/%m/%Y') AS Fecha_Cartera_Vencida, 
                Saldo_Contable, Saldo_Insoluto, Porc_Provision, Reserva, nCAT, vOpTable, Status, 
                @FechaGenerado
            FROM D2_Stage_Saldos_Contables;";

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
                    logBuilder.AppendLine("Inserted historic data successfully into D2_Saldos_Contables.");
                    _logger.LogInformation("Inserted historic data successfully into D2_Saldos_Contables.");
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

    // Convert file encoding to UTF-8 with BOM
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

    // Move files to historic folder
    private void D2_MoveFilesToHistoric(string filePath, StringBuilder logBuilder)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");
        var historicFilePath = Path.Combine(_historicFilePath, $"{Path.GetFileNameWithoutExtension(filePath)}_{timestamp}{Path.GetExtension(filePath)}");

        System.IO.File.Move(filePath, historicFilePath);
        logBuilder.AppendLine($"Moved file to historic: {historicFilePath}");
    }

    // Write logs to a file
    private async Task D2_WriteLog(string logContent, string logPath)
    {
        await System.IO.File.WriteAllTextAsync(logPath, logContent);
        _logger.LogInformation($"Log written to: {logPath}");
    }
}
