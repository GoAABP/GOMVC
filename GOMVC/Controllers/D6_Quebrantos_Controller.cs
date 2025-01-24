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

public class D6_Quebrantos_Controller : Controller
{
    private readonly ILogger<D6_Quebrantos_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

    public D6_Quebrantos_Controller(ILogger<D6_Quebrantos_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance); // Handle special characters
    }

    // Process current files
    public async Task<IActionResult> D6_ProcessQuebrantos()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D6_Quebrantos_Bulk.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.");
        _logger.LogInformation("Process started.");

        var files = Directory.GetFiles(_filePath, "Quebrantos datos Cobranza_*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No files matching 'Quebrantos datos Cobranza_*.csv' found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D6_WriteLog(logBuilder.ToString(), logPath);
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

                await D6_BulkInsertQuebrantosData(sanitizedFilePath, logBuilder);

                await D6_ExecuteQuebrantosInsert(logBuilder, logPath);

                D6_MoveFilesToHistoric(file, logBuilder);
                D6_MoveFilesToHistoric(sanitizedFilePath, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}.");
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed.");
        _logger.LogInformation("Process completed.");
        await D6_WriteLog(logBuilder.ToString(), logPath);

        return Ok("Files processed successfully.");
    }

    // Process historic files
    public async Task<IActionResult> D6_ProcessHistoricQuebrantos()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D6_Quebrantos_Historic.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process started.");
        _logger.LogInformation("Historic process started.");

        var files = Directory.GetFiles(_filePath, "Quebrantos datos Cobranza_*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No historic files matching 'Quebrantos datos Cobranza_*.csv' found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D6_WriteLog(logBuilder.ToString(), logPath);
            return NotFound(errorLog);
        }

        foreach (var file in files)
        {
            logBuilder.AppendLine($"Processing file: {file}");

            try
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                var match = Regex.Match(fileName, @"Quebrantos datos Cobranza_(\d{2})(\d{2})(\d{4})");

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
                await D6_BulkInsertQuebrantosData(sanitizedFilePath, logBuilder);
                await D6_InsertHistoricQuebrantos(fechaGenerado, logBuilder, logPath);

                D6_MoveFilesToHistoric(file, logBuilder);
                D6_MoveFilesToHistoric(sanitizedFilePath, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}.");
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process completed.");
        _logger.LogInformation("Historic process completed.");
        await D6_WriteLog(logBuilder.ToString(), logPath);

        return Ok("Historic files processed successfully.");
    }

    // Bulk insert into staging table
    private async Task D6_BulkInsertQuebrantosData(string csvFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D6_Stage_Quebrantos;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D6_Stage_Quebrantos.");

                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{csvFilePath.Replace("\\", "\\\\")}' " +
                                          "INTO TABLE D6_Stage_Quebrantos " +
                                          "FIELDS TERMINATED BY ',' " +
                                          "ENCLOSED BY '\"' " +
                                          "LINES TERMINATED BY '\\n' " +
                                          "IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Bulk inserted data into D6_Stage_Quebrantos.");

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
    private async Task D6_ExecuteQuebrantosInsert(StringBuilder logBuilder, string logPath)
    {
        var sqlInsertCommand = @"
            INSERT INTO D6_Quebrantos (
                Operacion, Referencia, Nombre, Convenio, vFinancingtypeid, KVigente, KVencido, IntVencido,
                IVAIntVencido, IntVencidoCO, IVAIntVencidoCO, TotalQuebranto, PagosRealizados, SdoPendiente,
                IntXDevengar, SdoTotalXPagar, FechaQuebranto, UltPagoTeorico, UltimoPago, UltPagoApl,
                Gestor, nCommission, nCommTax, vMotive, FechaGenerado
            )
            SELECT 
                Operacion, Referencia, Nombre, Convenio, vFinancingtypeid, KVigente, KVencido, IntVencido,
                IVAIntVencido, IntVencidoCO, IVAIntVencidoCO, TotalQuebranto, PagosRealizados, SdoPendiente,
                IntXDevengar, SdoTotalXPagar,
                STR_TO_DATE(NULLIF(FechaQuebranto, ''), '%d/%m/%Y') AS FechaQuebranto,
                STR_TO_DATE(NULLIF(UltPagoTeorico, ''), '%d/%m/%Y') AS UltPagoTeorico,
                STR_TO_DATE(NULLIF(UltimoPago, ''), '%d/%m/%Y') AS UltimoPago,
                STR_TO_DATE(NULLIF(UltPagoApl, ''), '%d/%m/%Y') AS UltPagoApl,
                Gestor, nCommission, nCommTax, vMotive, NOW() AS FechaGenerado
            FROM D6_Stage_Quebrantos;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Inserted data successfully into D6_Quebrantos.");
                    _logger.LogInformation("Inserted data successfully into D6_Quebrantos.");
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

    private async Task D6_InsertHistoricQuebrantos(DateTime fechaGenerado, StringBuilder logBuilder, string logPath)
    {
        var sqlInsertCommand = @"
            INSERT INTO D6_Quebrantos (
                Operacion, Referencia, Nombre, Convenio, vFinancingtypeid, KVigente, KVencido, IntVencido,
                IVAIntVencido, IntVencidoCO, IVAIntVencidoCO, TotalQuebranto, PagosRealizados, SdoPendiente,
                IntXDevengar, SdoTotalXPagar, FechaQuebranto, UltPagoTeorico, UltimoPago, UltPagoApl,
                Gestor, nCommission, nCommTax, vMotive, FechaGenerado
            )
            SELECT 
                Operacion, Referencia, Nombre, Convenio, vFinancingtypeid, KVigente, KVencido, IntVencido,
                IVAIntVencido, IntVencidoCO, IVAIntVencidoCO, TotalQuebranto, PagosRealizados, SdoPendiente,
                IntXDevengar, SdoTotalXPagar,
                STR_TO_DATE(NULLIF(FechaQuebranto, ''), '%d/%m/%Y') AS FechaQuebranto,
                STR_TO_DATE(NULLIF(UltPagoTeorico, ''), '%d/%m/%Y') AS UltPagoTeorico,
                STR_TO_DATE(NULLIF(UltimoPago, ''), '%d/%m/%Y') AS UltimoPago,
                STR_TO_DATE(NULLIF(UltPagoApl, ''), '%d/%m/%Y') AS UltPagoApl,
                Gestor, nCommission, nCommTax, vMotive, @FechaGenerado
            FROM D6_Stage_Quebrantos;";

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
                    logBuilder.AppendLine("Inserted historic data successfully into D6_Quebrantos.");
                    _logger.LogInformation("Inserted historic data successfully into D6_Quebrantos.");
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

    // Validate the file for invalid characters or errors
    private void ValidateFile(string filePath, StringBuilder logBuilder)
    {
        try
        {
            using (var reader = new StreamReader(filePath, Encoding.UTF8))
            {
                int lineNumber = 0;
                while (!reader.EndOfStream)
                {
                    string line = reader.ReadLine();
                    lineNumber++;

                    // Check for invalid characters
                    if (line.Contains("�"))
                    {
                        logBuilder.AppendLine($"Warning: Invalid character found on line {lineNumber}: {line}");
                    }

                    // Add any other validations as needed
                }
            }

            logBuilder.AppendLine("File validation completed successfully.");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error during file validation: {ex.Message}");
            throw;
        }
    }

    // File conversion to UTF-8
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

    // Preprocessing CSV files
    private string PreprocessCsvFile(string inputFilePath, StringBuilder logBuilder)
    {
        var sanitizedFilePath = Path.Combine(
            Path.GetDirectoryName(inputFilePath)!,
            Path.GetFileNameWithoutExtension(inputFilePath) + "_sanitized.csv"
        );

        try
        {
            using (var reader = new StreamReader(inputFilePath))
            using (var writer = new StreamWriter(sanitizedFilePath))
            {
                string? line;
                bool headerProcessed = false;

                while ((line = reader.ReadLine()) != null)
                {
                    // Handle header row separately
                    if (!headerProcessed)
                    {
                        writer.WriteLine(line); // Write header as-is
                        headerProcessed = true;
                        continue;
                    }

                    // Split the line to check the first column
                    var columns = line.Split(',');

                    // Check if the first column equals '999999999'
                    if (columns[0].Trim() == "999999999")
                    {
                        logBuilder.AppendLine("Delimiter '999999999' found. Stopping further processing.");
                        break; // Stop processing further rows
                    }

                    // Write valid rows to the sanitized file
                    writer.WriteLine(line);
                }
            }

            logBuilder.AppendLine($"File successfully sanitized and saved to: {sanitizedFilePath}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error during preprocessing: {ex.Message}");
            throw;
        }

        return sanitizedFilePath;
    }

    private void D6_MoveFilesToHistoric(string filePath, StringBuilder logBuilder)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");
        var historicFilePath = Path.Combine(_historicFilePath, $"{Path.GetFileNameWithoutExtension(filePath)}_{timestamp}{Path.GetExtension(filePath)}");

        System.IO.File.Move(filePath, historicFilePath);
        logBuilder.AppendLine($"Moved file to historic: {historicFilePath}");
    }

    private async Task D6_WriteLog(string logContent, string logPath)
    {
        await System.IO.File.WriteAllTextAsync(logPath, logContent);
        _logger.LogInformation($"Log written to: {logPath}");
    }
}
