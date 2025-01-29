using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

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

    [HttpPost]
    public async Task<IActionResult> D6_ProcessQuebrantos()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D6_Quebrantos.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - D6 process started.");
        _logger.LogInformation("D6 process started.");

        var files = Directory.GetFiles(_filePath, "Quebrantos datos Cobranza_*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No files matching 'Quebrantos datos Cobranza_*.csv' found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D6_WriteLog(logBuilder.ToString(), logPath);
            return NotFound(errorLog);
        }

        var archiveFolder = Path.Combine(_historicFilePath, "Archive");
        var processedFolder = Path.Combine(_historicFilePath, "Processed");
        var errorFolder = Path.Combine(_historicFilePath, "Error");

        foreach (var file in files)
        {
            logBuilder.AppendLine($"Processing file: {file}");
            try
            {
                var convertedFilePath = D6_ConvertToUTF8WithBOM(file);
                logBuilder.AppendLine($"Converted file to UTF-8 with BOM: {convertedFilePath}");

                var sanitizedFilePath = D6_PreprocessCsvFile(convertedFilePath, logBuilder);
                logBuilder.AppendLine($"Sanitized file: {sanitizedFilePath}");

                await D6_BulkInsertToStage(sanitizedFilePath, logBuilder);
                await D6_InsertToFinalTable(logBuilder);

                D6_MoveFile(file, archiveFolder, logBuilder);
                D6_MoveFile(convertedFilePath, processedFolder, logBuilder);
                D6_MoveFile(sanitizedFilePath, processedFolder, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}");
                D6_MoveFile(file, errorFolder, logBuilder);
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - D6 process completed.");
        _logger.LogInformation("D6 process completed.");
        await D6_WriteLog(logBuilder.ToString(), logPath);

        D6_MoveLogToHistoric(logPath, Path.Combine(_historicFilePath, "Logs"));
        return Ok("D6 files processed successfully.");
    }

    private string D6_PreprocessCsvFile(string inputFilePath, StringBuilder logBuilder)
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
                    if (!headerProcessed)
                    {
                        writer.WriteLine(line); // Write the header row as-is
                        headerProcessed = true;
                        continue;
                    }

                    var columns = line.Split(',');

                    // Stop processing if the stopper is found in the first column
                    if (columns[0].Trim() == "999999999")
                    {
                        logBuilder.AppendLine("Stopper '999999999' found in the first column. Stopping further processing.");
                        break;
                    }

                    writer.WriteLine(line);
                }
            }

            logBuilder.AppendLine($"File sanitized successfully: {sanitizedFilePath}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error during file sanitization: {ex.Message}");
            throw;
        }

        return sanitizedFilePath;
    }

    private async Task D6_BulkInsertToStage(string csvFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    // Truncate the staging table
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D6_Stage_Quebrantos;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D6_Stage_Quebrantos.");

                    // Bulk insert into staging table
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

    private async Task D6_InsertToFinalTable(StringBuilder logBuilder)
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
                    var rowsAffected = await command.ExecuteNonQueryAsync();

                    logBuilder.AppendLine($"Inserted {rowsAffected} rows into D6_Quebrantos.");
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

    private void D6_MoveFile(string sourceFilePath, string destinationFolder, StringBuilder logBuilder)
    {
        try
        {
            if (!Directory.Exists(destinationFolder))
            {
                Directory.CreateDirectory(destinationFolder);
            }

            var destinationFilePath = Path.Combine(destinationFolder, Path.GetFileName(sourceFilePath));
            if (System.IO.File.Exists(destinationFilePath))
            {
                System.IO.File.Delete(destinationFilePath); // Overwrite existing file
            }

            System.IO.File.Move(sourceFilePath, destinationFilePath);
            logBuilder.AppendLine($"Moved file: {sourceFilePath} -> {destinationFilePath}");
            _logger.LogInformation($"Moved file: {sourceFilePath} -> {destinationFilePath}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error moving file {sourceFilePath} to {destinationFolder}: {ex.Message}");
            _logger.LogError(ex, $"Error moving file {sourceFilePath} to {destinationFolder}");
        }
    }

    private async Task D6_WriteLog(string content, string logPath)
    {
        try
        {
            if (!Directory.Exists(Path.GetDirectoryName(logPath)))
            {
                Directory.CreateDirectory(Path.GetDirectoryName(logPath)!);
            }

            await System.IO.File.WriteAllTextAsync(logPath, content);
            _logger.LogInformation($"Log written to: {logPath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error writing log to {logPath}");
            throw;
        }
    }

    private void D6_MoveLogToHistoric(string logPath, string historicLogsFolder)
    {
        try
        {
            if (!System.IO.File.Exists(logPath))
            {
                _logger.LogWarning($"Log file does not exist: {logPath}");
                return;
            }

            if (!Directory.Exists(historicLogsFolder))
            {
                Directory.CreateDirectory(historicLogsFolder);
            }

            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var logFileName = Path.GetFileNameWithoutExtension(logPath) + $"_{timestamp}" + Path.GetExtension(logPath);
            var destinationFilePath = Path.Combine(historicLogsFolder, logFileName);

            System.IO.File.Move(logPath, destinationFilePath);
            _logger.LogInformation($"Log file moved to historic folder: {destinationFilePath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error moving log file to historic folder: {ex.Message}");
            throw;
        }
    }

    [HttpPost]
    public async Task<IActionResult> D6_ProcessHistoricQuebrantos()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D6_Historic_Quebrantos.log";
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

        var archiveFolder = Path.Combine(_historicFilePath, "Archive");
        var processedFolder = Path.Combine(_historicFilePath, "Processed");
        var errorFolder = Path.Combine(_historicFilePath, "Error");

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
                    D6_MoveFile(file, errorFolder, logBuilder);
                    continue;
                }

                var day = int.Parse(match.Groups[1].Value);
                var month = int.Parse(match.Groups[2].Value);
                var year = int.Parse(match.Groups[3].Value);
                var fechaGenerado = new DateTime(year, month, day);

                logBuilder.AppendLine($"Parsed FechaGenerado: {fechaGenerado} for file: {file}");

                var convertedFilePath = D6_ConvertToUTF8WithBOM(file);
                logBuilder.AppendLine($"Converted file to UTF-8 with BOM: {convertedFilePath}");

                var sanitizedFilePath = D6_PreprocessCsvFile(convertedFilePath, logBuilder);
                logBuilder.AppendLine($"Sanitized file: {sanitizedFilePath}");

                await D6_BulkInsertToStage(sanitizedFilePath, logBuilder);
                await D6_InsertHistoricData(fechaGenerado, logBuilder);

                D6_MoveFile(file, archiveFolder, logBuilder);
                D6_MoveFile(convertedFilePath, processedFolder, logBuilder);
                D6_MoveFile(sanitizedFilePath, processedFolder, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}");
                D6_MoveFile(file, errorFolder, logBuilder);
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process completed.");
        _logger.LogInformation("Historic process completed.");
        await D6_WriteLog(logBuilder.ToString(), logPath);

        D6_MoveLogToHistoric(logPath, Path.Combine(_historicFilePath, "Logs"));
        return Ok("Historic files processed successfully.");
    }

    private async Task D6_InsertHistoricData(DateTime fechaGenerado, StringBuilder logBuilder)
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
                    var rowsAffected = await command.ExecuteNonQueryAsync();

                    logBuilder.AppendLine($"Inserted {rowsAffected} rows into D6_Quebrantos.");
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

    private void D6_ValidateFile(string filePath, StringBuilder logBuilder)
    {
        try
        {
            using (var reader = new StreamReader(filePath, Encoding.UTF8))
            {
                int lineNumber = 0;
                while (!reader.EndOfStream)
                {
                    string? line = reader.ReadLine();
                    lineNumber++;

                    if (string.IsNullOrWhiteSpace(line))
                    {
                        logBuilder.AppendLine($"Warning: Empty line found at line {lineNumber}.");
                        continue;
                    }

                    if (line.Contains("�"))
                    {
                        logBuilder.AppendLine($"Warning: Invalid character found at line {lineNumber}: {line}");
                    }

                    var columns = line.Split(',');
                    if (columns.Length < 10) // Example: Check for at least 10 columns
                    {
                        logBuilder.AppendLine($"Error: Insufficient columns at line {lineNumber}: {line}");
                        throw new InvalidDataException($"Invalid data structure in file: {filePath}");
                    }
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

    private void D6_LogStep(string message, StringBuilder logBuilder)
    {
        var timestampedMessage = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}";
        logBuilder.AppendLine(timestampedMessage);
        _logger.LogInformation(timestampedMessage);
    }

    private string D6_ConvertToUTF8WithBOM(string filePath)
    {
        var newFilePath = Path.Combine(
            Path.GetDirectoryName(filePath)!,
            Path.GetFileNameWithoutExtension(filePath) + "_utf8" + Path.GetExtension(filePath)
        );

        try
        {
            using (var reader = new StreamReader(filePath, Encoding.GetEncoding("Windows-1252")))
            using (var writer = new StreamWriter(newFilePath, false, new UTF8Encoding(true)))
            {
                while (!reader.EndOfStream)
                {
                    writer.WriteLine(reader.ReadLine());
                }
            }

            _logger.LogInformation($"File successfully converted to UTF-8 with BOM: {newFilePath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error converting file to UTF-8 with BOM: {filePath}");
            throw;
        }

        return newFilePath;
    }

}