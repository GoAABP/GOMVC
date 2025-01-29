using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

public class D3_Aplicaciones_Pagos_Controller : Controller
{
    private readonly ILogger<D3_Aplicaciones_Pagos_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";

    public D3_Aplicaciones_Pagos_Controller(ILogger<D3_Aplicaciones_Pagos_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance); // Handle special characters
    }

    public async Task<IActionResult> D3_ProcessAplicacionPagos()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D3_Aplicaciones_Pagos.log";
        var archiveFolder = @"C:\Users\Go Credit\Documents\DATA\ARCHIVE";
        var processedFolder = @"C:\Users\Go Credit\Documents\DATA\PROCESSED";
        var errorFolder = @"C:\Users\Go Credit\Documents\DATA\ERROR";
        var logBuilder = new StringBuilder();
        var hasErrors = false;

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - D3 process started.");
        _logger.LogInformation("D3 process started.");

        // Get relevant files
        var files = Directory.GetFiles(_filePath, "Aplicacion de pagos por fecha de Aplica*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No files found matching the pattern.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D3_WriteLog(logBuilder.ToString(), logPath);
            return NotFound(errorLog);
        }

        foreach (var file in files)
        {
            logBuilder.AppendLine($"Processing file: {file}");
            try
            {
                // File Conversion to UTF-8 with BOM
                var convertedFilePath = D3_ConvertToUTF8WithBOM(file);
                logBuilder.AppendLine($"Converted file to UTF-8 with BOM: {convertedFilePath}");

                // Preprocessing: Stopper and sanitization
                var sanitizedFilePath = D3_PreprocessCsvFile(convertedFilePath, logBuilder);
                logBuilder.AppendLine($"Sanitized file: {sanitizedFilePath}");

                // Load data into staging table
                await D3_LoadDataToStage(sanitizedFilePath, logBuilder);

                // Insert validated data into the final table
                await D3_InsertValidatedData(logBuilder);

                // Move files to the archive and processed folders
                D3_MoveFile(file, archiveFolder);
                D3_MoveFile(convertedFilePath, processedFolder);
                D3_MoveFile(sanitizedFilePath, processedFolder);

                logBuilder.AppendLine($"File {file} processed successfully.");
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}");
                hasErrors = true;

                // Move problematic files to error folder
                D3_MoveFile(file, errorFolder);
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - D3 process completed.");
        _logger.LogInformation("D3 process completed.");
        await D3_WriteLog(logBuilder.ToString(), logPath);

        // Finalize response based on errors
        return hasErrors
            ? StatusCode(500, "D3 process completed with errors. Check the log for details.")
            : Ok("D3 process completed successfully.");
    }

    private string D3_PreprocessCsvFile(string inputFilePath, StringBuilder logBuilder)
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
                    // Write header row without validation
                    if (!headerProcessed)
                    {
                        writer.WriteLine(line);
                        headerProcessed = true;
                        continue;
                    }

                    // Check for stopper in the first column
                    var columns = line.Split(',');
                    if (columns[0].Trim() == "0")
                    {
                        logBuilder.AppendLine("Stopper '0' found in the first column. Stopping further processing.");
                        break;
                    }

                    writer.WriteLine(line);
                }
            }

            logBuilder.AppendLine($"File successfully sanitized: {sanitizedFilePath}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error during preprocessing: {ex.Message}");
            throw;
        }

        return sanitizedFilePath;
    }

    private string D3_ConvertToUTF8WithBOM(string filePath)
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

    private async Task D3_LoadDataToStage(string csvFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    // Truncate staging table
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D3_Stage_Aplicacion_Pagos;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D3_Stage_Aplicacion_Pagos.");

                    // Load data into staging table
                    var loadCommandText = "LOAD DATA LOCAL INFILE '" +
                        csvFilePath.Replace("\\", "\\\\") +
                        "' INTO TABLE D3_Stage_Aplicacion_Pagos " +
                        "FIELDS TERMINATED BY ',' " +
                        "ENCLOSED BY '\"' " +
                        "LINES TERMINATED BY '\\n' " +
                        "IGNORE 1 LINES;";

                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Loaded data into D3_Stage_Aplicacion_Pagos.");

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

    private async Task D3_InsertValidatedData(StringBuilder logBuilder)
    {
        var sqlInsertCommand = @"
            INSERT INTO D3_Aplicacion_Pagos (
                Id_Credito, Id_Convenio, Convenio, Referencia, Id_Pago, Nombre_Cliente, Financiamiento,
                Origen_de_Movimiento, Fecha_Pago, Fecha_Aplicacion, Fecha_Deposito, Status, Pago, Capital,
                Interes, IVA_Int, Comision_Financiada, IVA_Comision_Financ, Moratorios, IVA_Mora, Pago_Tardio,
                IVA_PagoTardio, Recuperacion, IVA_Recup, Com_Liquidacion, IVA_Com_Liquidacion, Retencion_X_Admon,
                IVA_Retencion_X_Admon, Pago_Exceso, Gestor, Forma_de_pago, vMotive
            )
            SELECT 
                Id_Credito, Id_Convenio, Convenio, Referencia, Id_Pago, Nombre_Cliente, Financiamiento,
                Origen_de_Movimiento,
                STR_TO_DATE(NULLIF(Fecha_Pago, ''), '%d/%m/%Y') AS Fecha_Pago,
                STR_TO_DATE(NULLIF(Fecha_Aplicacion, ''), '%d/%m/%Y') AS Fecha_Aplicacion,
                STR_TO_DATE(NULLIF(Fecha_Deposito, ''), '%d/%m/%Y') AS Fecha_Deposito,
                Status, Pago, Capital, Interes, IVA_Int, Comision_Financiada, IVA_Comision_Financ, Moratorios, IVA_Mora, 
                Pago_Tardio, IVA_PagoTardio, Recuperacion, IVA_Recup, Com_Liquidacion, IVA_Com_Liquidacion, 
                Retencion_X_Admon, IVA_Retencion_X_Admon, Pago_Exceso, Gestor, Forma_de_pago, vMotive
            FROM D3_Stage_Aplicacion_Pagos
            WHERE NOT EXISTS (
                SELECT 1 FROM D3_Aplicacion_Pagos WHERE D3_Aplicacion_Pagos.Id_Pago = D3_Stage_Aplicacion_Pagos.Id_Pago
            );";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Inserted validated data into D3_Aplicacion_Pagos.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error inserting data: {ex.Message}");
                    _logger.LogError(ex, "Error inserting data.");
                    throw;
                }
            }
        }
    }

    private void D3_MoveFile(string sourceFilePath, string destinationFolder)
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
            _logger.LogInformation($"File moved: {sourceFilePath} -> {destinationFilePath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Failed to move file: {sourceFilePath} -> {destinationFolder}");
            throw;
        }
    }

    private async Task D3_WriteLog(string content, string logPath)
    {
        try
        {
            if (!Directory.Exists(Path.GetDirectoryName(logPath)))
            {
                Directory.CreateDirectory(Path.GetDirectoryName(logPath)!);
            }

            await System.IO.File.WriteAllTextAsync(logPath, content);
            _logger.LogInformation($"Log written to {logPath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to write log file.");
            throw;
        }
    }

    private void D3_MoveLogToHistoric(string logPath, string historicLogsFolder)
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
            _logger.LogInformation($"Log file moved to historic logs: {destinationFilePath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to move log file to historic logs.");
            throw;
        }
    }
}