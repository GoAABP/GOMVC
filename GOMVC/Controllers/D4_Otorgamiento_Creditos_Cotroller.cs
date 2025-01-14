using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

public class D4_Otorgamiento_Creditos_Controller : Controller
{
    private readonly ILogger<D4_Otorgamiento_Creditos_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

    public D4_Otorgamiento_Creditos_Controller(ILogger<D4_Otorgamiento_Creditos_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
    }

    // D4_ProcessOtorgamientoCreditos: Main entry point for processing
    [HttpPost("ProcessD4OtorgamientoCreditos")]
    public async Task<IActionResult> D4_ProcessOtorgamientoCreditos()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadOtorgamientoCreditos.log";
        var logBuilder = new StringBuilder();
        var startLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.";
        logBuilder.AppendLine(startLog);
        _logger.LogInformation(startLog);

        // Check for files in the directory
        var files = Directory.GetFiles(_filePath, "BARTURO*.csv");
        if (files.Length == 0)
        {
            var errorLog = "File not found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D4_WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }

        var file = files[0];
        logBuilder.AppendLine("File found.");
        try
        {
            var textFilePath = await D4_ConvertOtorgamientoCreditosCsvToText(file, logBuilder);
            await D4_BulkInsertOtorgamientoCreditosData(textFilePath, logBuilder);
            await D4_ExecuteInsert(logBuilder, logPath); // Direct insert from staging to final table
            D4_MoveFilesToHistoric(file, textFilePath, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await D4_WriteLog(logBuilder.ToString(), logPath);
            throw;
        }

        var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.";
        logBuilder.AppendLine(endLog);
        _logger.LogInformation(endLog);
        await D4_WriteLog(logBuilder.ToString(), logPath);
        return Ok("Activity processed successfully.");
    }

    // D4_ConvertOtorgamientoCreditosCsvToText: Convert CSV to text file
    private async Task<string> D4_ConvertOtorgamientoCreditosCsvToText(string csvFilePath, StringBuilder logBuilder)
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        var textFilePath = Path.ChangeExtension(csvFilePath, ".txt");
        var sb = new StringBuilder();

        try
        {
            using (var reader = new StreamReader(csvFilePath, Encoding.GetEncoding("windows-1252")))
            {
                string line;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    var columns = line.Split(',');
                    if (columns[0] == "0")
                    {
                        var errorLog = "Encountered 0 in the first column. Stopping processing.";
                        logBuilder.AppendLine(errorLog);
                        _logger.LogError(errorLog);
                        break;
                    }

                    // Normalize line and handle commas inside quotes
                    var processedLine = line.Normalize(NormalizationForm.FormC);
                    processedLine = Regex.Replace(processedLine, ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", "\n");
                    processedLine = Regex.Replace(processedLine, @"(\d{2})/(\d{2})/(\d{4})", "$3-$2-$1");

                    sb.AppendLine(processedLine);
                }
            }

            using (var writer = new StreamWriter(textFilePath, false, Encoding.UTF8))
            {
                await writer.WriteAsync(sb.ToString());
            }

            var logMessage = $"Converted CSV to text for OtorgamientoCreditos: {textFilePath}";
            logBuilder.AppendLine(logMessage);
            _logger.LogInformation(logMessage);
        }
        catch (Exception ex)
        {
            var errorLog = $"Error during conversion: {ex.Message}";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(ex, errorLog);
            throw;
        }

        return textFilePath;
    }

    // D4_BulkInsertOtorgamientoCreditosData: Perform bulk insert into the staging table
    private async Task D4_BulkInsertOtorgamientoCreditosData(string textFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE Stage_Otorgamiento_Creditos;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    var logMessage = "Truncated table Stage_Otorgamiento_Creditos.";
                    logBuilder.AppendLine(logMessage);
                    _logger.LogInformation(logMessage);

                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{textFilePath.Replace("\\", "\\\\")}' " +
                                          "INTO TABLE Stage_Otorgamiento_Creditos " +
                                          "FIELDS TERMINATED BY '|' " +
                                          "ENCLOSED BY '\"' " +
                                          "LINES TERMINATED BY '\\n' " +
                                          "IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logMessage = "Bulk inserted data into Stage_Otorgamiento_Creditos.";
                    logBuilder.AppendLine(logMessage);
                    _logger.LogInformation(logMessage);

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error during bulk insert for OtorgamientoCreditos: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    throw;
                }
            }
        }
    }

    // D4_ExecuteInsert: Direct insert from the staging table to the final table
    private async Task D4_ExecuteInsert(StringBuilder logBuilder, string logPath)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var insertCommandText = @"
                        INSERT INTO D4_Otorgamiento_Creditos (
                            Id_Credito, Referencia, Nombre, Fecha_Apertura, F_Cobro, Id_Convenio, Convenio, Id_Sucursal, Sucursal,
                            Capital, Primer_Pago, Comision, IVA, Cobertura, IVA_Cobertura, Disposicion, Monto_Retenido, Pago_de_Deuda,
                            Comision_Financiada, IVA_Comision_Financiada, Solicitud, Vendedor, Nombre_Vendedor, TipoVendedor, vSupervisorId,
                            vSupName, Producto, Descripcion_Tasa, Persona, Plazo, Id_Producto, vCampaign, Tipo_de_Financiamiento,
                            vFinancingTypeId, vAliado
                        )
                        SELECT 
                            Id_Credito, Referencia, Nombre, Fecha_Apertura, F_Cobro, Id_Convenio, Convenio, Id_Sucursal, Sucursal,
                            Capital, Primer_Pago, Comision, IVA, Cobertura, IVA_Cobertura, Disposicion, Monto_Retenido, Pago_de_Deuda,
                            Comision_Financiada, IVA_Comision_Financiada, Solicitud, Vendedor, Nombre_Vendedor, TipoVendedor, vSupervisorId,
                            vSupName, Producto, Descripcion_Tasa, Persona, Plazo, Id_Producto, vCampaign, Tipo_de_Financiamiento,
                            vFinancingTypeId, vAliado
                        FROM Stage_Otorgamiento_Creditos;";

                    var insertCommand = new MySqlCommand(insertCommandText, connection, transaction);
                    await insertCommand.ExecuteNonQueryAsync();
                    var logMessage = "Inserted data from Stage_Otorgamiento_Creditos into D4_Otorgamiento_Creditos.";
                    logBuilder.AppendLine(logMessage);
                    _logger.LogInformation(logMessage);

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error during insert for OtorgamientoCreditos from staging to final table: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    await D4_WriteLog(logBuilder.ToString(), logPath);
                    throw;
                }
            }
        }
    }

    // D4_MoveFiles: Move files to historic folder after processing
    private void D4_MoveFilesToHistoric(string originalFilePath, string textFilePath, StringBuilder logBuilder)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");

        // Move original file
        var originalFileName = Path.GetFileNameWithoutExtension(originalFilePath);
        var originalExtension = Path.GetExtension(originalFilePath);
        var newOriginalFileName = $"{originalFileName}_{timestamp}{originalExtension}";
        var newOriginalFilePath = Path.Combine(_historicFilePath, newOriginalFileName);

        System.IO.File.Move(originalFilePath, newOriginalFilePath);
        var logMessage = $"Moved original file to historic folder: {newOriginalFilePath}";
        logBuilder.AppendLine(logMessage);
        _logger.LogInformation(logMessage);

        // Move converted file
        var textFileName = Path.GetFileNameWithoutExtension(textFilePath);
        var textExtension = Path.GetExtension(textFilePath);
        var newTextFileName = $"{textFileName}_{timestamp}{textExtension}";
        var newTextFilePath = Path.Combine(_historicFilePath, newTextFileName);

        System.IO.File.Move(textFilePath, newTextFilePath);
        logMessage = $"Moved converted file to historic folder: {newTextFilePath}";
        logBuilder.AppendLine(logMessage);
        _logger.LogInformation(logMessage);
    }

    // D4_WriteLog: Write logs to a log file after processing
    private async Task D4_WriteLog(string logContent, string logPath)
    {
        var historicLogPath = Path.Combine(_historicFilePath, $"BulkLoad_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.log");
        await System.IO.File.WriteAllTextAsync(historicLogPath, logContent, Encoding.UTF8);
    }
}
