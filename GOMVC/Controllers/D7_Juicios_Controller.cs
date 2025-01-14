using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

public class D7_Juicios_Controller : Controller
{
    private readonly ILogger<D7_Juicios_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    public D7_Juicios_Controller(ILogger<D7_Juicios_Controller> logger, IConfiguration configuration)
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    {
        _logger = logger;
        _configuration = configuration;
#pragma warning disable CS8601 // Possible null reference assignment.
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
#pragma warning restore CS8601 // Possible null reference assignment.
    }

    public async Task D7_ProcessJuicios()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadJuicios.log";
        var logBuilder = new StringBuilder();
        var startLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.";
        logBuilder.AppendLine(startLog);
        _logger.LogInformation(startLog);

        var files = Directory.GetFiles(_filePath, "Juicios*.csv");
        if (files.Length == 0)
        {
            var errorLog = "File not found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D7_WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }

        var file = files[0];
        logBuilder.AppendLine("File found.");

        try
        {
            var textFilePath = await D7_ConvertJuiciosCsvToText(file, logBuilder);
            await D7_BulkInsertJuicios(textFilePath, logBuilder);
            await D7_ExecuteInsertJuicios(logBuilder);
            D7_MoveFilesToHistoric(file, textFilePath, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await D7_WriteLog(logBuilder.ToString(), logPath);
            throw;
        }

        var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.";
        logBuilder.AppendLine(endLog);
        _logger.LogInformation(endLog);
        await D7_WriteLog(logBuilder.ToString(), logPath);
    }
    
    private async Task<string> D7_ConvertJuiciosCsvToText(string csvFilePath, StringBuilder logBuilder)
    {
        var textFilePath = Path.ChangeExtension(csvFilePath, ".txt");
        var sb = new StringBuilder();

        try
        {
            using (var reader = new StreamReader(csvFilePath, Encoding.GetEncoding("windows-1252")))
            {
                string line;
#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    var processedLine = line.Normalize(NormalizationForm.FormC);
                    processedLine = processedLine.Replace(",", "|"); // Replace commas with the delimiter
                    sb.AppendLine(processedLine);
                }
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
            }

            await System.IO.File.WriteAllTextAsync(textFilePath, sb.ToString(), Encoding.UTF8);
            var logMessage = $"Converted CSV to text: {textFilePath}";
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

    private async Task D7_BulkInsertJuicios(string textFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D7_Stage_Juicios;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D7_Stage_Juicios.");
                    _logger.LogInformation("Truncated table D7_Stage_Juicios.");

                        var loadCommandText = $"LOAD DATA LOCAL INFILE '{textFilePath.Replace("\\", "\\\\")}' " +
                          "INTO TABLE D7_Stage_Juicios" +
                          "FIELDS TERMINATED BY '|' " +
                          "ENCLOSED BY '\"' " +
                          "LINES TERMINATED BY '\\n' " +
                          "IGNORE 1 LINES;";

                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Bulk inserted data into D7_Stage_Juicios.");
                    _logger.LogInformation("Bulk inserted data into D7_Stage_Juicios.");

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error during bulk insert: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    throw;
                }
            }
        }
    }

    private async Task D7_ExecuteInsertJuicios(StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var insertCommandText = @"
                        INSERT INTO D7_Juicios (
                            Credito_MC, Decla, Descripcion_Cierre, Dias_Activo, Dias_Caducar, Estatus,
                            Etapa_Procesal, Expediente, Fecha_Actualizacion, Fecha_Carga_Inicial, Fecha_Cierre,
                            Fecha_Ultima_Act, Id_Juicio, Juzgado, Motivo_Cierre, Producto_MC, Tipo_Juicio, Validar_Cierre
                        )
                        SELECT
                            Credito_MC, Decla, Descripcion_Cierre, Dias_Activo, Dias_Caducar, Estatus,
                            Etapa_Procesal, Expediente,
                            CASE WHEN Fecha_Actualizacion = '0000-00-00' OR Fecha_Actualizacion = '' THEN NULL ELSE STR_TO_DATE(Fecha_Actualizacion, '%Y-%m-%d') END,
                            CASE WHEN Fecha_Carga_Inicial = '0000-00-00' OR Fecha_Carga_Inicial = '' THEN NULL ELSE STR_TO_DATE(Fecha_Carga_Inicial, '%Y-%m-%d') END,
                            CASE WHEN Fecha_Cierre = '0000-00-00' OR Fecha_Cierre = '' THEN NULL ELSE STR_TO_DATE(Fecha_Cierre, '%Y-%m-%d') END,
                            CASE WHEN Fecha_Ultima_Act = '0000-00-00' OR Fecha_Ultima_Act = '' THEN NULL ELSE STR_TO_DATE(Fecha_Ultima_Act, '%Y-%m-%d') END,
                            Id_Juicio, Juzgado, Motivo_Cierre, Producto_MC, Tipo_Juicio, Validar_Cierre
                        FROM D7_Stage_Juicios;";

                    var insertCommand = new MySqlCommand(insertCommandText, connection, transaction);
                    await insertCommand.ExecuteNonQueryAsync();

                    logBuilder.AppendLine("Insert successful.");
                    _logger.LogInformation("Insert successful.");

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error executing insert: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    throw;
                }
            }
        }
    }

    private void D7_MoveFilesToHistoric(string originalFilePath, string textFilePath, StringBuilder logBuilder)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");

        // Move original file
        var originalFileName = Path.GetFileNameWithoutExtension(originalFilePath);
        var originalExtension = Path.GetExtension(originalFilePath);
        var newOriginalFileName = $"{originalFileName}_{timestamp}{originalExtension}";
        var newOriginalFilePath = Path.Combine(_historicFilePath, newOriginalFileName);
        System.IO.File.Move(originalFilePath, newOriginalFilePath);
        logBuilder.AppendLine($"Moved original file to historic: {newOriginalFilePath}");
        _logger.LogInformation($"Moved original file to historic: {newOriginalFilePath}");

        // Move converted file
        var textFileName = Path.GetFileNameWithoutExtension(textFilePath);
        var textExtension = Path.GetExtension(textFilePath);
        var newTextFileName = $"{textFileName}_{timestamp}{textExtension}";
        var newTextFilePath = Path.Combine(_historicFilePath, newTextFileName);
        System.IO.File.Move(textFilePath, newTextFilePath);
        logBuilder.AppendLine($"Moved converted file to historic: {newTextFilePath}");
        _logger.LogInformation($"Moved converted file to historic: {newTextFilePath}");
    }

    private async Task D7_WriteLog(string logContent, string logPath)
    {
        var historicLogPath = Path.Combine(_historicFilePath, $"BulkLoad_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.log");
        if (System.IO.File.Exists(logPath))
        {
            System.IO.File.Move(logPath, historicLogPath);
        }
        await System.IO.File.WriteAllTextAsync(logPath, logContent);
    }
}