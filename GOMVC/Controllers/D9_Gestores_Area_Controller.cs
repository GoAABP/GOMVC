using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using OfficeOpenXml;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

public class D9_Gestores_Area_Controller : Controller
{
    private readonly ILogger<D9_Gestores_Area_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    public D9_Gestores_Area_Controller(ILogger<D9_Gestores_Area_Controller> logger, IConfiguration configuration)
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    {
        _logger = logger;
        _configuration = configuration;
#pragma warning disable CS8601 // Possible null reference assignment.
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
#pragma warning restore CS8601 // Possible null reference assignment.
    }

    public async Task D9_ProcessGestoresArea()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadGestoresArea.log";
        var logBuilder = new StringBuilder();
        var startLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.";
        logBuilder.AppendLine(startLog);
        _logger.LogInformation(startLog);

        var files = Directory.GetFiles(_filePath, "GestoresArea_*.xlsx");
        if (files.Length == 0)
        {
            var errorLog = "File not found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D9_WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }

        var file = files[0];
        logBuilder.AppendLine("File found.");

        try
        {
            var textFilePath = await D9_ConvertGestoresAreaExcelToText(file, logBuilder);
            await D9_BulkInsertGestoresArea(textFilePath, logBuilder);
            await D9_ExecuteInsertGestoresArea(logBuilder);
            D9_MoveFilesToHistoric(file, textFilePath, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await D9_WriteLog(logBuilder.ToString(), logPath);
            throw;
        }

        var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.";
        logBuilder.AppendLine(endLog);
        _logger.LogInformation(endLog);
        await D9_WriteLog(logBuilder.ToString(), logPath);
    }

    private async Task<string> D9_ConvertGestoresAreaExcelToText(string excelFilePath, StringBuilder logBuilder)
    {
        var textFilePath = Path.ChangeExtension(excelFilePath, ".txt");
        var delimiter = "|";
        var sb = new StringBuilder();

        try
        {
            using (var package = new ExcelPackage(new FileInfo(excelFilePath)))
            {
                var worksheet = package.Workbook.Worksheets.First();
                var rowCount = worksheet.Dimension.Rows;

                // Write the data rows
                for (int row = 2; row <= rowCount; row++)
                {
                    var usuarioMC = worksheet.Cells[row, 1].Text.Trim();
                    var area = worksheet.Cells[row, 2].Text.Trim();
                    var estatus = worksheet.Cells[row, 3].Text.Trim();

                    sb.AppendLine($"{usuarioMC}{delimiter}{area}{delimiter}{estatus}");
                }
            }

            await System.IO.File.WriteAllTextAsync(textFilePath, sb.ToString(), Encoding.UTF8);
            logBuilder.AppendLine($"Converted Excel to text: {textFilePath}");
            _logger.LogInformation($"Converted Excel to text: {textFilePath}");
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

    private async Task D9_BulkInsertGestoresArea(string textFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D9_Stage_Gestores_Area;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D9_Stage_Gestores_Area.");
                    _logger.LogInformation("Truncated table D9_Stage_Gestores_Area.");


                        var loadCommandText = $"LOAD DATA LOCAL INFILE '{textFilePath.Replace("\\", "\\\\")}' " +
                          "INTO TABLE D9_Stage_Gestores_Area" +
                          "FIELDS TERMINATED BY '|' " +
                          "ENCLOSED BY '\"' " +
                          "LINES TERMINATED BY '\\n' " +
                          "IGNORE 1 LINES;";


                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Bulk inserted data into D9_Stage_Gestores_Area.");
                    _logger.LogInformation("Bulk inserted data into D9_Stage_Gestores_Area.");

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

    private async Task D9_ExecuteInsertGestoresArea(StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var insertCommandText = @"
                        INSERT INTO D9_Gestores_Area (
                            UsuarioMC, Area, Estatus, Perfil
                        )
                        SELECT 
                            UsuarioMC, Area, Estatus, Perfil
                        FROM D9_Stage_Gestores_Area;";

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

    private void D9_MoveFilesToHistoric(string originalFilePath, string textFilePath, StringBuilder logBuilder)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");

        try
        {
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
        catch (Exception ex)
        {
            var errorLog = $"Error while moving files to historic: {ex.Message}";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(ex, errorLog);
            throw;
        }
    }

    private async Task D9_WriteLog(string logContent, string logPath)
    {
        try
        {
            var historicLogPath = Path.Combine(_historicFilePath, $"BulkLoad_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.log");

            // Move the existing log file to the historic folder
            if (System.IO.File.Exists(logPath))
            {
                System.IO.File.Move(logPath, historicLogPath);
            }

            // Write the new log content
            await System.IO.File.WriteAllTextAsync(logPath, logContent, Encoding.UTF8);
            _logger.LogInformation($"Log file updated: {logPath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error while writing the log file: {logPath}. Error: {ex.Message}");
            throw;
        }
    }
}



