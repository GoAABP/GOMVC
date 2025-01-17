using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using OfficeOpenXml;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

public class C5_Referencias_Controller : Controller
{
    private readonly ILogger<C5_Referencias_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

#pragma warning disable CS8618
    public C5_Referencias_Controller(ILogger<C5_Referencias_Controller> logger, IConfiguration configuration)
#pragma warning restore CS8618
    {
        _logger = logger;
        _configuration = configuration;
#pragma warning disable CS8601 // Possible null reference assignment.
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
#pragma warning restore CS8601 // Possible null reference assignment.
    }

    public async Task C5_ProcessReferencias()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadReferencias.log";
        var logBuilder = new StringBuilder();
        var startLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.";
        logBuilder.AppendLine(startLog);
        _logger.LogInformation(startLog);

        var files = Directory.GetFiles(_filePath, "Referencias_*.xlsx");
        if (files.Length == 0)
        {
            var errorLog = "File not found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await C5_WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }

        var file = files[0];
        logBuilder.AppendLine("File found.");

        try
        {
            var textFilePath = await C5_ConvertReferenciasExcelToText(file, logBuilder);
            await C5_BulkInsertReferencias(textFilePath, logBuilder);
            await C5_ExecuteInsertReferencias(logBuilder);
            C5_MoveFilesToHistoric(file, textFilePath, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await C5_WriteLog(logBuilder.ToString(), logPath);
            throw;
        }

        var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.";
        logBuilder.AppendLine(endLog);
        _logger.LogInformation(endLog);
        await C5_WriteLog(logBuilder.ToString(), logPath);
    }

    private async Task<string> C5_ConvertReferenciasExcelToText(string excelFilePath, StringBuilder logBuilder)
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
                var colCount = worksheet.Dimension.Columns;

                // Write the header
                for (int col = 1; col <= colCount; col++)
                {
                    sb.Append(worksheet.Cells[1, col].Text + delimiter);
                }
                sb.AppendLine();

                // Write the data rows
                for (int row = 2; row <= rowCount; row++)
                {
                    for (int col = 1; col <= colCount; col++)
                    {
                        var cellValue = worksheet.Cells[row, col].Text;
                        cellValue = cellValue.Replace("\r\n", " ").Replace("\n", " ").Replace("\r", " ");
                        sb.Append(cellValue + delimiter);
                    }
                    sb.AppendLine();
                }
            }

            await System.IO.File.WriteAllTextAsync(textFilePath, sb.ToString(), Encoding.UTF8);
            var logMessage = $"Converted Excel to text: {textFilePath}";
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

    private async Task C5_BulkInsertReferencias(string textFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE C5_Stage_Referencias;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table C5_Stage_Referencias.");
                    _logger.LogInformation("Truncated table C5_Stage_Referencias.");

                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{textFilePath.Replace("\\", "\\\\")}' " +
                                          "INTO TABLE C5_Stage_Referencias " +
                                          "FIELDS TERMINATED BY '|' " +
                                          "ENCLOSED BY '\"' " +
                                          "LINES TERMINATED BY '\\n' " +
                                          "IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Bulk inserted data into C5_Stage_Referencias.");
                    _logger.LogInformation("Bulk inserted data into C5_Stage_Referencias.");

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

    private async Task C5_ExecuteInsertReferencias(StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var insertCommandText = @"
                        INSERT INTO C5_Referencias (
                            Id_Persona, Referencia1, RELACION1, vphonenumber1,
                            Referencia2, RELACION2, vphonenumber2,
                            Referencia3, RELACION3, vphonenumber3,
                            Referencia4, RELACION4, vphonenumber4,
                            Referencia5, RELACION5, vphonenumber5,
                            Referencia6, RELACION6, vphonenumber6,
                            Referencia7, RELACION7, vphonenumber7,
                            Referencia8, RELACION8, vphonenumber8,
                            Referencia9, RELACION9, vphonenumber9,
                            Referencia10, RELACION10, vphonenumber10,
                            Referencia11, RELACION11, vphonenumber11,
                            Referencia12, RELACION12, vphonenumber12,
                            Referencia13, RELACION13, vphonenumber13,
                            Referencia14, RELACION14, vphonenumber14,
                            Referencia15, RELACION15, vphonenumber15,
                            Referencia16, RELACION16, vphonenumber16,
                            Referencia17, RELACION17, vphonenumber17,
                            Referencia18, RELACION18, vphonenumber18,
                            Referencia19, RELACION19, vphonenumber19,
                            Referencia20, RELACION20, vphonenumber20,
                            Referencia21, RELACION21, vphonenumber21,
                            Referencia22, RELACION22, vphonenumber22,
                            FechaGenerado
                        )
                        SELECT 
                            Id_Persona, Referencia1, RELACION1, vphonenumber1,
                            Referencia2, RELACION2, vphonenumber2,
                            Referencia3, RELACION3, vphonenumber3,
                            Referencia4, RELACION4, vphonenumber4,
                            Referencia5, RELACION5, vphonenumber5,
                            Referencia6, RELACION6, vphonenumber6,
                            Referencia7, RELACION7, vphonenumber7,
                            Referencia8, RELACION8, vphonenumber8,
                            Referencia9, RELACION9, vphonenumber9,
                            Referencia10, RELACION10, vphonenumber10,
                            Referencia11, RELACION11, vphonenumber11,
                            Referencia12, RELACION12, vphonenumber12,
                            Referencia13, RELACION13, vphonenumber13,
                            Referencia14, RELACION14, vphonenumber14,
                            Referencia15, RELACION15, vphonenumber15,
                            Referencia16, RELACION16, vphonenumber16,
                            Referencia17, RELACION17, vphonenumber17,
                            Referencia18, RELACION18, vphonenumber18,
                            Referencia19, RELACION19, vphonenumber19,
                            Referencia20, RELACION20, vphonenumber20,
                            Referencia21, RELACION21, vphonenumber21,
                            Referencia22, RELACION22, vphonenumber22,
                            NOW() AS FechaGenerado
                        FROM C5_Stage_Referencias;";

                    var insertCommand = new MySqlCommand(insertCommandText, connection, transaction);
                    var affectedRows = await insertCommand.ExecuteNonQueryAsync();

                    logBuilder.AppendLine($"Inserted {affectedRows} rows into C5_Referencias.");
                    _logger.LogInformation($"Inserted {affectedRows} rows into C5_Referencias.");

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error during insert into C5_Referencias: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    throw;
                }
            }
        }
    }

    private void C5_MoveFilesToHistoric(string originalFilePath, string textFilePath, StringBuilder logBuilder)
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

    private async Task C5_WriteLog(string logContent, string logPath)
    {
        var historicLogPath = Path.Combine(_historicFilePath, $"BulkLoad_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.log");
        if (System.IO.File.Exists(logPath))
        {
            // Move the existing log file to the historic folder
            System.IO.File.Move(logPath, historicLogPath);
        }
        // Write the new log content
        await System.IO.File.WriteAllTextAsync(logPath, logContent);
    }
}
