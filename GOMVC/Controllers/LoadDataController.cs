using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using OfficeOpenXml;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

public class LoadDataController : Controller
{
    private readonly ILogger<LoadDataController> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";
    private readonly string _logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadJuicios.log";
    private readonly string _historicLogPath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC LOGS";
    private static readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

    public LoadDataController(ILogger<LoadDataController> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
    }

    [HttpGet]
    public IActionResult Index()
    {
        var activities = new List<string>
        {
            "1-ActivityOne",
            "2-ActivityTwo",
            "3-ActivityThree",
            "4-ActivityFour",
            "5-ActivityFive",
            "6-ActivitySix",
            "7-Juicios",
            "8-ActivityEight"
        };
        return View(activities);
    }

    [HttpPost("HandleActivity")]
    public async Task<IActionResult> HandleActivity(string activityName)
    {
        try
        {
            await _semaphore.WaitAsync();
            try
            {
                switch (activityName.ToLower())
                {
                    case "7-juicios":
                        await Process7Juicios();
                        break;
                    // Add cases for other activities here
                    default:
                        _logger.LogError($"Unknown activity: {activityName}");
                        return BadRequest("Unknown activity.");
                }
            }
            finally
            {
                _semaphore.Release();
            }

            _logger.LogInformation("File processed successfully.");
            return Ok("File processed successfully.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing file.");
            return StatusCode(500, "Internal server error.");
        }
    }

    private async Task Process7Juicios()
    {
        var logBuilder = new StringBuilder();
        var todayDate = DateTime.Now.ToString("yyyy-MM-dd");
        var startLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.";

        logBuilder.AppendLine(startLog);
        _logger.LogInformation(startLog);

        var files = Directory.GetFiles(_filePath, "Re_Juicios_*.xlsx");

        if (files.Length == 0)
        {
            var errorLog = "File not found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await WriteLog(logBuilder.ToString());
            throw new FileNotFoundException(errorLog);
        }

        var file = files[0]; // Assuming you want to process the first matching file
        logBuilder.AppendLine("File found.");

        try
        {
            var textFilePath = Convert7JuiciosExcelToText(file, logBuilder);
            await BulkInsert7JuiciosData(textFilePath, logBuilder);
            await ExecuteSqlFile("C:\\Users\\Go Credit\\Documents\\DATA\\SQL\\InsertJuicios.sql", logBuilder);
            MoveFilesToHistoric(file, textFilePath, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await WriteLog(logBuilder.ToString());
            throw;
        }

        var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.";
        logBuilder.AppendLine(endLog);
        _logger.LogInformation(endLog);
        await WriteLog(logBuilder.ToString());
    }

    private string Convert7JuiciosExcelToText(string excelFilePath, StringBuilder logBuilder)
    {
        var textFilePath = Path.ChangeExtension(excelFilePath, ".txt");
        var sb = new StringBuilder();

        using (var package = new ExcelPackage(new FileInfo(excelFilePath)))
        {
            var worksheet = package.Workbook.Worksheets.First();
            var rowCount = worksheet.Dimension.Rows;
            var colCount = worksheet.Dimension.Columns;

            for (int row = 2; row <= rowCount; row++)
            {
                var rowValues = new List<string>();
                for (int col = 2; col <= colCount; col++) // Skip the first column
                {
                    var cellValue = worksheet.Cells[row, col].Text;
                    // Handle date conversion and line breaks here
                    if (DateTime.TryParse(cellValue, out DateTime dateValue))
                    {
                        cellValue = dateValue.ToString("yyyy-MM-dd HH:mm:ss");
                    }
                    else
                    {
                        // Attempt to parse custom date formats
                        cellValue = TryParseCustomDateFormats(cellValue);
                    }
                    rowValues.Add(cellValue.Replace("\r\n", " ").Replace("\n", " ").Replace("\r", " "));
                }
                sb.AppendLine(string.Join("|", rowValues));
            }
        }

        System.IO.File.WriteAllText(textFilePath, sb.ToString());
        var logMessage = $"Converted Excel to text for 7-Juicios: {textFilePath}";
        logBuilder.AppendLine(logMessage);
        _logger.LogInformation(logMessage);
        return textFilePath;
    }

    private string TryParseCustomDateFormats(string input)
    {
        string[] formats = { 
            "d/M/yy H:mm", "M/d/yy H:mm", "dd/MM/yyyy", "MM/dd/yyyy", "yyyy-MM-dd HH:mm:ss",
            "d 'de' MMMM 'de' yyyy H:mm", "d 'de' MMM 'de' yyyy H:mm", // Spanish formats with month names
            "d/M/yyyy", "M/d/yyyy", "d/M/yy", "M/d/yy" // Additional formats
        };

        var spanishCulture = new System.Globalization.CultureInfo("es-ES");
        if (DateTime.TryParseExact(input, formats, spanishCulture, System.Globalization.DateTimeStyles.None, out DateTime dateValue))
        {
            return dateValue.ToString("yyyy-MM-dd HH:mm:ss");
        }
        return input;
    }


    private async Task BulkInsert7JuiciosData(string textFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE stage_juicios;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    var logMessage = "Truncated table stage_juicios for 7-Juicios.";
                    logBuilder.AppendLine(logMessage);
                    _logger.LogInformation(logMessage);

                    var loadCommandText = "LOAD DATA LOCAL INFILE '" + textFilePath.Replace("\\", "\\\\") + "' " +
                                        "INTO TABLE stage_juicios " +
                                        "CHARACTER SET utf8mb4 " +
                                        "FIELDS TERMINATED BY '|' " +
                                        "ENCLOSED BY '\"' " +
                                        "LINES TERMINATED BY '\\n' " +
                                        "IGNORE 1 LINES;";

                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logMessage = "Bulk inserted data into stage_juicios for 7-Juicios.";
                    logBuilder.AppendLine(logMessage);
                    _logger.LogInformation(logMessage);

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error during bulk insert for 7-Juicios: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    throw;
                }
            }
        }
    }

    private async Task ExecuteSqlFile(string sqlFilePath, StringBuilder logBuilder)
    {
        var sql = await System.IO.File.ReadAllTextAsync(sqlFilePath);

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sql, connection, transaction);
                    await command.ExecuteNonQueryAsync();
                    var logMessage = "Executed SQL file successfully.";
                    logBuilder.AppendLine(logMessage);
                    _logger.LogInformation(logMessage);

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error executing SQL file: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    throw;
                }
            }
        }
    }


    private void MoveFilesToHistoric(string originalFilePath, string textFilePath, StringBuilder logBuilder)
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

    private async Task WriteLog(string logContent)
    {
        var logFilePath = _logPath;
        var historicLogFilePath = Path.Combine(_historicLogPath, $"BulkLoadJuicios_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.log");

        if (System.IO.File.Exists(logFilePath))
        {
            System.IO.File.Move(logFilePath, historicLogFilePath);
        }

        await System.IO.File.WriteAllTextAsync(logFilePath, logContent);
    }
}
