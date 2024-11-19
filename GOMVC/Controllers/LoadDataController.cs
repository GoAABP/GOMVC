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
using System.Text.RegularExpressions;
using System.Net.Mail;
using System.Net;
using MimeKit;
using MailKit.Security;
using MailKit;

public class LoadDataController : Controller
{
 private readonly ILogger<LoadDataController> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";
    private static readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    public LoadDataController(ILogger<LoadDataController> logger, IConfiguration configuration)
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    {
        _logger = logger;
        _configuration = configuration;
#pragma warning disable CS8601 // Possible null reference assignment.
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
#pragma warning restore CS8601 // Possible null reference assignment.
    }

    [HttpGet]
    public IActionResult Index()
    {
        var activities = new List<string>
        {
            "1-SaldosCartera",
            "2-ActivityTwo",
            "3-ActivityThree",
            "4-ActivityFour",
            "5-ActivityFive",
            "6-ActivitySix",
            "7-Juicios",
            "8-ActivityEight",
            "9-FormulateDependencia"
        };
        return View(activities);
    }

    [HttpPost("HandleActivity")]
    public async Task<IActionResult> HandleActivity(string activityName)
    {
        try
        {
            switch (activityName.ToLower())
            {
                case "1-saldoscartera":
                    await Process1SaldosCartera();
                    break;
                case "7-juicios":
                    await Process7Juicios();
                    break;
                case "9-formulatedependencia":
                    await Process9FormulateDependencia();
                    break;
                default:
                    _logger.LogError($"Unknown activity: {activityName}");
                    return BadRequest("Unknown activity.");
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

    private async Task Process1SaldosCartera()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadSaldosCartera.log";
        var logBuilder = new StringBuilder();
        var todayDate = DateTime.Now.ToString("yyyy-MM-dd");
        var startLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.";
        logBuilder.AppendLine(startLog);
        _logger.LogInformation(startLog);
        var files = Directory.GetFiles(_filePath, "SaldosCarteraXConvenio*.csv");
        if (files.Length == 0)
        {
            var errorLog = "File not found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }
        var file = files[0];
        logBuilder.AppendLine("File found.");
        try
        {
            var textFilePath = await Convert1SaldosCarteraCsvToText(file, logBuilder);
            await BulkInsert1SaldosCarteraData(textFilePath, logBuilder);
            await Execute1SaldosCarteraInsert(logBuilder, logPath);
            MoveFilesToHistoric(file, textFilePath, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await WriteLog(logBuilder.ToString(), logPath);
            throw;
        }
        var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.";
        logBuilder.AppendLine(endLog);
        _logger.LogInformation(endLog);
        await WriteLog(logBuilder.ToString(), logPath);
    }

    private async Task<string> Convert1SaldosCarteraCsvToText(string csvFilePath, StringBuilder logBuilder)
    {
        // Register the code pages encoding provider
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

        var textFilePath = Path.ChangeExtension(csvFilePath, ".txt");
        var sb = new StringBuilder();
        
        try
        {
            // Use StreamReader with Windows-1252 encoding
            using (var reader = new StreamReader(csvFilePath, Encoding.GetEncoding("windows-1252")))
            {
                string line;
                bool isFirstLine = true;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (isFirstLine)
                    {
                        isFirstLine = false;
                        continue; // Skip header
                    }
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    
                    // Handle accents and other special characters
                    var processedLine = line.Normalize(NormalizationForm.FormC);
                    
                    // Replace commas outside quotes with pipes
                    processedLine = Regex.Replace(processedLine, ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", "|");
                    
                    // Convert date formats
                    processedLine = Regex.Replace(processedLine, @"(\d{2})/(\d{2})/(\d{4})", "$3-$2-$1");
                    
                    sb.AppendLine(processedLine);
                }
            }
            
            // Use StreamWriter with UTF-8 encoding to ensure the output file is in UTF-8
            using (var writer = new StreamWriter(textFilePath, false, Encoding.UTF8))
            {
                await writer.WriteAsync(sb.ToString());
            }
            
            var logMessage = $"Converted CSV to text for SaldosCartera: {textFilePath}";
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
    
    private async Task BulkInsert1SaldosCarteraData(string textFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE Stage_Saldos_Cartera;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    var logMessage = "Truncated table Stage_Saldos_Cartera.";
                    logBuilder.AppendLine(logMessage);
                    _logger.LogInformation(logMessage);
                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{textFilePath.Replace("\\", "\\\\")}' " +
                                        "INTO TABLE Stage_Saldos_Cartera " +
                                        "FIELDS TERMINATED BY '|' " +
                                        "ENCLOSED BY '\"' " +
                                        "LINES TERMINATED BY '\\n' " +
                                        "IGNORE 1 LINES;"; // Ensure to ignore the header line
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logMessage = "Bulk inserted data into Stage_Saldos_Cartera.";
                    logBuilder.AppendLine(logMessage);
                    _logger.LogInformation(logMessage);
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error during bulk insert for SaldosCartera: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    throw;
                }
            }
        }
    }

    private async Task Execute1SaldosCarteraInsert(StringBuilder logBuilder, string logPath)
    {
        var sqlFilePath = @"C:\Users\Go Credit\Documents\DATA\SQL\InsertSaldosCartera.sql";
        var sql = await System.IO.File.ReadAllTextAsync(sqlFilePath);

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    // Determine the current period
                    var currentPeriod = GetCurrentPeriod();
                    var todayDate = DateTime.Now.ToString("yyyy-MM-dd");

                    // Check if a record with today's date and the current period already exists
                    var checkCommandText = @"
                        SELECT 1
                        FROM Saldos_Cartera
                        WHERE DATE(FechaGenerado) = @TodayDate
                        AND TIME(FechaGenerado) = @CurrentPeriod;";
                    var checkCommand = new MySqlCommand(checkCommandText, connection, transaction);
                    checkCommand.Parameters.AddWithValue("@TodayDate", todayDate);
                    checkCommand.Parameters.AddWithValue("@CurrentPeriod", currentPeriod);

                    var exists = await checkCommand.ExecuteScalarAsync() != null;

                    if (exists)
                    {
                        var logMessage = "Validation failed: Record with today's date and current period already exists.";
                        logBuilder.AppendLine(logMessage);
                        _logger.LogInformation(logMessage);
                        await WriteLog(logBuilder.ToString(), logPath);
                        return; // Exit the method if the record exists
                    }

                    // Execute the SQL file
                    var command = new MySqlCommand(sql, connection, transaction);
                    command.Parameters.AddWithValue("@CurrentPeriod", currentPeriod);
                    await command.ExecuteNonQueryAsync();
                    var logMessageSuccess = "Executed SQL file successfully.";
                    logBuilder.AppendLine(logMessageSuccess);
                    _logger.LogInformation(logMessageSuccess);

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error executing SQL file: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    await WriteLog(logBuilder.ToString(), logPath);
                    throw;
                }
            }
        }
    }

    private string GetCurrentPeriod()
    {
        var now = DateTime.Now.TimeOfDay;
        if (now >= TimeSpan.Parse("00:00:00") && now <= TimeSpan.Parse("07:00:00"))
        {
            return "07:00:00";
        }
        else if (now >= TimeSpan.Parse("07:01:00") && now <= TimeSpan.Parse("18:00:00"))
        {
            return "18:00:00";
        }
        else
        {
            return "23:59:59";
        }
    }
    
    private async Task Process7Juicios()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadJuicios.log";
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
            await WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }
        var file = files[0];
        logBuilder.AppendLine("File found.");
        try
        {
            var textFilePath = Convert7JuiciosExcelToText(file, logBuilder);
            await BulkInsert7JuiciosData(textFilePath, logBuilder);
            await ExecuteSqlFile(@"C:\Users\Go Credit\Documents\DATA\SQL\InsertJuicios.sql", logBuilder);
            MoveFilesToHistoric(file, textFilePath, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await WriteLog(logBuilder.ToString(), logPath);
            throw;
        }
        var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.";
        logBuilder.AppendLine(endLog);
        _logger.LogInformation(endLog);
        await WriteLog(logBuilder.ToString(), logPath);
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
                                          "FIELDS TERMINATED BY '|' " +
                                          "ENCLOSED BY '\"' " +
                                          "LINES TERMINATED BY '\\n' ";
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

    
    private async Task WriteLog(string logContent, string logPath)
    {
        var historicLogPath = Path.Combine(_historicFilePath, $"BulkLoad_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.log");
        if (System.IO.File.Exists(logPath))
        {
            System.IO.File.Move(logPath, historicLogPath);
        }
        await System.IO.File.WriteAllTextAsync(logPath, logContent);
    }

    private async Task Process9FormulateDependencia()
    {
        var logBuilder = new StringBuilder();
        var sqlFilePath = @"C:\Users\Go Credit\Documents\DATA\SQL\FormulateDependencia.sql";
        var sql = await System.IO.File.ReadAllTextAsync(sqlFilePath);
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting Process9FormulateDependencia.");
        logBuilder.AppendLine($"SQL File Path: {sqlFilePath}");
        logBuilder.AppendLine($"SQL Command: {sql}");
        _logger.LogInformation("Starting Process9FormulateDependencia.");
        _logger.LogInformation($"SQL File Path: {sqlFilePath}");
        _logger.LogInformation($"SQL Command: {sql}");
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sql, connection, transaction);
                    var affectedRows = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - SQL file executed. Rows affected: {affectedRows}.");
                    _logger.LogInformation($"SQL file executed. Rows affected: {affectedRows}.");
                    if (affectedRows >= 1)
                    {
                        _ = SendEmailAlert("New dependencies have been added");
                        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Email alert sent.");
                        _logger.LogInformation("Email alert sent.");
                    }
                    else
                    {
                        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - No new dependencies added. No email sent.");
                        _logger.LogInformation("No new dependencies added. No email sent.");
                    }
                    await transaction.CommitAsync();
                    logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Transaction committed.");
                    _logger.LogInformation("Transaction committed.");
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Error executing SQL file: {ex.Message}");
                    _logger.LogError(ex, "Error executing SQL file.");
                    throw;
                }
            }
        }
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process9FormulateDependencia completed.");
        _logger.LogInformation("Process9FormulateDependencia completed.");
        await WriteLog(logBuilder.ToString(), @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadFormulateDependencia.log");
    }
    
    private async Task SendEmailAlert(string message)
    {
        var emailMessage = new MimeMessage();
        emailMessage.From.Add(new MailboxAddress("Your Name", "gomvc.notice@gmail.com"));
        emailMessage.To.Add(new MailboxAddress("Alfredo Bueno", "alfredo.bueno@gocredit.mx"));
        emailMessage.Subject = "Alert: New Dependencies Added";
        emailMessage.Body = new TextPart("plain")
        {
            Text = message
        };

        using (var client = new MailKit.Net.Smtp.SmtpClient())
        {
            await client.ConnectAsync("smtp.gmail.com", 587, SecureSocketOptions.StartTls);

            // Use the app password here
            await client.AuthenticateAsync("gomvc.notice@gmail.com", "rnbn ugwd jwgu znav");

            await client.SendAsync(emailMessage);
            await client.DisconnectAsync(true);
        }
    }
}