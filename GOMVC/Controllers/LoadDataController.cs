using Microsoft.AspNetCore.Mvc;
using OfficeOpenXml;
using MySql.Data.MySqlClient;
using System.Text;
using System.Text.RegularExpressions;
using System.Net;
using MimeKit;
using MailKit.Security;
using SharpCompress.Archives;
using SharpCompress.Common;
using System.Data;


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
            "D1_Saldos_Cartera",
            "D2-Saldos_Contables",
            "D3_Aplicacion_Pagos",
            "D4_Otorgamiento_Creditos",
            "D5_Gestiones",
            "D6_Quebrantos",
            "D7_Juicios",
            "D8_Sistema",
            "D9_Gestores_Area",
            "9-FormulateDependencia",
            "10-TipoFinanciamiento",
            "11-Motivo",
            "12-CatalogoBancos",
            "14-CatalogoResultadosAvance",
            "15-LoadDemograficos"
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
                case "d1_saldos_cartera":
                    var saldosCarteraController = new D1_Saldos_Cartera_Controller((ILogger<D1_Saldos_Cartera_Controller>)_logger, _configuration);
                    await saldosCarteraController.D1_ProcessSaldosCartera();
                    break;

                case "d2-saldos_contables":
                     var saldosContablesController = new D2_Saldos_Contables_Controller((ILogger<D2_Saldos_Contables_Controller>)_logger, _configuration);
                    await saldosContablesController.D2_ProcessSaldosContables();
                break;

                case "d3_aplicacion_pagos":
                    var aplicaciones_Pagos_Controller = new D3_Aplicaciones_Pagos_Controller((ILogger<D3_Aplicaciones_Pagos_Controller>)_logger, _configuration);
                    await aplicaciones_Pagos_Controller.D3_ProcessAplicacionPagos();
                    break;

                case "d4_otorgamiento_creditos":
                    var otorgamientoCreditosController = new D4_Otorgamiento_Creditos_Controller((ILogger<D4_Otorgamiento_Creditos_Controller>)_logger, _configuration);
                    await otorgamientoCreditosController.D4_ProcessOtorgamientoCreditos();
                    break;

                case "d5_gestiones":
                    var gestionesController = new D5_Gestiones_Controller((ILogger<D5_Gestiones_Controller>)_logger, _configuration);
                    await gestionesController.D5_ProcessGestiones();
                    break;    
                    
                case "d6_quebrantos":
                    var quebrantosController = new D6_Quebrantos_Controller((ILogger<D6_Quebrantos_Controller>)_logger, _configuration);
                    await quebrantosController.D6_ProcessQuebrantos();
                    break;  

                case "d7_juicios":
                    var juicioController = new D7_Juicios_Controller((ILogger<D7_Juicios_Controller>)_logger, _configuration);
                    await juicioController.D7_ProcessJuicios();
                    break; 

                case "d8_sistema":
                    var sistemaController = new D8_Sistema_Controller((ILogger<D8_Sistema_Controller>)_logger, _configuration);
                    await sistemaController.D8_ProcessSistema();
                    break; 

                case "d7_gestores_area":
                    var gestoresAreaController = new D9_Gestores_Area_Controller((ILogger<D9_Gestores_Area_Controller>)_logger, _configuration);
                    await gestoresAreaController.D9_ProcessGestoresArea();
                    break;   

                case "9-formulatedependencia":
                    await Process9FormulateDependencia();
                    break;

                case "10-tipofinanciamiento":
                    await Process10TipoFinanciamiento();
                    break;

                case "11-motivo":
                    await Process11Motivo();
                    break;

                case "12-catalogobancos":
                    await Process12CatalogoBancos();
                    break;

                case "14-catalogoresultadosavance":
                    await Process14CatalogoResultadosAvanceAsync();
                    break;

                case "15-loaddemograficos":
                    await Process15LoadDemograficos();
                    break;

                default:
                    _logger.LogError($"Unknown activity: {activityName}");
                    return BadRequest("Unknown activity.");
            }

            _logger.LogInformation("Activity processed successfully.");
            return Ok("Activity processed successfully.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing activity.");
            return StatusCode(500, "Internal server error.");
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
            // Move the existing log file to the historic folder
            System.IO.File.Move(logPath, historicLogPath);
        }
        // Write the new log content
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
    
    private async Task Process10TipoFinanciamiento()
    {
        var logBuilder = new StringBuilder();
        var sqlFilePath = @"C:\\Users\\Go Credit\\Documents\\DATA\\SQL\\FormulateFinanciamiento.sql";
        var sql = await System.IO.File.ReadAllTextAsync(sqlFilePath);
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting Process10TipoFinanciamiento.");
        logBuilder.AppendLine($"SQL File Path: {sqlFilePath}");
        logBuilder.AppendLine($"SQL Command: {sql}");
        _logger.LogInformation("Starting Process10TipoFinanciamiento.");
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
                    _ = SendEmailAlert("New tipo_Financiamiento records have been added");
                    logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Email alert sent.");
                    _logger.LogInformation("Email alert sent.");
                }
                else
                {
                    logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - No new tipo_Financiamiento records added. No email sent.");
                    _logger.LogInformation("No new tipo_Financiamiento records added. No email sent.");
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
    }

    private async Task Process11Motivo()
    {
        var logBuilder = new StringBuilder();
        var sqlFilePath = @"C:\\Users\\Go Credit\\Documents\\DATA\\SQL\\FormulateMotivo.sql";
        var sql = await System.IO.File.ReadAllTextAsync(sqlFilePath);
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting Process11Motivo.");
        logBuilder.AppendLine($"SQL File Path: {sqlFilePath}");
        logBuilder.AppendLine($"SQL Command: {sql}");
        _logger.LogInformation("Starting Process11Motivo.");
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
                        _ = SendEmailAlert("New motivo records have been added");
                        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Email alert sent.");
                        _logger.LogInformation("Email alert sent.");
                    }
                    else
                    {
                        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - No new motivo records added. No email sent.");
                        _logger.LogInformation("No new motivo records added. No email sent.");
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
        
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process11Motivo completed.");
        _logger.LogInformation("Process11Motivo completed.");
        await WriteLog(logBuilder.ToString(), @"C:\\Users\\Go Credit\\Documents\\DATA\\LOGS\\BulkLoadMotivo.log");
    }

    private async Task Process12CatalogoBancos()
    {
        var logBuilder = new StringBuilder();
        var sqlFilePath = @"C:\\Users\\Go Credit\\Documents\\DATA\\SQL\\FormulateCatalogoBancos.sql";
        var sql = await System.IO.File.ReadAllTextAsync(sqlFilePath);
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting Process12Clabe.");
        logBuilder.AppendLine($"SQL File Path: {sqlFilePath}");
        logBuilder.AppendLine($"SQL Command: {sql}");
        _logger.LogInformation("Starting Process12Clabe.");
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
                        _ = SendEmailAlert("New clabe records have been added");
                        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Email alert sent.");
                        _logger.LogInformation("Email alert sent.");
                    }
                    else
                    {
                        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - No new clabe records added. No email sent.");
                        _logger.LogInformation("No new clabe records added. No email sent.");
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
        
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process12Clabe completed.");
        _logger.LogInformation("Process12Clabe completed.");
        await WriteLog(logBuilder.ToString(), @"C:\\Users\\Go Credit\\Documents\\DATA\\LOGS\\BulkLoadClabe.log");
    }

    private async Task Process14CatalogoResultadosAvanceAsync()
    {
        try
        {
            // Log the start of the process
            _logger.LogInformation("Starting Process14CatalogoResultadosAvance...");

            // Define the file path for the input data
            string inputFilePath = Path.Combine(_filePath, "gestiones.csv"); // Adjust the file name as necessary
            var resultados = new List<string>();

            // Step 1: Read the CSV file and extract Resultado values
            using (var reader = new StreamReader(inputFilePath))
            {
                string headerLine = await reader.ReadLineAsync(); // Read the header line
                int resultadoIndex = Array.IndexOf(headerLine.Split(','), "Resultado"); // Find the index of Resultado column

                if (resultadoIndex == -1)
                {
                    _logger.LogError("Resultado column not found in the CSV file.");
                    return;
                }

                while (!reader.EndOfStream)
                {
                    var line = await reader.ReadLineAsync();
                    var values = line.Split(',');

                    // Ensure the Resultado column exists in the current line
                    if (values.Length > resultadoIndex)
                    {
                        string resultado = values[resultadoIndex].Trim();
                        if (!string.IsNullOrEmpty(resultado) && !resultados.Contains(resultado))
                        {
                            resultados.Add(resultado); // Add distinct Resultado values
                        }
                    }
                }
            }

            // Step 2: Insert distinct Resultado values into catalogoresultadosavance
            string connectionString = _configuration.GetConnectionString("DefaultConnection");
            using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();

                string insertQuery = "INSERT INTO catalogoresultadosavance (Clave) VALUES (@Clave)";
                using (var insertCommand = new MySqlCommand(insertQuery, connection))
                {
                    insertCommand.Parameters.Add("@Clave", MySqlDbType.VarChar);

                    foreach (var resultado in resultados)
                    {
                        insertCommand.Parameters["@Clave"].Value = resultado;
                        await insertCommand.ExecuteNonQueryAsync();
                    }
                }
            }

            // Log the successful completion of the process
            _logger.LogInformation("Process14CatalogoResultadosAvance completed successfully.");
        }
        catch (Exception ex)
        {
            // Log any errors that occur during the process
            _logger.LogError($"An error occurred in Process14CatalogoResultadosAvance: {ex.Message}");
        }
    }
    
    public async Task<IActionResult> Process15LoadDemograficos()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadDemograficos.log"; 
        var logBuilder = new StringBuilder(); 
        var startLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started. "; 
        logBuilder.AppendLine(startLog); 
        _logger.LogInformation(startLog); 

        try
        {
            // Step 1: Download and Extract Files
            await DownloadAndExtract15Demograficos();
            logBuilder.AppendLine("Downloaded and extracted Demograficos files.");
            _logger.LogInformation("Downloaded and extracted Demograficos files.");

            // Step 2: Restore Database
            string bakFilePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES\your_database.bak"; // Adjust the path
            Restore15DemograficosDatabase(bakFilePath);
            logBuilder.AppendLine("Restored Demograficos database.");
            _logger.LogInformation("Restored Demograficos database.");

            // Step 3: Export CSV
            string csvFilePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES\dwtClient.csv"; // Adjust the path
            Export15DwtClientToCsvFile(csvFilePath);
            logBuilder.AppendLine("Exported dwtClient to CSV file.");
            _logger.LogInformation("Exported dwtClient to CSV file.");

            // Step 4: Bulk Insert Data
            await BulkInsert15DemograficosFromCsv(csvFilePath);
            logBuilder.AppendLine("Bulk inserted Demograficos data from CSV.");
            _logger.LogInformation("Bulk inserted Demograficos data from CSV.");

            // Step 5: Move files to historic folder
            MoveFilesToHistoric(bakFilePath, csvFilePath, logBuilder); // Assuming you have a method to move files
            logBuilder.AppendLine("Moved files to historic folder.");
            _logger.LogInformation("Moved files to historic folder.");

            // Final Log
            var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully."; 
            logBuilder.AppendLine(endLog); 
            _logger.LogInformation(endLog); 

            await WriteLog(logBuilder.ToString(), logPath); // Write the log content to the log file
            return Ok("Demograficos loaded successfully.");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}"); 
            _logger.LogError(ex, "Error during loading Demograficos."); 
            await WriteLog(logBuilder.ToString(), logPath); // Log the error details
            return BadRequest($"Error loading Demograficos: {ex.Message}");
        }
    }
  
    public async Task DownloadAndExtract15Demograficos()
    {
        string url = "http://gocredit.zell.mx/gbckupgo/dwGocredit.rar";
        string destinationPath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES\dwGocredit.rar";
        string extractPath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
        string password = "Zell#G0";

        using (WebClient client = new WebClient())
        {
            await client.DownloadFileTaskAsync(new Uri(url), destinationPath);
        }

        using (var archive = ArchiveFactory.Open(destinationPath))
        {
            var readerOptions = new SharpCompress.Readers.ReaderOptions() { Password = password };
            foreach (var entry in archive.Entries.Where(entry => !entry.IsDirectory))
            {
                try
                {
                    entry.WriteToDirectory(extractPath, new ExtractionOptions { ExtractFullPath = true, Overwrite = true });
                    _logger.LogInformation($"Extracted {entry.Key} with size {entry.Size} bytes.");
                }
                catch (Exception ex)
                {
                    if (ex.Message.Contains("password"))
                    {
                        _logger.LogError(ex, $"Password error extracting {entry.Key}: {ex.Message}");
                    }
                    else
                    {
                        _logger.LogError(ex, $"Error extracting {entry.Key}: {ex.Message}");
                    }
                }
            }
        }
    }

    public void Restore15DemograficosDatabase(string bakFilePath)
    {
        string sqlCmd = $"/C sqlcmd -S your_server -U your_username -P your_password -Q \"RESTORE DATABASE YourDatabase FROM DISK='{bakFilePath}' WITH REPLACE\"";
        System.Diagnostics.Process.Start("cmd.exe", sqlCmd);
    }

    public void Export15DwtClientToCsvFile(string csvFilePath)
    {
        string sqlCmd = $"/C sqlcmd -S your_server -U your_username -P your_password -d YourDatabase -Q \"SELECT * FROM dwtClient\" -o \"{csvFilePath}\" -s \"|\" -W";
        System.Diagnostics.Process.Start("cmd.exe", sqlCmd);
    }

    public async Task BulkInsert15DemograficosFromCsv(string csvFilePath)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE Demograficos;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();

                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{csvFilePath.Replace("\\", "\\\\")}' " +
                                        "INTO TABLE Demograficos " +
                                        "FIELDS TERMINATED BY '|' " +
                                        "ENCLOSED BY '\"' " +
                                        "LINES TERMINATED BY '\\n';";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    throw;
                }
            }
        }
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