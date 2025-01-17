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
using Org.BouncyCastle.Asn1.Misc;


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
            "C1_Dependencias",
            "C2_Financiamientos",
            "C3_Motios",
            "C4_Bancos",
            "C6_Resultados_Avance",
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
                // D# Controllers
                case "d1_saldos_cartera":
                    var saldosCarteraController = new D1_Saldos_Cartera_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D1_Saldos_Cartera_Controller>>(),
                        _configuration);
                    await saldosCarteraController.D1_ProcessSaldosCartera();
                    break;

                case "d2-saldos_contables":
                var saldosContablesController = new D2_Saldos_Contables_Controller(
                    HttpContext.RequestServices.GetRequiredService<ILogger<D2_Saldos_Contables_Controller>>(),
                    _configuration);
                    await saldosContablesController.D2_ProcessSaldosContables();
                    break;

                case "d3_aplicacion_pagos":
                    var aplicacionesPagosController = new D3_Aplicaciones_Pagos_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D3_Aplicaciones_Pagos_Controller>>(),
                        _configuration);
                    await aplicacionesPagosController.D3_ProcessAplicacionPagos();
                    break;

                case "d4_otorgamiento_creditos":
                    var otorgamientoCreditosController = new D4_Otorgamiento_Creditos_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D4_Otorgamiento_Creditos_Controller>>(),
                        _configuration);
                    await otorgamientoCreditosController.D4_ProcessOtorgamientoCreditos();
                    break;

                case "d5_gestiones":
                    var gestionesController = new D5_Gestiones_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D5_Gestiones_Controller>>(),
                        _configuration);
                    await gestionesController.D5_ProcessGestiones();
                    break;

                case "d6_quebrantos":
                    var quebrantosController = new D6_Quebrantos_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D6_Quebrantos_Controller>>(),
                        _configuration);
                    await quebrantosController.D6_ProcessQuebrantos();
                    break;

                case "d7_juicios":
                    var juiciosController = new D7_Juicios_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D7_Juicios_Controller>>(),
                        _configuration);
                    await juiciosController.D7_ProcessJuicios();
                    break;

                case "d8_sistema":
                    var sistemaController = new D8_Sistema_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D8_Sistema_Controller>>(),
                        _configuration);
                    await sistemaController.D8_ProcessSistema();
                    break;

                case "d9_gestores_area":
                    var gestoresAreaController = new D9_Gestores_Area_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D9_Gestores_Area_Controller>>(),
                        _configuration);
                    await gestoresAreaController.D9_ProcessGestoresArea();
                    break;

                // C# Controllers
                case "c1_dependencias":
                    var dependenciasController = new C1_Dependencias_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<C1_Dependencias_Controller>>(),
                        _configuration);
                    await dependenciasController.C1_First_Time_Dependencias_Execution();
                    break;

                case "c2_financiamientos":
                    var financiamientosController = new C2_Financiamientos_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<C2_Financiamientos_Controller>>(),
                        _configuration);
                    await financiamientosController.C2_First_Time_Financiamiento_Execution();
                    break;

                case "c3_motivos":
                    var motivosController = new C3_Motivos_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<C3_Motivos_Controller>>(),
                        _configuration);
                    await motivosController.C3_First_Time_Motivo_Execution();
                    break;

                case "c4_bancos":
                    var bancosController = new C4_Bancos_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<C4_Bancos_Controller>>(),
                        _configuration);
                    await bancosController.C4_First_Time_Bancos_Execution();
                    break;

                case "c5_referencias":
                    var referenciasController = new C5_Referencias_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<C5_Referencias_Controller>>(),
                        _configuration);
                    await referenciasController.C5_ProcessReferencias();
                    break;

                case "c6_resultados_avances":
                    var resultadosAvancesController = new C6_Resultados_Avances_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<C6_Resultados_Avances_Controller>>(),
                        _configuration);
                    await resultadosAvancesController.C6_First_Time_Resultados_Execution();
                    break;

                default:
                    _logger.LogError($"Unknown activity: {activityName}");
                    return BadRequest($"Unknown activity: {activityName}");
            }

            _logger.LogInformation($"Activity '{activityName}' processed successfully.");
            return Ok($"Activity '{activityName}' processed successfully.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error processing activity: {activityName}");
            return StatusCode(500, $"Internal server error while processing activity: {activityName}");
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
    
}