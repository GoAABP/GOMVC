using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

public class B1_Demograficos_Controller : Controller
{
    private readonly ILogger<B1_Demograficos_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _logsFolder = @"C:\Users\Go Credit\Documents\DATA\LOGS";
    private readonly string _historicLogsFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC LOGS";
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _sqlServerInstance = "GoCre793BM34";
    private readonly string _dbName = "dwGoCredit";
    private readonly string _logFileName;
    private readonly string _mysqlConnectionString;

    public B1_Demograficos_Controller(ILogger<B1_Demograficos_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _mysqlConnectionString = _configuration.GetConnectionString("MySQLConnection")!;
        _logFileName = $"{nameof(B1_Demograficos_Controller)}.log";
        var logPath = Path.Combine(_logsFolder, _logFileName);

        if (System.IO.File.Exists(logPath))
        {
            B1_MoveExistingLog(logPath, _historicLogsFolder);
        }
    }

    [HttpPost]
    public async Task<IActionResult> B1_ProcessDemograficos()
    {
        var logPath = Path.Combine(_logsFolder, _logFileName);
        var logBuilder = new StringBuilder();

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - B1 process started.");
        _logger.LogInformation("B1 process started.");

        string compressedFilePath = Path.Combine(_filePath, "dwGocredit.rar");
        string extractFolderPath = Path.Combine(_filePath, "Extracted");
        string backupFilePath = Path.Combine(extractFolderPath, "databases", "backups", "dwGoCredit.bak");
        string sqlServerInstance = "GoCre793BM34";
        string databaseName = "dwGoCredit";
        string rarPassword = "Zell#G0";
        string unrarExePath = @"C:\Program Files\WinRAR\UnRAR.exe"; 

        try
        {
            // --- SECCIÓN DE DESCARGA ---
            string downloadUrl = "http://gocredit.zell.mx/gbckupgo/dwGocredit.rar";
            logBuilder.AppendLine($"Starting download from: {downloadUrl}");
            _logger.LogInformation($"Starting download from: {downloadUrl}");

            using (HttpClient client = new HttpClient { Timeout = TimeSpan.FromMinutes(30) })
            {
                using (HttpResponseMessage response = await client.GetAsync(downloadUrl, HttpCompletionOption.ResponseHeadersRead))
                {
                    response.EnsureSuccessStatusCode();
                    using (var contentStream = await response.Content.ReadAsStreamAsync())
                    using (var fileStream = new FileStream(compressedFilePath, FileMode.Create, FileAccess.Write, FileShare.None, 8192, true))
                    {
                        await contentStream.CopyToAsync(fileStream);
                    }
                }
            }

            logBuilder.AppendLine($"File downloaded successfully to: {compressedFilePath}");
            _logger.LogInformation($"File downloaded successfully to: {compressedFilePath}");

            // --- VERIFICAR QUE EL ARCHIVO REALMENTE EXISTA ANTES DE PROCEDER ---
            int maxWaitTime = 600; // Esperar hasta 10 minutos (600 segundos)
            int waitInterval = 5; // Revisar cada 5 segundos
            int elapsed = 0;

            while (!System.IO.File.Exists(compressedFilePath) && elapsed < maxWaitTime)
            {
                logBuilder.AppendLine($"Waiting for compressed file to be available: {compressedFilePath} ({elapsed}/{maxWaitTime} seconds)");
                _logger.LogInformation($"Waiting for compressed file to be available: {compressedFilePath} ({elapsed}/{maxWaitTime} seconds)");
                await Task.Delay(waitInterval * 1000);
                elapsed += waitInterval;
            }

            if (!System.IO.File.Exists(compressedFilePath))
            {
                throw new FileNotFoundException("Compressed file not found after download.", compressedFilePath);
            }

            // --- PROCESO DE EXTRACCIÓN ---
            logBuilder.AppendLine($"Starting extraction of: {compressedFilePath}");
            _logger.LogInformation($"Starting extraction of: {compressedFilePath}");

            if (!Directory.Exists(extractFolderPath))
            {
                Directory.CreateDirectory(extractFolderPath);
            }

            var extractProcessInfo = new System.Diagnostics.ProcessStartInfo
            {
                FileName = unrarExePath,
                Arguments = $"x -p{rarPassword} -o+ \"{compressedFilePath}\" \"{extractFolderPath}\"",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            using (var process = new System.Diagnostics.Process { StartInfo = extractProcessInfo })
            {
                process.Start();
                string output = await process.StandardOutput.ReadToEndAsync();
                string error = await process.StandardError.ReadToEndAsync();
                process.WaitForExit();

                logBuilder.AppendLine($"Extraction output: {output}");
                _logger.LogInformation($"Extraction output: {output}");

                if (!string.IsNullOrEmpty(error))
                {
                    logBuilder.AppendLine($"Extraction error: {error}");
                    _logger.LogError($"Extraction error: {error}");
                    throw new Exception($"UnRAR.exe error: {error}");
                }
            }

            logBuilder.AppendLine($"Extraction completed successfully to: {extractFolderPath}");
            _logger.LogInformation($"Extraction completed successfully to: {extractFolderPath}");

            // --- ESPERAR A QUE EL ARCHIVO .BAK ESTÉ DISPONIBLE ---
            elapsed = 0;
            while (!System.IO.File.Exists(backupFilePath) && elapsed < maxWaitTime)
            {
                logBuilder.AppendLine($"Waiting for backup file to be available: {backupFilePath} ({elapsed}/{maxWaitTime} seconds)");
                _logger.LogInformation($"Waiting for backup file to be available: {backupFilePath} ({elapsed}/{maxWaitTime} seconds)");
                await Task.Delay(waitInterval * 1000);
                elapsed += waitInterval;
            }

            if (!System.IO.File.Exists(backupFilePath))
            {
                throw new FileNotFoundException("Backup file not found after extraction.", backupFilePath);
            }

            logBuilder.AppendLine($"Backup file found: {backupFilePath}");
            _logger.LogInformation($"Backup file found: {backupFilePath}");

        }
        catch (FileNotFoundException ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "File not found.");
            await B1_WriteLog(logBuilder.ToString(), logPath);
            return StatusCode(500, $"Error: {ex.Message}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Unexpected error: {ex.Message}");
            _logger.LogError(ex, "Unexpected error.");
            await B1_WriteLog(logBuilder.ToString(), logPath);
            return StatusCode(500, "Unexpected error occurred.");
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - B1 process completed.");
        _logger.LogInformation("B1 process completed.");
        await B1_WriteLog(logBuilder.ToString(), logPath);

        return Ok("B1 process completed successfully.");
    }

    private async Task<bool> B1_BulkInsertToMySQL(string csvFilePath, StringBuilder logBuilder, string logPath)
    {
        try
        {
            using (var connection = new MySqlConnection(_mysqlConnectionString))
            {
                await connection.OpenAsync();
                string truncateTable = "TRUNCATE TABLE B1_Demograficos;";
                using (var truncateCmd = new MySqlCommand(truncateTable, connection))
                {
                    await truncateCmd.ExecuteNonQueryAsync();
                }

                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{csvFilePath.Replace("\\", "\\\\")}' " +
                                        "INTO TABLE B1_Demograficos " +
                                        "FIELDS TERMINATED BY '|' " +
                                        "ENCLOSED BY '\"' " +
                                        "LINES TERMINATED BY '\\n' " +
                                        "IGNORE 1 LINES;";

                using (var loadCmd = new MySqlCommand(loadCommandText, connection))
                {
                    int rowsInserted = await loadCmd.ExecuteNonQueryAsync();
                    if (rowsInserted == 0)
                    {
                        logBuilder.AppendLine("ERR301: No data inserted.");
                        await B1_WriteLog(logBuilder.ToString(), logPath);
                        return false;
                    }
                }
            }
            return true;
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"ERR302: Bulk insert error - {ex.Message}");
            _logger.LogError(ex, "Bulk insert error.");
            await B1_WriteLog(logBuilder.ToString(), logPath);
            return false;
        }
    }

    private async Task B1_WriteLog(string content, string logPath)
    {
        try
        {
            await System.IO.File.WriteAllTextAsync(logPath, content);
            _logger.LogInformation($"Log written to: {logPath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error writing log to {logPath}");
        }
    }

    private void B1_MoveExistingLog(string logPath, string historicLogsFolder)
    {
        if (System.IO.File.Exists(logPath))
        {
            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string historicLogPath = Path.Combine(historicLogsFolder, $"{Path.GetFileNameWithoutExtension(logPath)}_{timestamp}.log");
            System.IO.File.Move(logPath, historicLogPath);
        }
    }

    private void B1_MoveLogToHistoric(string logPath, string historicLogsFolder)
    {
        B1_MoveExistingLog(logPath, historicLogsFolder);
    }

    private void B1_GrantPermissionsToSQL(string filePath, StringBuilder logBuilder)
    {
        try
        {
            var processInfo = new System.Diagnostics.ProcessStartInfo
            {
                FileName = "icacls",
                Arguments = $"\"{filePath}\" /grant \"NT AUTHORITY\\SYSTEM\":(F) \"NETWORK SERVICE\":(F) \"BUILTIN\\Administrators\":(F)",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            using (var process = new System.Diagnostics.Process { StartInfo = processInfo })
            {
                process.Start();
                string output = process.StandardOutput.ReadToEnd();
                string error = process.StandardError.ReadToEnd();
                process.WaitForExit();

                logBuilder.AppendLine($"Permissions granted output: {output}");
                _logger.LogInformation($"Permissions granted output: {output}");

                if (!string.IsNullOrEmpty(error))
                {
                    logBuilder.AppendLine($"Permissions granted error: {error}");
                    _logger.LogError($"Permissions granted error: {error}");
                    throw new Exception($"Permission grant error: {error}");
                }
            }

            logBuilder.AppendLine($"Permissions granted successfully for: {filePath}");
            _logger.LogInformation($"Permissions granted successfully for: {filePath}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error granting permissions: {ex.Message}");
            _logger.LogError(ex, "Error granting permissions.");
            throw;
        }
    }

}
