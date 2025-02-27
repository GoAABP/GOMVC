using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

public class Backup_Zell_Controller : Controller
{
    private readonly ILogger<Backup_Zell_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _logsFolder = @"C:\Users\Go Credit\Documents\DATA\LOGS";
    private readonly string _historicLogsFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC LOGS";
    // Ruta base para archivos FLAT FILES
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _sqlServerInstance = "GoCre793BM34";
    private readonly string _dbName = "dwGoCredit";
    private readonly string _logFileName;
    // Se utiliza la cadena "DefaultConnection" para MySQL
    private readonly string _mysqlConnectionString;

    public Backup_Zell_Controller(ILogger<Backup_Zell_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _mysqlConnectionString = _configuration.GetConnectionString("DefaultConnection")!;
        _logFileName = $"{nameof(Backup_Zell_Controller)}.log";
        var logPath = Path.Combine(_logsFolder, _logFileName);
        if (System.IO.File.Exists(logPath))
        {
            MoveExistingLog(logPath, _historicLogsFolder);
        }
    }

    [HttpPost]
    public async Task<IActionResult> ProcessBackup()
    {
        var logPath = Path.Combine(_logsFolder, _logFileName);
        var logBuilder = new StringBuilder();
        bool allStepsSucceeded = false;

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Backup process started.");
        _logger.LogInformation("Backup process started.");

        // Parámetros fijos
        string downloadUrl = "http://gocredit.zell.mx/gbckupgo/dwGocredit.rar";
        string compressedFilePath = Path.Combine(_filePath, "dwGoCredit.rar");
        string extractFolderPath = @"C:\databases\backups"; // Ruta para extraer el .bak
        string backupFilePath = Path.Combine(extractFolderPath, "dwGoCredit.bak");
        string rarPassword = "Zell#G0";
        string unrarExePath = @"C:\Program Files\WinRAR\UnRAR.exe";
        int maxWaitTime = 600; // segundos
        int waitInterval = 5;  // segundos

        // Variables para logs de exportación
        var demograficosExportLog = new StringBuilder();
        var amortizacionesExportLog = new StringBuilder();

        try
        {
            // 1. DESCARGA
            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting download from: {downloadUrl}");
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
            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - File downloaded successfully to: {compressedFilePath}");
            _logger.LogInformation($"File downloaded successfully to: {compressedFilePath}");

            // Esperar a que el archivo exista
            int elapsed = 0;
            while (!System.IO.File.Exists(compressedFilePath) && elapsed < maxWaitTime)
            {
                logBuilder.AppendLine($"Waiting for compressed file: {compressedFilePath} ({elapsed}/{maxWaitTime} seconds)");
                _logger.LogInformation($"Waiting for compressed file: {compressedFilePath} ({elapsed}/{maxWaitTime} seconds)");
                await Task.Delay(waitInterval * 1000);
                elapsed += waitInterval;
            }
            if (!System.IO.File.Exists(compressedFilePath))
                throw new FileNotFoundException("Compressed file not found after download.", compressedFilePath);

            // 2. DESCOMPRESIÓN (comando "e" para extraer sin rutas)
            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting extraction of: {compressedFilePath}");
            _logger.LogInformation($"Starting extraction of: {compressedFilePath}");
            if (!Directory.Exists(extractFolderPath))
                Directory.CreateDirectory(extractFolderPath);

            var extractProcessInfo = new ProcessStartInfo
            {
                FileName = unrarExePath,
                Arguments = $"e -p{rarPassword} -o+ \"{compressedFilePath}\" \"{extractFolderPath}\"",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };
            using (var process = new Process { StartInfo = extractProcessInfo })
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
            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Extraction completed successfully to: {extractFolderPath}");
            _logger.LogInformation($"Extraction completed successfully to: {extractFolderPath}");

            // Esperar a que el archivo .bak exista
            elapsed = 0;
            while (!System.IO.File.Exists(backupFilePath) && elapsed < maxWaitTime)
            {
                logBuilder.AppendLine($"Waiting for backup file: {backupFilePath} ({elapsed}/{maxWaitTime} seconds)");
                _logger.LogInformation($"Waiting for backup file: {backupFilePath} ({elapsed}/{maxWaitTime} seconds)");
                await Task.Delay(waitInterval * 1000);
                elapsed += waitInterval;
            }
            if (!System.IO.File.Exists(backupFilePath))
                throw new FileNotFoundException("Backup file not found after extraction.", backupFilePath);

            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Backup file found: {backupFilePath}");
            _logger.LogInformation($"Backup file found: {backupFilePath}");

            // 3. RESTAURAR BASE DE DATOS (.bak) USANDO SQLCMD
            string restoreCommand = $"sqlcmd -S {_sqlServerInstance} -d master -E -Q \"RESTORE DATABASE [{_dbName}] FROM DISK = '{backupFilePath}' WITH REPLACE\"";
            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting database restore using: {restoreCommand}");
            _logger.LogInformation($"Starting database restore using: {restoreCommand}");
            using (var restoreProcess = new Process())
            {
                restoreProcess.StartInfo.FileName = "cmd.exe";
                restoreProcess.StartInfo.Arguments = $"/C {restoreCommand}";
                restoreProcess.StartInfo.RedirectStandardOutput = true;
                restoreProcess.StartInfo.RedirectStandardError = true;
                restoreProcess.StartInfo.UseShellExecute = false;
                restoreProcess.StartInfo.CreateNoWindow = true;
                restoreProcess.Start();
                string restoreOutput = await restoreProcess.StandardOutput.ReadToEndAsync();
                string restoreError = await restoreProcess.StandardError.ReadToEndAsync();
                restoreProcess.WaitForExit();
                logBuilder.AppendLine($"Restore output: {restoreOutput}");
                _logger.LogInformation($"Restore output: {restoreOutput}");
                if (!string.IsNullOrEmpty(restoreError))
                {
                    logBuilder.AppendLine($"Restore error: {restoreError}");
                    _logger.LogError($"Restore error: {restoreError}");
                    throw new Exception($"Database restore error: {restoreError}");
                }
            }
            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Database restored successfully.");
            _logger.LogInformation("Database restored successfully.");

            // 4. EXPORTAR LA TABLA dwtClient (demográficos) USANDO SQLCMD
            // Se usa -h -1 para no incluir encabezados ni guiones
            string exportDemograficosCommand = $"sqlcmd -S {_sqlServerInstance} -d {_dbName} -E -Q \"SELECT * FROM dwtClient\" -s \"|\" -W -h -1 -o \"{Path.Combine(_filePath, "Demograficos.csv")}\"";
            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting export of dwtClient using: {exportDemograficosCommand}");
            _logger.LogInformation($"Starting export of dwtClient using: {exportDemograficosCommand}");
            var demograficosExportLogBuilder = new StringBuilder();
            using (var exportProcess = new Process())
            {
                exportProcess.StartInfo.FileName = "cmd.exe";
                exportProcess.StartInfo.Arguments = $"/C {exportDemograficosCommand}";
                exportProcess.StartInfo.RedirectStandardOutput = true;
                exportProcess.StartInfo.RedirectStandardError = true;
                exportProcess.StartInfo.UseShellExecute = false;
                exportProcess.StartInfo.CreateNoWindow = true;
                exportProcess.Start();
                string exportOutput = await exportProcess.StandardOutput.ReadToEndAsync();
                string exportError = await exportProcess.StandardError.ReadToEndAsync();
                exportProcess.WaitForExit();
                demograficosExportLogBuilder.AppendLine($"Export dwtClient output: {exportOutput}");
                _logger.LogInformation($"Export dwtClient output: {exportOutput}");
                if (!string.IsNullOrEmpty(exportError))
                {
                    demograficosExportLogBuilder.AppendLine($"Export dwtClient error: {exportError}");
                    _logger.LogError($"Export dwtClient error: {exportError}");
                    throw new Exception($"Export dwtClient error: {exportError}");
                }
            }
            demograficosExportLogBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - dwtClient exported successfully as Demograficos.csv.");
            _logger.LogInformation("dwtClient exported successfully.");
            // Escribir log específico para export demográficos
            string demograficosExportLogPath = Path.Combine(_logsFolder, "B1_Demograficos_Export.log");
            await WriteLog(demograficosExportLogBuilder.ToString(), demograficosExportLogPath);

            // 5. CONSUMO DE LA TABLA dwtClient EN MySQL (Bulk Insert a B1_Demograficos)
            string demograficosCsv = Path.Combine(_filePath, "Demograficos.csv");
            bool bulkInsertDemograficosOk = await BulkInsertToMySQL(demograficosCsv, logBuilder, logPath);
            if (!bulkInsertDemograficosOk)
                throw new Exception("Bulk insert to MySQL for demograficos failed.");

            // 6. EXPORTAR LA TABLA dwtCreditDeferral (amortizaciones) USANDO SQLCMD
            string exportAmortizacionesCommand = $"sqlcmd -S {_sqlServerInstance} -d {_dbName} -E -Q \"SELECT * FROM dwtCreditDeferral\" -s \"|\" -W -h -1 -o \"{Path.Combine(_filePath, "amortizaciones.csv")}\"";
            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting export of dwtCreditDeferral using: {exportAmortizacionesCommand}");
            _logger.LogInformation($"Starting export of dwtCreditDeferral using: {exportAmortizacionesCommand}");
            var amortizacionesExportLogBuilder = new StringBuilder();
            using (var exportProcess2 = new Process())
            {
                exportProcess2.StartInfo.FileName = "cmd.exe";
                exportProcess2.StartInfo.Arguments = $"/C {exportAmortizacionesCommand}";
                exportProcess2.StartInfo.RedirectStandardOutput = true;
                exportProcess2.StartInfo.RedirectStandardError = true;
                exportProcess2.StartInfo.UseShellExecute = false;
                exportProcess2.StartInfo.CreateNoWindow = true;
                exportProcess2.Start();
                string exportOutput2 = await exportProcess2.StandardOutput.ReadToEndAsync();
                string exportError2 = await exportProcess2.StandardError.ReadToEndAsync();
                exportProcess2.WaitForExit();
                amortizacionesExportLogBuilder.AppendLine($"Export dwtCreditDeferral output: {exportOutput2}");
                _logger.LogInformation($"Export dwtCreditDeferral output: {exportOutput2}");
                if (!string.IsNullOrEmpty(exportError2))
                {
                    amortizacionesExportLogBuilder.AppendLine($"Export dwtCreditDeferral error: {exportError2}");
                    _logger.LogError($"Export dwtCreditDeferral error: {exportError2}");
                    throw new Exception($"Export dwtCreditDeferral error: {exportError2}");
                }
            }
            amortizacionesExportLogBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - dwtCreditDeferral exported successfully as amortizaciones.csv.");
            _logger.LogInformation("dwtCreditDeferral exported successfully.");
            // Escribir log específico para export amortizaciones
            string amortizacionesExportLogPath = Path.Combine(_logsFolder, "B2_Amortizaciones_Export.log");
            await WriteLog(amortizacionesExportLogBuilder.ToString(), amortizacionesExportLogPath);

            // 7. CONSUMO DE LA TABLA dwtCreditDeferral EN MySQL (Bulk Insert a B2_Stage_Amortizaciones)
            string amortizacionesCsv = Path.Combine(_filePath, "amortizaciones.csv");
            bool bulkInsertAmortizacionesOk = await BulkInsertToAmortizaciones(amortizacionesCsv, logBuilder, logPath);
            if (!bulkInsertAmortizacionesOk)
                throw new Exception("Bulk insert to MySQL for amortizaciones failed.");

            allStepsSucceeded = true;
        }
        catch (FileNotFoundException ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "File not found.");
            await WriteLog(logBuilder.ToString(), logPath);
            return StatusCode(500, $"Error: {ex.Message}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Unexpected error: {ex.Message}");
            _logger.LogError(ex, "Unexpected error.");
            await WriteLog(logBuilder.ToString(), logPath);
            return StatusCode(500, "Unexpected error occurred.");
        }

        if (allStepsSucceeded)
        {
            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Backup process completed successfully.");
            _logger.LogInformation("Backup process completed successfully.");
            await WriteLog(logBuilder.ToString(), logPath);
            MoveExistingLog(logPath, _historicLogsFolder);

            // Post-proceso: eliminar el archivo RAR y el .bak, y mover los CSV a HISTORIC FILES\Archive
            try
            {
                // Eliminar archivo RAR
                if (System.IO.File.Exists(compressedFilePath))
                {
                    System.IO.File.Delete(compressedFilePath);
                    _logger.LogInformation($"Compressed file deleted: {compressedFilePath}");
                    logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Compressed file deleted: {compressedFilePath}");
                }
                // Eliminar archivo .bak
                if (System.IO.File.Exists(backupFilePath))
                {
                    System.IO.File.Delete(backupFilePath);
                    _logger.LogInformation($"Backup file deleted: {backupFilePath}");
                    logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Backup file deleted: {backupFilePath}");
                }
                // Mover archivo exportado de dwtClient (Demograficos.csv) a HISTORIC FILES\Archive
                string demograficosCsvPath = Path.Combine(_filePath, "Demograficos.csv");
                string archiveFolderDemograficos = Path.Combine(@"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES", "Archive");
                if (!Directory.Exists(archiveFolderDemograficos))
                {
                    Directory.CreateDirectory(archiveFolderDemograficos);
                }
                string archiveFilePathDemograficos = Path.Combine(archiveFolderDemograficos, Path.GetFileName(demograficosCsvPath));
                if (System.IO.File.Exists(archiveFilePathDemograficos))
                {
                    System.IO.File.Delete(archiveFilePathDemograficos);
                }
                System.IO.File.Move(demograficosCsvPath, archiveFilePathDemograficos);
                _logger.LogInformation($"Exported file (Demograficos.csv) moved to archive: {archiveFilePathDemograficos}");
                logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Exported file (Demograficos.csv) moved to archive: {archiveFilePathDemograficos}");

                // Mover archivo exportado de dwtCreditDeferral (amortizaciones.csv) a HISTORIC FILES\Archive
                string amortizacionesCsvPath = Path.Combine(_filePath, "amortizaciones.csv");
                string archiveFolderAmortizaciones = Path.Combine(@"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES", "Archive");
                if (!Directory.Exists(archiveFolderAmortizaciones))
                {
                    Directory.CreateDirectory(archiveFolderAmortizaciones);
                }
                string archiveFilePathAmortizaciones = Path.Combine(archiveFolderAmortizaciones, Path.GetFileName(amortizacionesCsvPath));
                if (System.IO.File.Exists(archiveFilePathAmortizaciones))
                {
                    System.IO.File.Delete(archiveFilePathAmortizaciones);
                }
                System.IO.File.Move(amortizacionesCsvPath, archiveFilePathAmortizaciones);
                _logger.LogInformation($"Exported file (amortizaciones.csv) moved to archive: {archiveFilePathAmortizaciones}");
                logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Exported file (amortizaciones.csv) moved to archive: {archiveFilePathAmortizaciones}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during post-process cleanup.");
                logBuilder.AppendLine($"Error during post-process cleanup: {ex.Message}");
            }
            
            return Ok("Backup process completed successfully.");
        }
        else
        {
            await WriteLog(logBuilder.ToString(), logPath);
            return StatusCode(500, "Backup process failed.");
        }
    }

    private async Task<bool> BulkInsertToMySQL(string csvFilePath, StringBuilder logBuilder, string logPath)
    {
        try
        {
            using (var connection = new MySqlConnection(_mysqlConnectionString))
            {
                await connection.OpenAsync();
                // Limpiar tabla B1_Demograficos
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
                                      "IGNORE 0 LINES;";
                using (var loadCmd = new MySqlCommand(loadCommandText, connection))
                {
                    // Aumentar timeout a 600 segundos
                    loadCmd.CommandTimeout = 600;
                    int rowsInserted = await loadCmd.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Bulk insert (B1_Demograficos): {rowsInserted} rows inserted.");
                    if (rowsInserted == 0)
                    {
                        logBuilder.AppendLine("ERR301: No data inserted into B1_Demograficos.");
                        await WriteLog(logBuilder.ToString(), logPath);
                        return false;
                    }
                }
            }
            return true;
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"ERR302: Bulk insert error (B1_Demograficos) - {ex.Message}");
            _logger.LogError(ex, "Bulk insert error (B1_Demograficos).");
            await WriteLog(logBuilder.ToString(), logPath);
            return false;
        }
    }

    private async Task<bool> BulkInsertToAmortizaciones(string csvFilePath, StringBuilder logBuilder, string logPath)
    {
        try
        {
            using (var connection = new MySqlConnection(_mysqlConnectionString))
            {
                await connection.OpenAsync();
                // Limpiar tabla B2_Stage_Amortizaciones
                string truncateTable = "TRUNCATE TABLE B2_Stage_Amortizaciones;";
                using (var truncateCmd = new MySqlCommand(truncateTable, connection))
                {
                    await truncateCmd.ExecuteNonQueryAsync();
                }
                var loadCommandText = $"LOAD DATA LOCAL INFILE '{csvFilePath.Replace("\\", "\\\\")}' " +
                                      "INTO TABLE B2_Stage_Amortizaciones " +
                                      "FIELDS TERMINATED BY '|' " +
                                      "ENCLOSED BY '\"' " +
                                      "LINES TERMINATED BY '\\n' " +
                                      "IGNORE 0 LINES;";
                using (var loadCmd = new MySqlCommand(loadCommandText, connection))
                {
                    // Aumentar timeout a 600 segundos
                    loadCmd.CommandTimeout = 600;
                    int rowsInserted = await loadCmd.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Bulk insert (B2_Stage_Amortizaciones): {rowsInserted} rows inserted.");
                    if (rowsInserted == 0)
                    {
                        logBuilder.AppendLine("ERR303: No data inserted into B2_Stage_Amortizaciones.");
                        await WriteLog(logBuilder.ToString(), logPath);
                        return false;
                    }
                }
            }
            return true;
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"ERR304: Bulk insert error (B2_Stage_Amortizaciones) - {ex.Message}");
            _logger.LogError(ex, "Bulk insert error (B2_Stage_Amortizaciones).");
            await WriteLog(logBuilder.ToString(), logPath);
            return false;
        }
    }

    private async Task WriteLog(string content, string logPath)
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

    private void MoveExistingLog(string logPath, string historicLogsFolder)
    {
        if (System.IO.File.Exists(logPath))
        {
            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string historicLogPath = Path.Combine(historicLogsFolder, $"{Path.GetFileNameWithoutExtension(logPath)}_{timestamp}.log");
            System.IO.File.Move(logPath, historicLogPath);
        }
    }
}
