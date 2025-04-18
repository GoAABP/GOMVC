using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

public class D6_Quebrantos_Controller : Controller
{
    private readonly ILogger<D6_Quebrantos_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;

    // Directorios base
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";
    private readonly string _logsFolder = @"C:\Users\Go Credit\Documents\DATA\LOGS";

    public D6_Quebrantos_Controller(ILogger<D6_Quebrantos_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
    }

    [HttpPost]
    public async Task<IActionResult> D6_ProcessQuebrantos()
    {
        var logPath = Path.Combine(_logsFolder, "D6_Quebrantos.log");
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - D6 process started.");
        _logger.LogInformation("D6 process started.");

        var files = Directory.GetFiles(_filePath, "Quebrantos datos Cobranza_*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No files matching 'Quebrantos datos Cobranza_*.csv' found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D6_WriteLog(logBuilder.ToString(), logPath);
            return NotFound(errorLog);
        }

        var archiveFolder = Path.Combine(_historicFilePath, "Archive");
        var processedFolder = Path.Combine(_historicFilePath, "Processed");
        var errorFolder = Path.Combine(_historicFilePath, "Error");

        foreach (var file in files)
        {
            logBuilder.AppendLine($"Processing file: {file}");
            string convertedFilePath = null;
            string sanitizedFilePath = null;
            try
            {
                convertedFilePath = D6_ConvertToUTF8WithBOM(file);
                logBuilder.AppendLine($"Converted file to UTF-8 with BOM: {convertedFilePath}");

                sanitizedFilePath = D6_PreprocessCsvFile(convertedFilePath, logBuilder);
                logBuilder.AppendLine($"Sanitized file: {sanitizedFilePath}");

                await D6_BulkInsertToStage(sanitizedFilePath, logBuilder);
                await D6_InsertToFinalTable(logBuilder);

                D6_MoveFile(file, archiveFolder, logBuilder);
                D6_MoveFile(convertedFilePath, processedFolder, logBuilder);
                D6_MoveFile(sanitizedFilePath, processedFolder, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}");
                D6_MoveFile(file, errorFolder, logBuilder);
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - D6 process completed.");
        _logger.LogInformation("D6 process completed.");
        await D6_WriteLog(logBuilder.ToString(), logPath);

        D6_MoveLogToHistoric(logPath, Path.Combine(_historicFilePath, "Logs"));
        return Ok("D6 files processed successfully.");
    }

    private string D6_ConvertToUTF8WithBOM(string filePath)
    {
        var newFilePath = Path.Combine(
            Path.GetDirectoryName(filePath)!,
            Path.GetFileNameWithoutExtension(filePath) + "_utf8" + Path.GetExtension(filePath)
        );
        try
        {
            using (var reader = new StreamReader(filePath, Encoding.GetEncoding("Windows-1252")))
            using (var writer = new StreamWriter(newFilePath, false, new UTF8Encoding(true)))
            {
                while (!reader.EndOfStream)
                {
                    writer.WriteLine(reader.ReadLine());
                }
            }
            _logger.LogInformation($"File successfully converted to UTF-8 with BOM: {newFilePath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error converting file to UTF-8 with BOM: {filePath}");
            throw;
        }
        return newFilePath;
    }

    private string D6_PreprocessCsvFile(string inputFilePath, StringBuilder logBuilder)
    {
        var sanitizedFilePath = Path.Combine(
            Path.GetDirectoryName(inputFilePath)!,
            Path.GetFileNameWithoutExtension(inputFilePath) + "_sanitized.csv"
        );
        try
        {
            using (var reader = new StreamReader(inputFilePath))
            using (var writer = new StreamWriter(sanitizedFilePath))
            {
                string line;
                bool headerProcessed = false;
                while ((line = reader.ReadLine()) != null)
                {
                    if (!headerProcessed)
                    {
                        writer.WriteLine(line);
                        headerProcessed = true;
                        continue;
                    }

                    var columns = line.Split(',');
                    if (columns[0].Trim() == "999999999")
                    {
                        logBuilder.AppendLine("Stopper '999999999' found in the first column. Stopping further processing.");
                        break;
                    }
                    writer.WriteLine(line);
                }
            }
            logBuilder.AppendLine($"File sanitized successfully: {sanitizedFilePath}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error during file sanitization: {ex.Message}");
            throw;
        }
        return sanitizedFilePath;
    }

    private async Task D6_BulkInsertToStage(string csvFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D6_Stage_Quebrantos;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D6_Stage_Quebrantos.");

                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{csvFilePath.Replace("\\", "\\\\")}' " +
                                            "INTO TABLE D6_Stage_Quebrantos " +
                                            "FIELDS TERMINATED BY ',' " +
                                            "ENCLOSED BY '\"' " +
                                            "LINES TERMINATED BY '\\n' " +
                                            "IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Bulk inserted data into D6_Stage_Quebrantos.");

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during bulk insert: {ex.Message}");
                    _logger.LogError(ex, "Error during bulk insert.");
                    throw;
                }
            }
        }
    }

    private async Task D6_InsertToFinalTable(StringBuilder logBuilder)
    {
        var sqlInsertCommand = @"
            INSERT INTO D6_Quebrantos (
                Operacion, Referencia, Nombre, Convenio, vFinancingtypeid, KVigente, KVencido, IntVencido,
                IVAIntVencido, IntVencidoCO, IVAIntVencidoCO, TotalQuebranto, PagosRealizados, SdoPendiente,
                IntXDevengar, SdoTotalXPagar, FechaQuebranto, UltPagoTeorico, UltimoPago, UltPagoApl,
                Gestor, nCommission, nCommTax, vMotive, FechaGenerado
            )
            SELECT 
                Operacion, Referencia, Nombre, Convenio, vFinancingtypeid, KVigente, KVencido, IntVencido,
                IVAIntVencido, IntVencidoCO, IVAIntVencidoCO, TotalQuebranto, PagosRealizados, SdoPendiente,
                IntXDevengar, SdoTotalXPagar,
                STR_TO_DATE(NULLIF(FechaQuebranto, ''), '%d/%m/%Y') AS FechaQuebranto,
                STR_TO_DATE(NULLIF(UltPagoTeorico, ''), '%d/%m/%Y') AS UltPagoTeorico,
                STR_TO_DATE(NULLIF(UltimoPago, ''), '%d/%m/%Y') AS UltimoPago,
                STR_TO_DATE(NULLIF(UltPagoApl, ''), '%d/%m/%Y') AS UltPagoApl,
                Gestor, nCommission, nCommTax, vMotive, NOW() AS FechaGenerado
            FROM D6_Stage_Quebrantos;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    var rowsAffected = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Inserted {rowsAffected} rows into D6_Quebrantos.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during insert: {ex.Message}");
                    _logger.LogError(ex, "Error during insert.");
                    throw;
                }
            }
        }
    }

    private void D6_MoveFile(string sourceFilePath, string destinationFolder, StringBuilder logBuilder)
    {
        try
        {
            if (!Directory.Exists(destinationFolder))
            {
                Directory.CreateDirectory(destinationFolder);
            }
            var destinationFilePath = Path.Combine(destinationFolder, Path.GetFileName(sourceFilePath));
            if (System.IO.File.Exists(destinationFilePath))
            {
                System.IO.File.Delete(destinationFilePath);
            }
            System.IO.File.Move(sourceFilePath, destinationFilePath);
            logBuilder.AppendLine($"Moved file: {sourceFilePath} -> {destinationFilePath}");
            _logger.LogInformation($"Moved file: {sourceFilePath} -> {destinationFilePath}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error moving file {sourceFilePath} to {destinationFolder}: {ex.Message}");
            _logger.LogError(ex, $"Error moving file {sourceFilePath} to {destinationFolder}");
        }
    }

    private async Task D6_WriteLog(string content, string logPath)
    {
        try
        {
            var directory = Path.GetDirectoryName(logPath);
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory!);
            }
            await System.IO.File.WriteAllTextAsync(logPath, content);
            _logger.LogInformation($"Log written to: {logPath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error writing log to {logPath}");
            throw;
        }
    }

    private void D6_MoveLogToHistoric(string logPath, string historicLogsFolder)
    {
        try
        {
            if (!System.IO.File.Exists(logPath))
            {
                _logger.LogWarning($"Log file does not exist: {logPath}");
                return;
            }
            if (!Directory.Exists(historicLogsFolder))
            {
                Directory.CreateDirectory(historicLogsFolder);
            }
            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var logFileName = Path.GetFileNameWithoutExtension(logPath) + $"_{timestamp}" + Path.GetExtension(logPath);
            var destinationFilePath = Path.Combine(historicLogsFolder, logFileName);
            System.IO.File.Move(logPath, destinationFilePath);
            _logger.LogInformation($"Log file moved to historic folder: {destinationFilePath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error moving log file to historic folder: {ex.Message}");
            throw;
        }
    }

    [HttpPost]
    public async Task<IActionResult> D6_ProcessHistoricQuebrantos()
    {
        var logPath = Path.Combine(_logsFolder, "D6_Historic_Quebrantos.log");
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process started.");
        _logger.LogInformation("Historic process started.");

        var files = Directory.GetFiles(_filePath, "Quebrantos datos Cobranza_*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No historic files matching 'Quebrantos datos Cobranza_*.csv' found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D6_WriteLog(logBuilder.ToString(), logPath);
            return NotFound(errorLog);
        }

        var archiveFolder = Path.Combine(_historicFilePath, "Archive");
        var processedFolder = Path.Combine(_historicFilePath, "Processed");
        var errorFolder = Path.Combine(_historicFilePath, "Error");

        foreach (var file in files)
        {
            logBuilder.AppendLine($"Processing file: {file}");
            try
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                var match = Regex.Match(fileName, @"Quebrantos datos Cobranza_(\d{2})(\d{2})(\d{4})");
                if (!match.Success)
                {
                    var errorLog = $"Invalid file name format: {fileName}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(errorLog);
                    D6_MoveFile(file, errorFolder, logBuilder);
                    continue;
                }
                var day = int.Parse(match.Groups[1].Value);
                var month = int.Parse(match.Groups[2].Value);
                var year = int.Parse(match.Groups[3].Value);
                var fechaGenerado = new DateTime(year, month, day);
                logBuilder.AppendLine($"Parsed FechaGenerado: {fechaGenerado} for file: {file}");

                var convertedFilePath = D6_ConvertToUTF8WithBOM(file);
                logBuilder.AppendLine($"Converted file to UTF-8 with BOM: {convertedFilePath}");

                var sanitizedFilePath = D6_PreprocessCsvFile(convertedFilePath, logBuilder);
                logBuilder.AppendLine($"Sanitized file: {sanitizedFilePath}");

                await D6_BulkInsertToStage(sanitizedFilePath, logBuilder);
                await D6_InsertHistoricData(fechaGenerado, logBuilder);

                D6_MoveFile(file, archiveFolder, logBuilder);
                D6_MoveFile(convertedFilePath, processedFolder, logBuilder);
                D6_MoveFile(sanitizedFilePath, processedFolder, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}");
                D6_MoveFile(file, errorFolder, logBuilder);
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process completed.");
        _logger.LogInformation("Historic process completed.");
        await D6_WriteLog(logBuilder.ToString(), logPath);

        D6_MoveLogToHistoric(logPath, Path.Combine(_historicFilePath, "Logs"));
        return Ok("Historic files processed successfully.");
    }

    private async Task D6_InsertHistoricData(DateTime fechaGenerado, StringBuilder logBuilder)
    {
        var sqlInsertCommand = @"
            INSERT INTO D6_Quebrantos (
                Operacion, Referencia, Nombre, Convenio, vFinancingtypeid, KVigente, KVencido, IntVencido,
                IVAIntVencido, IntVencidoCO, IVAIntVencidoCO, TotalQuebranto, PagosRealizados, SdoPendiente,
                IntXDevengar, SdoTotalXPagar, FechaQuebranto, UltPagoTeorico, UltimoPago, UltPagoApl,
                Gestor, nCommission, nCommTax, vMotive, FechaGenerado
            )
            SELECT 
                Operacion, Referencia, Nombre, Convenio, vFinancingtypeid, KVigente, KVencido, IntVencido,
                IVAIntVencido, IntVencidoCO, IVAIntVencidoCO, TotalQuebranto, PagosRealizados, SdoPendiente,
                IntXDevengar, SdoTotalXPagar,
                STR_TO_DATE(NULLIF(FechaQuebranto, ''), '%d/%m/%Y') AS FechaQuebranto,
                STR_TO_DATE(NULLIF(UltPagoTeorico, ''), '%d/%m/%Y') AS UltPagoTeorico,
                STR_TO_DATE(NULLIF(UltimoPago, ''), '%d/%m/%Y') AS UltimoPago,
                STR_TO_DATE(NULLIF(UltPagoApl, ''), '%d/%m/%Y') AS UltPagoApl,
                Gestor, nCommission, nCommTax, vMotive, @FechaGenerado
            FROM D6_Stage_Quebrantos;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    command.Parameters.AddWithValue("@FechaGenerado", fechaGenerado);
                    var rowsAffected = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Inserted {rowsAffected} rows into D6_Quebrantos.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during historic insert: {ex.Message}");
                    _logger.LogError(ex, "Error during historic insert.");
                    throw;
                }
            }
        }
    }

    [HttpPost]
    public async Task<IActionResult> D6_ProcessQuebrantosCalculationsAndExport()
    {
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started: Quebrantos Calculation & Export.");
        _logger.LogInformation("Quebrantos Calculation & Export process started.");

        try
        {
            // Step 1: Perform the Quebrantos Calculation (insert into R1_Quebrantos_Calculado)
            logBuilder.AppendLine("Executing Quebrantos calculations...");
            await D6_CalculateQuebrantos(logBuilder);
            logBuilder.AppendLine("Quebrantos calculations completed successfully.");

            // Step 2: Export the most recent data to CSV
            logBuilder.AppendLine("Executing export to CSV...");
            var exportResult = await D6_ExportQuebrantosToCSV();
            logBuilder.AppendLine($"Export result: {exportResult}");

            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.");
            _logger.LogInformation("Quebrantos Calculation & Export process completed.");

            return Ok("Quebrantos calculation and export completed successfully.");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error during process execution: {ex.Message}");
            _logger.LogError(ex, "Error in Quebrantos Calculation & Export process.");
            return StatusCode(500, "An error occurred while processing Quebrantos calculations and export.");
        }
    }

    private async Task<IActionResult> D6_ExportQuebrantosToCSV()
    {
        var logBuilder = new StringBuilder();
        var exportFolderPath = @"C:\Users\Go Credit\Documents\DATA\EXPORTS";
        var mostRecentDateQuery = "SELECT MAX(Fecha_Generado) FROM R1_Quebrantos_Calculado;";
        
        try
        {
            if (!Directory.Exists(exportFolderPath))
            {
                Directory.CreateDirectory(exportFolderPath);
            }

            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();

                DateTime mostRecentDate;
                using (var cmd = new MySqlCommand(mostRecentDateQuery, connection))
                {
                    var result = await cmd.ExecuteScalarAsync();
                    if (result == null || result == DBNull.Value)
                    {
                        logBuilder.AppendLine("No data found in R1_Quebrantos_Calculado.");
                        return new NotFoundObjectResult("No data available for export.");
                    }
                    mostRecentDate = Convert.ToDateTime(result);
                }

                logBuilder.AppendLine($"Exporting data for most recent Fecha_Generado: {mostRecentDate:yyyy-MM-dd HH:mm:ss}");

                var sqlQuery = @"
                    SELECT * 
                    FROM R1_Quebrantos_Calculado
                    WHERE Fecha_Generado = @MostRecentDate;";

                using (var command = new MySqlCommand(sqlQuery, connection))
                {
                    command.Parameters.AddWithValue("@MostRecentDate", mostRecentDate);
                    
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (!reader.HasRows)
                        {
                            logBuilder.AppendLine("No records found for the most recent date.");
                            return new NotFoundObjectResult("No records to export.");
                        }

                        var exportFilePath = Path.Combine(exportFolderPath, $"Quebrantos_Export_{mostRecentDate:yyyyMMdd_HHmmss}.csv");

                        using (var writer = new StreamWriter(exportFilePath, false, new UTF8Encoding(true)))
                        {
                            // Write header
                            var columnNames = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName);
                            await writer.WriteLineAsync(string.Join(",", columnNames));

                            // Write rows
                            while (await reader.ReadAsync())
                            {
                                var row = new List<string>();
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    var value = reader[i]?.ToString() ?? "";
                                    value = value.Replace("\"", "\"\"");
                                    row.Add($"\"{value}\"");
                                }
                                await writer.WriteLineAsync(string.Join(",", row));
                            }
                        }

                        logBuilder.AppendLine($"Export successful: {exportFilePath}");
                        return new OkObjectResult($"Exported successfully to: {exportFilePath}");
                    }
                }
            }
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error exporting data: {ex.Message}");
            _logger.LogError(ex, "Error exporting R1_Quebrantos_Calculado.");
            return new StatusCodeResult(500);
        }
    }

    // Método que realiza el cálculo e inserción en R1_Quebrantos_Calculado utilizando la nueva estructura y fórmulas
    private async Task D6_CalculateQuebrantos(StringBuilder logBuilder)
    {
        var sqlCommands = @"
            -- Step 1: Drop and recreate the temporary table
            DROP TEMPORARY TABLE IF EXISTS Temp_Latest_Quebrantos;
            CREATE TEMPORARY TABLE Temp_Latest_Quebrantos (
                Operacion INT,
                Referencia INT,
                Nombre VARCHAR(255),
                Convenio VARCHAR(255),
                vFinancingtypeid VARCHAR(50),
                KVigente DECIMAL(10, 2),
                KVencido DECIMAL(10, 2),
                IntVencido DECIMAL(10, 2),
                IVAIntVencido DECIMAL(10, 2),
                IntVencidoCO DECIMAL(10, 2),
                IVAIntVencidoCO DECIMAL(10, 2),
                TotalQuebranto DECIMAL(10, 2),
                PagosRealizados DECIMAL(10, 2),
                SdoPendiente DECIMAL(10, 2),
                IntXDevengar DECIMAL(10, 2),
                SdoTotalXPagar DECIMAL(10, 2),
                FechaQuebranto VARCHAR(10),
                UltPagoTeorico VARCHAR(10),
                UltimoPago VARCHAR(10),
                UltPagoApl VARCHAR(10),
                Gestor VARCHAR(255),
                nCommission DECIMAL(10, 2),
                nCommTax DECIMAL(10, 2),
                vMotive VARCHAR(255),
                INDEX idx_operacion (Operacion)
            ) ENGINE=InnoDB;

            -- Step 2: Insert records from D6_Quebrantos where FechaGenerado is the most recent
            INSERT INTO Temp_Latest_Quebrantos
            SELECT 
                q.Operacion,
                q.Referencia,
                q.Nombre,
                q.Convenio,
                q.vFinancingtypeid,
                q.KVigente,
                q.KVencido,
                q.IntVencido,
                q.IVAIntVencido,
                q.IntVencidoCO,
                q.IVAIntVencidoCO,
                q.TotalQuebranto,
                q.PagosRealizados,
                q.SdoPendiente,
                q.IntXDevengar,
                q.SdoTotalXPagar,
                q.FechaQuebranto,
                q.UltPagoTeorico,
                q.UltimoPago,
                q.UltPagoApl,
                q.Gestor,
                q.nCommission,
                q.nCommTax,
                q.vMotive
            FROM D6_Quebrantos q
            JOIN (
                SELECT Operacion, MAX(FechaGenerado) AS MostRecentFechaGenerado
                FROM D6_Quebrantos
                GROUP BY Operacion
            ) lfg 
              ON q.Operacion = lfg.Operacion 
              AND q.FechaGenerado = lfg.MostRecentFechaGenerado;

            -- Drop and recreate Temp_Total_Estrategias
            DROP TEMPORARY TABLE IF EXISTS Temp_Total_Estrategias;
            CREATE TEMPORARY TABLE Temp_Total_Estrategias (
                id_credito INT NOT NULL,
                total_pagos DECIMAL(18,2) NOT NULL DEFAULT 0
            );
            INSERT INTO Temp_Total_Estrategias (id_credito, total_pagos)
            SELECT 
                d.id_credito,
                COALESCE(SUM(d.pago), 0) AS total_pagos
            FROM d3_aplicacion_pagos d
            INNER JOIN ci1_pagos_estrategia_acumulados c ON d.id_pago = c.id_pago
            GROUP BY d.id_credito;

            -- Insert into R1_Quebrantos_Calculado using the new structure and calculations
            INSERT INTO R1_Quebrantos_Calculado (
                Operacion, 
                Referencia, 
                Nombre, 
                Convenio, 
                vFinancing_typeid, 
                K_Vigente, 
                K_Vencido, 
                Int_Vencido, 
                IVA_Int_Vencido, 
                Int_Vencido_CO, 
                IVA_Int_Vencido_CO, 
                Total_Quebranto, 
                Pagos_Realizados, 
                Sdo_Pendiente, 
                Int_X_Devengar, 
                Sdo_Total_X_Pagar, 
                Fecha_Quebranto, 
                Ult_Pago_Teorico, 
                Ultimo_Pago, 
                Ult_Pago_Apl, 
                Gestor, 
                nCommission, 
                nCommTax, 
                v_Motive, 
                Total_Estrategia, 
                Recuperacion, 
                Quebranto_Pagare, 
                Saldo_Q_Pagare, 
                Quebranto_Capital, 
                Saldo_Q_Capital, 
                Quebranto_Contable, 
                Saldo_Q_Contable, 
                Motivo, 
                Valid, 
                Month, 
                Year, 
                Producto, 
                Financiamiento, 
                Fecha_Generado
            )
            SELECT 
                t.Operacion,
                t.Referencia,
                t.Nombre,
                t.Convenio,
                t.vFinancingtypeid AS vFinancing_typeid,
                t.KVigente AS K_Vigente,
                t.KVencido AS K_Vencido,
                t.IntVencido AS Int_Vencido,
                t.IVAIntVencido AS IVA_Int_Vencido,
                t.IntVencidoCO AS Int_Vencido_CO,
                t.IVAIntVencidoCO AS IVA_Int_Vencido_CO,
                t.TotalQuebranto AS Total_Quebranto,
                t.PagosRealizados AS Pagos_Realizados,
                t.SdoPendiente AS Sdo_Pendiente,
                t.IntXDevengar AS Int_X_Devengar,
                t.SdoTotalXPagar AS Sdo_Total_X_Pagar,
                t.FechaQuebranto AS Fecha_Quebranto,
                t.UltPagoTeorico AS Ult_Pago_Teorico,
                t.UltimoPago AS Ultimo_Pago,
                t.UltPagoApl AS Ult_Pago_Apl,
                t.Gestor,
                t.nCommission,
                t.nCommTax,
                t.vMotive AS v_Motive,
                COALESCE(te.total_pagos, 0) AS Total_Estrategia,
                COALESCE(t.PagosRealizados, 0) - COALESCE(te.total_pagos, 0) AS Recuperacion,
                (COALESCE(t.SdoTotalXPagar, 0) + COALESCE(te.total_pagos, 0))
                  + (COALESCE(t.PagosRealizados, 0) - COALESCE(te.total_pagos, 0)) AS Quebranto_Pagare,
                COALESCE(t.SdoTotalXPagar, 0) + COALESCE(te.total_pagos, 0) AS Saldo_Q_Pagare,
                COALESCE(t.KVigente, 0) + COALESCE(t.KVencido, 0) AS Quebranto_Capital,
                (COALESCE(t.KVigente, 0) + COALESCE(t.KVencido, 0))
                  - (COALESCE(t.PagosRealizados, 0) - COALESCE(te.total_pagos, 0)) AS Saldo_Q_Capital,
                (COALESCE(t.KVigente, 0) + COALESCE(t.KVencido, 0)
                  + COALESCE(t.IntVencido, 0) + COALESCE(t.IVAIntVencido, 0)) AS Quebranto_Contable,
                (COALESCE(t.KVigente, 0) + COALESCE(t.KVencido, 0)
                  + COALESCE(t.IntVencido, 0) + COALESCE(t.IVAIntVencido, 0))
                  - (COALESCE(t.PagosRealizados, 0) - COALESCE(te.total_pagos, 0)) AS Saldo_Q_Contable,
                NULL AS Motivo,
                COUNT(*) OVER(PARTITION BY t.Operacion) AS Valid,
                MONTH(STR_TO_DATE(t.FechaQuebranto, '%Y-%m-%d')) AS Month,
                YEAR(STR_TO_DATE(t.FechaQuebranto, '%Y-%m-%d')) AS Year,
                COALESCE(cf.producto, 'Desconocido') AS Producto,
                t.vFinancingtypeid AS Financiamiento,
                NOW() AS Fecha_Generado
            FROM Temp_Latest_Quebrantos t
            LEFT JOIN Temp_Total_Estrategias te ON t.Operacion = te.id_credito
            LEFT JOIN c2_financiamiento cf ON t.vFinancingtypeid = cf.Tipo_Financiamiento;
        ";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlCommands, connection, transaction);
                    await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Quebrantos calculations completed successfully.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during Quebrantos calculations: {ex.Message}");
                    _logger.LogError(ex, "Error during Quebrantos calculations.");
                    throw;
                }
            }
        }
    }
}
