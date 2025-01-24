using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

public class D4_Otorgamiento_Creditos_Controller : Controller
{
    private readonly ILogger<D4_Otorgamiento_Creditos_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";

    public D4_Otorgamiento_Creditos_Controller(ILogger<D4_Otorgamiento_Creditos_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance); // Handle special characters
    }

    public async Task<IActionResult> D4_ProcessOtorgamientoCreditos()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D4_Otorgamiento_Creditos.log";
        var archiveFolder = @"C:\Users\Go Credit\Documents\DATA\ARCHIVE";
        var processedFolder = @"C:\Users\Go Credit\Documents\DATA\PROCESSED";
        var errorFolder = @"C:\Users\Go Credit\Documents\DATA\ERROR";
        var logBuilder = new StringBuilder();
        var hasErrors = false;

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - D4 process started.");
        _logger.LogInformation("D4 process started.");

        var files = Directory.GetFiles(_filePath, "BARTURO*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No files matching 'BARTURO*.csv' were found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D4_WriteLog(logBuilder.ToString(), logPath);
            return StatusCode(500, errorLog); // Return a 500 error if no files are found
        }

        foreach (var file in files)
        {
            logBuilder.AppendLine($"Processing file: {file}");

            try
            {
                // Step 1: Convert file to UTF-8 with BOM
                var convertedFilePath = D4_ConvertToUTF8WithBOM(file);
                logBuilder.AppendLine($"Converted file to UTF-8 with BOM: {convertedFilePath}");

                // Step 2: Preprocess (sanitize) the file
                var sanitizedFilePath = D4_PreprocessCsvFile(convertedFilePath, logBuilder);
                logBuilder.AppendLine($"Sanitized file: {sanitizedFilePath}");

                // Step 3: Load sanitized data into stage table
                await D4_LoadDataToStage(sanitizedFilePath, logBuilder);

                // Step 4: Insert validated data into the final table
                await D4_InsertValidatedData(logBuilder);

                // Step 5: Move files to archive/processed folders upon successful processing
                D4_MoveFile(file, archiveFolder); // Move original file
                D4_MoveFile(convertedFilePath, processedFolder); // Move UTF-8 file
                D4_MoveFile(sanitizedFilePath, processedFolder); // Move sanitized file

                logBuilder.AppendLine($"File {file} processed successfully.");
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}");
                hasErrors = true;

                // Move problematic files to error folder for debugging
                D4_MoveFile(file, errorFolder);
            }
        }

        // Log completion
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - D4 process completed.");
        _logger.LogInformation("D4 process completed.");
        await D4_WriteLog(logBuilder.ToString(), logPath);

        // Decide the response based on whether errors occurred
        if (hasErrors)
        {
            return StatusCode(500, "D4 process completed with errors. Check the log for details.");
        }

        return Ok("D4 process completed successfully."); // Success
    }

    private string D4_PreprocessCsvFile(string inputFilePath, StringBuilder logBuilder)
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
                string? line;
                bool headerProcessed = false;

                while ((line = reader.ReadLine()) != null)
                {
                    // Write header row without validation
                    if (!headerProcessed)
                    {
                        writer.WriteLine(line);
                        headerProcessed = true;
                        continue;
                    }

                    // Split the line and check for stopper
                    var columns = line.Split(',');
                    if (columns[0].Trim() == "0")
                    {
                        logBuilder.AppendLine("Stopper '0' found in the first column. Stopping further processing.");
                        break;
                    }

                    writer.WriteLine(line);
                }
            }

            logBuilder.AppendLine($"File successfully sanitized: {sanitizedFilePath}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error during preprocessing: {ex.Message}");
            throw;
        }

        return sanitizedFilePath;
    }

    private async Task D4_LoadDataToStage(string csvFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D4_Stage_Otorgamiento_Creditos;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D4_Stage_Otorgamiento_Creditos.");

                    var loadCommandText = "LOAD DATA LOCAL INFILE '" +
                        csvFilePath.Replace("\\", "\\\\") +
                        "' INTO TABLE D4_Stage_Otorgamiento_Creditos " +
                        "FIELDS TERMINATED BY ',' " +
                        "ENCLOSED BY '\"' " +
                        "LINES TERMINATED BY '\\n' " +
                        "IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Loaded data into D4_Stage_Otorgamiento_Creditos.");

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

    private async Task D4_InsertValidatedData(StringBuilder logBuilder)
    {
        var sqlInsertCommand = @"
            INSERT INTO D4_Otorgamiento_Creditos (
                Id_Credito, Referencia, Nombre, Fecha_Apertura, F_Cobro, Id_Convenio, Convenio, Id_Sucursal, Sucursal,
                Capital, Primer_Pago, Comision, IVA, Cobertura, IVA_Cobertura, Disposicion, Monto_Retenido, Pago_de_Deuda,
                Comision_Financiada, IVA_Comision_Financiada, Solicitud, Vendedor, Nombre_Vendedor, TipoVendedor, vSupervisorId,
                vSupName, Producto, Descripcion_Tasa, Persona, Plazo, Id_Producto, vCampaign, Tipo_de_Financiamiento,
                vFinancingTypeId, vAliado, vComisionable, vSolActivas
            )
            SELECT 
                s.Id_Credito, s.Referencia, s.Nombre,
                STR_TO_DATE(NULLIF(s.Fecha_Apertura, ''), '%d/%m/%Y') AS Fecha_Apertura,
                STR_TO_DATE(NULLIF(s.F_Cobro, ''), '%d/%m/%Y') AS F_Cobro,
                s.Id_Convenio, s.Convenio, s.Id_Sucursal, s.Sucursal,
                s.Capital, STR_TO_DATE(NULLIF(s.Primer_Pago, ''), '%d/%m/%Y') AS Primer_Pago, s.Comision, s.IVA, s.Cobertura,
                s.IVA_Cobertura, s.Disposicion, s.Monto_Retenido, s.Pago_de_Deuda, s.Comision_Financiada, s.IVA_Comision_Financiada,
                s.Solicitud, s.Vendedor, s.Nombre_Vendedor, s.TipoVendedor, s.vSupervisorId, s.vSupName, s.Producto,
                s.Descripcion_Tasa, s.Persona, s.Plazo, s.Id_Producto, s.vCampaign, s.Tipo_de_Financiamiento,
                s.vFinancingTypeId, s.vAliado, s.vComisionable, s.vSolActivas
            FROM D4_Stage_Otorgamiento_Creditos s
            WHERE NOT EXISTS (
                SELECT 1 
                FROM D4_Otorgamiento_Creditos t
                WHERE 
                    s.Id_Credito = t.Id_Credito
                    AND s.Referencia = t.Referencia
                    AND s.Nombre = t.Nombre
                    AND STR_TO_DATE(NULLIF(s.Fecha_Apertura, ''), '%d/%m/%Y') = t.Fecha_Apertura
                    AND STR_TO_DATE(NULLIF(s.F_Cobro, ''), '%d/%m/%Y') = t.F_Cobro
                    AND s.Id_Convenio = t.Id_Convenio
                    AND s.Convenio = t.Convenio
                    AND s.Id_Sucursal = t.Id_Sucursal
                    AND s.Sucursal = t.Sucursal
                    AND s.Capital = t.Capital
                    AND STR_TO_DATE(NULLIF(s.Primer_Pago, ''), '%d/%m/%Y') = t.Primer_Pago
                    AND s.Comision = t.Comision
                    AND s.IVA = t.IVA
                    AND s.Cobertura = t.Cobertura
                    AND s.IVA_Cobertura = t.IVA_Cobertura
                    AND s.Disposicion = t.Disposicion
                    AND s.Monto_Retenido = t.Monto_Retenido
                    AND s.Pago_de_Deuda = t.Pago_de_Deuda
                    AND s.Comision_Financiada = t.Comision_Financiada
                    AND s.IVA_Comision_Financiada = t.IVA_Comision_Financiada
                    AND s.Solicitud = t.Solicitud
                    AND s.Vendedor = t.Vendedor
                    AND s.Nombre_Vendedor = t.Nombre_Vendedor
                    AND s.TipoVendedor = t.TipoVendedor
                    AND s.vSupervisorId = t.vSupervisorId
                    AND s.vSupName = t.vSupName
                    AND s.Producto = t.Producto
                    AND s.Descripcion_Tasa = t.Descripcion_Tasa
                    AND s.Persona = t.Persona
                    AND s.Plazo = t.Plazo
                    AND s.Id_Producto = t.Id_Producto
                    AND s.vCampaign = t.vCampaign
                    AND s.Tipo_de_Financiamiento = t.Tipo_de_Financiamiento
                    AND s.vFinancingTypeId = t.vFinancingTypeId
                    AND s.vAliado = t.vAliado
                    AND s.vComisionable = t.vComisionable
                    AND s.vSolActivas = t.vSolActivas
            );";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    logBuilder.AppendLine("Executing validated data insert query using NOT EXISTS...");

                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    var rowsAffected = await command.ExecuteNonQueryAsync();

                    logBuilder.AppendLine($"Inserted {rowsAffected} rows into D4_Otorgamiento_Creditos.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error inserting data: {ex.Message}");
                    _logger.LogError(ex, "Error inserting data.");
                    throw;
                }
            }
        }
    }

    private string D4_ConvertToUTF8WithBOM(string filePath)
    {
        var newFilePath = Path.Combine(
            Path.GetDirectoryName(filePath)!,
            Path.GetFileNameWithoutExtension(filePath) + "_utf8" + Path.GetExtension(filePath)
        );

        using (var reader = new StreamReader(filePath, Encoding.GetEncoding("Windows-1252")))
        using (var writer = new StreamWriter(newFilePath, false, new UTF8Encoding(true)))
        {
            while (!reader.EndOfStream)
            {
                writer.WriteLine(reader.ReadLine());
            }
        }

        return newFilePath;
    }

    private void D4_MoveFile(string sourceFilePath, string destinationFolder)
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
                System.IO.File.Delete(destinationFilePath); // Overwrite existing file
            }

            System.IO.File.Move(sourceFilePath, destinationFilePath);
            _logger.LogInformation($"File moved: {sourceFilePath} -> {destinationFilePath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Failed to move file: {sourceFilePath} -> {destinationFolder}");
            throw;
        }
    }

    private async Task D4_WriteLog(string content, string logPath)
    {
        await System.IO.File.WriteAllTextAsync(logPath, content);
    }

    private void D4_MoveLogToHistoric(string logPath, string historicLogsFolder)
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
            _logger.LogInformation($"Log file moved to historic logs: {destinationFilePath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to move log file to historic logs.");
        }
    }
}
