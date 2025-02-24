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
    
    // Directorios base
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _logsFolder = @"C:\Users\Go Credit\Documents\DATA\LOGS";
    private readonly string _historicLogsFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC LOGS";
    
    // Carpetas de movimiento dentro de Historic Files (para éxito y error)
    private readonly string _archiveFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Archive";
    private readonly string _processedFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Processed";
    private readonly string _errorFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Error";

    // Nombre dinámico del log basado en el controlador
    private readonly string _logFileName;

    public D4_Otorgamiento_Creditos_Controller(ILogger<D4_Otorgamiento_Creditos_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

        // Generar el nombre del log de forma dinámica usando el nombre del controlador.
        _logFileName = $"{nameof(D4_Otorgamiento_Creditos_Controller)}.log";
        var logPath = System.IO.Path.Combine(_logsFolder, _logFileName);
        // Si ya existe un log con ese nombre, moverlo a la carpeta histórica de logs.
        if (System.IO.File.Exists(logPath))
        {
            MoveExistingLog(logPath, _historicLogsFolder);
        }
    }

    public async Task<IActionResult> D4_ProcessOtorgamientoCreditos()
    {
        var logPath = System.IO.Path.Combine(_logsFolder, _logFileName);
        var logBuilder = new StringBuilder();
        bool hasErrors = false;

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - D4 process started.");
        _logger.LogInformation("D4 process started.");

        var files = Directory.GetFiles(_filePath, "BARTURO*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No files matching 'BARTURO*.csv' were found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D4_WriteLog(logBuilder.ToString(), logPath);
            return StatusCode(500, errorLog);
        }

        foreach (var file in files)
        {
            // Variables para archivos derivados
            string? convertedFilePath = null;
            string? sanitizedFilePath = null;
            logBuilder.AppendLine($"Processing file: {file}");

            try
            {
                // Paso 1: Convertir el archivo a UTF-8 con BOM.
                convertedFilePath = D4_ConvertToUTF8WithBOM(file);
                logBuilder.AppendLine($"Converted file to UTF-8 with BOM: {convertedFilePath}");

                // Paso 2: Preprocesar (sanitizar) el archivo.
                sanitizedFilePath = D4_PreprocessCsvFile(convertedFilePath, logBuilder);
                logBuilder.AppendLine($"Sanitized file: {sanitizedFilePath}");

                // Paso 3: Cargar los datos del archivo sanitizado a la tabla de staging.
                await D4_LoadDataToStage(sanitizedFilePath, logBuilder);

                // Paso 4: Insertar los datos validados en la tabla final.
                await D4_InsertValidatedData(logBuilder);

                // Paso 5: Movimiento de archivos en caso de éxito.
                D4_MoveFile(file, _archiveFolder); // Mover el archivo original.
                if (convertedFilePath != null)
                    D4_MoveFile(convertedFilePath, _processedFolder); // Mover el archivo convertido.
                if (sanitizedFilePath != null)
                    D4_MoveFile(sanitizedFilePath, _processedFolder); // Mover el archivo sanitizado.

                logBuilder.AppendLine($"File {file} processed successfully.");
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}");
                hasErrors = true;

                // Movimiento de todos los archivos relacionados a la carpeta de error.
                D4_MoveFile(file, _errorFolder);
                if (convertedFilePath != null && System.IO.File.Exists(convertedFilePath))
                    D4_MoveFile(convertedFilePath, _errorFolder);
                if (sanitizedFilePath != null && System.IO.File.Exists(sanitizedFilePath))
                    D4_MoveFile(sanitizedFilePath, _errorFolder);

                await D4_WriteLog(logBuilder.ToString(), logPath);
                // Lanzar la excepción para asegurar que el proceso se considere fallido.
                throw;
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - D4 process completed.");
        _logger.LogInformation("D4 process completed.");
        await D4_WriteLog(logBuilder.ToString(), logPath);

        // Mover el log finalizado a la carpeta de logs históricos.
        D4_MoveLogToHistoric(logPath, _historicLogsFolder);

        return hasErrors
            ? StatusCode(500, "D4 process completed with errors. Check the log for details.")
            : Ok("D4 process completed successfully.");
    }

    private string D4_PreprocessCsvFile(string inputFilePath, StringBuilder logBuilder)
    {
        var sanitizedFilePath = System.IO.Path.Combine(
            System.IO.Path.GetDirectoryName(inputFilePath)!,
            System.IO.Path.GetFileNameWithoutExtension(inputFilePath) + "_sanitized.csv"
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
                    // Escribir la cabecera sin validación.
                    if (!headerProcessed)
                    {
                        writer.WriteLine(line);
                        headerProcessed = true;
                        continue;
                    }

                    // Verificar stopper: si la primera columna es "0", detener el procesamiento.
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
                    _logger.LogInformation("Executing validated data insert query using NOT EXISTS...");
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
        var newFilePath = System.IO.Path.Combine(
            System.IO.Path.GetDirectoryName(filePath)!,
            System.IO.Path.GetFileNameWithoutExtension(filePath) + "_utf8" + System.IO.Path.GetExtension(filePath)
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

            var destinationFilePath = System.IO.Path.Combine(destinationFolder, System.IO.Path.GetFileName(sourceFilePath));
            if (System.IO.File.Exists(destinationFilePath))
            {
                System.IO.File.Delete(destinationFilePath); // Sobrescribir si existe
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
        try
        {
            var directory = System.IO.Path.GetDirectoryName(logPath);
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory!);
            }

            await System.IO.File.WriteAllTextAsync(logPath, content);
            _logger.LogInformation($"Log written to {logPath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to write log file.");
            throw;
        }
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
            var logFileName = System.IO.Path.GetFileNameWithoutExtension(logPath) + $"_{timestamp}" + System.IO.Path.GetExtension(logPath);
            var destinationFilePath = System.IO.Path.Combine(historicLogsFolder, logFileName);

            System.IO.File.Move(logPath, destinationFilePath);
            _logger.LogInformation($"Log file moved to historic logs: {destinationFilePath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to move log file to historic logs.");
            throw;
        }
    }

    private void MoveExistingLog(string logPath, string historicLogsFolder)
    {
        try
        {
            if (!Directory.Exists(historicLogsFolder))
            {
                Directory.CreateDirectory(historicLogsFolder);
            }
            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var destinationFilePath = System.IO.Path.Combine(historicLogsFolder,
                System.IO.Path.GetFileNameWithoutExtension(logPath) + $"_{timestamp}" + System.IO.Path.GetExtension(logPath));
            System.IO.File.Move(logPath, destinationFilePath);
            _logger.LogInformation($"Existing log moved to historic folder: {destinationFilePath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error moving existing log file: {ex.Message}");
        }
    }
}
