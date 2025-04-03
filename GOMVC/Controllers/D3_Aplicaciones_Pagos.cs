using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

public class D3_Aplicaciones_Pagos_Controller : Controller
{
    private readonly ILogger<D3_Aplicaciones_Pagos_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    
    // Directorios base
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _logsFolder = @"C:\Users\Go Credit\Documents\DATA\LOGS";
    private readonly string _historicLogsFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC LOGS";
    
    // Carpetas de movimiento dentro de Historic Files
    private readonly string _archiveFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Archive";
    private readonly string _processedFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Processed";
    private readonly string _errorFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Error";

    // Nombre dinámico del log
    private readonly string _logFileName;

    public D3_Aplicaciones_Pagos_Controller(ILogger<D3_Aplicaciones_Pagos_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

        // Se genera el nombre del log usando el nombre del controlador
        _logFileName = $"{nameof(D3_Aplicaciones_Pagos_Controller)}.log";
        var logPath = Path.Combine(_logsFolder, _logFileName);
        // Si ya existe un log, se mueve a la carpeta de logs históricos
        if (System.IO.File.Exists(logPath))
        {
            MoveExistingLog(logPath, _historicLogsFolder);
        }
    }

    [HttpPost]
    public async Task<IActionResult> D3_ProcessAplicacionPagos()
    {
        // --- Inicia mejoras en el pipeline ---
        // Obtener o generar un Correlation ID para trazabilidad
        string correlationId = HttpContext?.Request?.Headers.ContainsKey("X-Correlation-ID") == true
            ? HttpContext.Request.Headers["X-Correlation-ID"].ToString()
            : Guid.NewGuid().ToString();

        // Iniciar medición de rendimiento
        var stopwatch = Stopwatch.StartNew();
        // --- Fin mejoras en pipeline ---

        var logPath = Path.Combine(_logsFolder, _logFileName);
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} [{correlationId}] - D3 process started.");
        _logger.LogInformation("[{CorrelationId}] D3 process started.", correlationId);

        try
        {
            // 1. Obtener archivos según el patrón
            var files = Directory.GetFiles(_filePath, "Aplicacion de pagos por fecha de Aplica*.csv");
            if (files.Length == 0)
            {
                var errorLog = "No files found matching the pattern.";
                logBuilder.AppendLine($"[{correlationId}] {errorLog}");
                _logger.LogError("[{CorrelationId}] {ErrorLog}", correlationId, errorLog);
                await D3_WriteLogAsync(logBuilder.ToString(), logPath);
                return NotFound(errorLog);
            }

            // 2. Procesamiento de cada archivo
            foreach (var file in files)
            {
                string? convertedFilePath = null;
                string? sanitizedFilePath = null;
                logBuilder.AppendLine($"[{correlationId}] Processing file: {file}");

                try
                {
                    // 2.1 Conversión a UTF-8 con BOM (transforma el archivo para MySQL)
                    convertedFilePath = await D3_ConvertToUTF8WithBOMAsync(file);
                    logBuilder.AppendLine($"[{correlationId}] Converted file to UTF-8 with BOM: {convertedFilePath}");

                    // 2.2 Preprocesamiento: stopper y sanitización (se omiten líneas si se cumple la condición)
                    sanitizedFilePath = await D3_PreprocessCsvFileAsync(convertedFilePath, logBuilder);
                    logBuilder.AppendLine($"[{correlationId}] Sanitized file: {sanitizedFilePath}");

                    // 2.3 Carga de datos a la tabla de staging (usando transacciones)
                    await D3_LoadDataToStageAsync(sanitizedFilePath, logBuilder);

                    // 2.4 Inserción de datos validados a la tabla final D3
                    await D3_InsertValidatedDataAsync(logBuilder);

                    // 2.5 Inserción en D3B: se insertan solo registros nuevos (validando por Id_Pago)
                    await D3_InsertIntoD3BAsync(logBuilder);

                    // 2.6 Movimiento de archivos:
                    // - Archivo original se mueve a Archive.
                    // - Archivos derivados (convertidos y sanitizados) se mueven a Processed.
                    D3_MoveFile(file, _archiveFolder);
                    if (convertedFilePath != null)
                        D3_MoveFile(convertedFilePath, _processedFolder);
                    if (sanitizedFilePath != null)
                        D3_MoveFile(sanitizedFilePath, _processedFolder);

                    logBuilder.AppendLine($"[{correlationId}] File {file} processed successfully.");
                }
                catch (Exception ex)
                {
                    logBuilder.AppendLine($"[{correlationId}] Error processing file {file}: {ex.Message}");
                    _logger.LogError(ex, "[{CorrelationId}] Error processing file {File}", correlationId, file);

                    // Movimiento de archivos a carpeta Error
                    D3_MoveFile(file, _errorFolder);
                    if (convertedFilePath != null && System.IO.File.Exists(convertedFilePath))
                        D3_MoveFile(convertedFilePath, _errorFolder);
                    if (sanitizedFilePath != null && System.IO.File.Exists(sanitizedFilePath))
                        D3_MoveFile(sanitizedFilePath, _errorFolder);

                    await D3_WriteLogAsync(logBuilder.ToString(), logPath);
                    throw;
                }
            }

            stopwatch.Stop();
            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} [{correlationId}] - D3 process completed in {stopwatch.ElapsedMilliseconds} ms.");
            _logger.LogInformation("[{CorrelationId}] D3 process completed in {ElapsedMilliseconds} ms.", correlationId, stopwatch.ElapsedMilliseconds);
            await D3_WriteLogAsync(logBuilder.ToString(), logPath);

            // Mover el log final a la carpeta de logs históricos
            D3_MoveLogToHistoric(logPath, _historicLogsFolder);

            // Se retorna 200 solo si el proceso se ejecuta completamente y sin errores.
            return Ok("D3 process completed successfully.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[{CorrelationId}] Process failed.", correlationId);
            return StatusCode(500, $"[{correlationId}] An error occurred during processing.");
        }
    }

    // --- Métodos de transformación y carga ---

    // Conversión asíncrona a UTF-8 con BOM
    private async Task<string> D3_ConvertToUTF8WithBOMAsync(string filePath)
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
                var line = await reader.ReadLineAsync();
                await writer.WriteLineAsync(line);
            }
        }
        return newFilePath;
    }

    // Sanitización asíncrona del archivo CSV (incluye stopper para omitir líneas a partir de "0")
    private async Task<string> D3_PreprocessCsvFileAsync(string inputFilePath, StringBuilder logBuilder)
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
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (!headerProcessed)
                    {
                        await writer.WriteLineAsync(line);
                        headerProcessed = true;
                        continue;
                    }
                    var columns = line.Split(',');
                    if (columns[0].Trim() == "0")
                    {
                        logBuilder.AppendLine("Stopper '0' found in the first column. Stopping further processing.");
                        break;
                    }
                    await writer.WriteLineAsync(line);
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

    // Carga asíncrona de datos a la tabla de staging (transacción y LOAD DATA)
    private async Task D3_LoadDataToStageAsync(string csvFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    // Limpiar la tabla staging
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D3_Stage_Aplicacion_Pagos;", connection, transaction)
                    {
                        CommandTimeout = 600
                    };
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D3_Stage_Aplicacion_Pagos.");

                    // Cargar datos desde el archivo CSV
                    var loadCommandText = "LOAD DATA LOCAL INFILE '" +
                        csvFilePath.Replace("\\", "\\\\") +
                        "' INTO TABLE D3_Stage_Aplicacion_Pagos " +
                        "FIELDS TERMINATED BY ',' " +
                        "ENCLOSED BY '\"' " +
                        "LINES TERMINATED BY '\\n' " +
                        "IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction)
                    {
                        CommandTimeout = 3600
                    };
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Loaded data into D3_Stage_Aplicacion_Pagos.");

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

    // Inserción asíncrona de datos validados a la tabla final D3 (transacción)
    private async Task D3_InsertValidatedDataAsync(StringBuilder logBuilder)
    {
        var sqlInsertCommand = @"
            INSERT INTO godatabase.D3_Aplicacion_Pagos (
                Id_Credito, Id_Convenio, Convenio, Referencia, Id_Pago, Nombre_Cliente, Financiamiento,
                Origen_de_Movimiento, Fecha_Pago, Fecha_Aplicacion, Fecha_Deposito, Status, Pago, Capital,
                Interes, IVA_Int, Comision_Financiada, IVA_Comision_Financ, Moratorios, IVA_Mora, Pago_Tardio,
                IVA_PagoTardio, Recuperacion, IVA_Recup, Com_Liquidacion, IVA_Com_Liquidacion, Retencion_X_Admon,
                IVA_Retencion_X_Admon, Pago_Exceso, Gestor, Forma_de_pago, vMotive
            )
            SELECT 
                Id_Credito, Id_Convenio, Convenio, Referencia, Id_Pago, Nombre_Cliente, Financiamiento,
                Origen_de_Movimiento,
                STR_TO_DATE(NULLIF(Fecha_Pago, ''), '%d/%m/%Y') AS Fecha_Pago,
                STR_TO_DATE(NULLIF(Fecha_Aplicacion, ''), '%d/%m/%Y') AS Fecha_Aplicacion,
                STR_TO_DATE(NULLIF(Fecha_Deposito, ''), '%d/%m/%Y') AS Fecha_Deposito,
                Status, Pago, Capital, Interes, IVA_Int, Comision_Financiada, IVA_Comision_Financ, 
                Moratorios, IVA_Mora, Pago_Tardio, IVA_PagoTardio, Recuperacion, IVA_Recup, 
                Com_Liquidacion, IVA_Com_Liquidacion, Retencion_X_Admon, IVA_Retencion_X_Admon, 
                Pago_Exceso, Gestor, Forma_de_pago, vMotive
            FROM D3_Stage_Aplicacion_Pagos
            WHERE NOT EXISTS (
                SELECT 1 FROM D3_Aplicacion_Pagos WHERE D3_Aplicacion_Pagos.Id_Pago = D3_Stage_Aplicacion_Pagos.Id_Pago
            );";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction)
                    {
                        CommandTimeout = 3600
                    };
                    await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Inserted validated data into D3_Aplicacion_Pagos.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Exception in D3_InsertValidatedDataAsync. Connection state: {State}", connection.State);
                    logBuilder.AppendLine($"[DEBUG] Exception in D3_InsertValidatedDataAsync. Connection state: {connection.State}");
                    logBuilder.AppendLine($"[DEBUG] Exception details: {ex}");
                    try
                    {
                        if (connection.State == System.Data.ConnectionState.Open)
                        {
                            await transaction.RollbackAsync();
                        }
                    }
                    catch (Exception rbEx)
                    {
                        _logger.LogError(rbEx, "Rollback failed in D3_InsertValidatedDataAsync.");
                        logBuilder.AppendLine($"[DEBUG] Rollback exception: {rbEx}");
                    }
                    logBuilder.AppendLine($"Error inserting data: {ex.Message}");
                    _logger.LogError(ex, "Error inserting data.");
                    throw;
                }
            }
        }
    }

    // Inserción asíncrona en D3B de registros nuevos, usando Id_Pago como llave única (LEFT JOIN)
    private async Task D3_InsertIntoD3BAsync(StringBuilder logBuilder)
    {
        var sqlInsertD3B = @"
            INSERT INTO godatabase.D3B_Aplicacion_Pagos_Limpio
            (
              Id_Credito,
              Id_Convenio,
              Convenio,
              Referencia,
              Id_Pago,
              Nombre_Cliente,
              Financiamiento,
              Origen_de_Movimiento,
              Fecha_Pago,
              Fecha_Aplicacion,
              Fecha_Deposito,
              Status,
              Pago,
              Capital,
              Interes,
              IVA_Int,
              Comision_Financiada,
              IVA_Comision_Financ,
              Moratorios,
              IVA_Mora,
              Pago_Tardio,
              IVA_PagoTardio,
              Recuperacion,
              IVA_Recup,
              Com_Liquidacion,
              IVA_Com_Liquidacion,
              Retencion_X_Admon,
              IVA_Retencion_X_Admon,
              Pago_Exceso,
              Gestor,
              Forma_de_pago,
              vMotive,
              Canal_De_pago
            )
            SELECT 
              d3.Id_Credito,
              d3.Id_Convenio,
              d3.Convenio,
              d3.Referencia,
              d3.Id_Pago,
              d3.Nombre_Cliente,
              d3.Financiamiento,
              d3.Origen_de_Movimiento,
              d3.Fecha_Pago,
              d3.Fecha_Aplicacion,
              d3.Fecha_Deposito,
              d3.Status,
              d3.Pago,
              d3.Capital,
              d3.Interes,
              d3.IVA_Int,
              d3.Comision_Financiada,
              d3.IVA_Comision_Financ,
              d3.Moratorios,
              d3.IVA_Mora,
              d3.Pago_Tardio,
              d3.IVA_PagoTardio,
              d3.Recuperacion,
              d3.IVA_Recup,
              d3.Com_Liquidacion,
              d3.IVA_Com_Liquidacion,
              d3.Retencion_X_Admon,
              d3.IVA_Retencion_X_Admon,
              d3.Pago_Exceso,
              d3.Gestor,
              d3.Forma_de_pago,
              d3.vMotive,
              c7.Canal_de_Pago
            FROM godatabase.D3_Aplicacion_Pagos AS d3
            LEFT JOIN godatabase.D3B_Aplicacion_Pagos_Limpio AS d3b
              ON d3.Id_Pago = d3b.Id_Pago
            LEFT JOIN godatabase.C7_Canal_De_Pago AS c7
              ON d3.Origen_de_Movimiento = c7.Origen_de_Movimiento
            WHERE d3b.Id_Pago IS NULL
              AND (c7.Canal_de_Pago <> 'No Aplica' OR c7.Canal_de_Pago IS NULL)
              AND d3.Id_Pago NOT IN (
                SELECT Id_Pago FROM godatabase.CI1_Pagos_Estrategia_Acumulados
              );
        ";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertD3B, connection, transaction)
                    {
                        CommandTimeout = 3600
                    };
                    var rowsAffected = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Inserted {rowsAffected} new record(s) into D3B_Aplicacion_Pagos_Limpio.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error inserting records into D3B_Aplicacion_Pagos_Limpio. Connection state: {State}", connection.State);
                    logBuilder.AppendLine($"[DEBUG] Exception details: {ex}");
                    try
                    {
                        if (connection.State == System.Data.ConnectionState.Open)
                        {
                            await transaction.RollbackAsync();
                        }
                    }
                    catch (Exception rbEx)
                    {
                        _logger.LogError(rbEx, "Rollback failed in D3_InsertIntoD3BAsync.");
                        logBuilder.AppendLine($"[DEBUG] Rollback exception: {rbEx}");
                    }
                    throw;
                }
            }
        }
    }

    // --- Métodos para mover archivos y logs ---

    // Movimiento de archivos entre carpetas (Archive, Processed, Error)
    private void D3_MoveFile(string sourceFilePath, string destinationFolder)
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
            _logger.LogInformation("File moved: {Source} -> {Destination}", sourceFilePath, destinationFilePath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to move file: {Source} -> {Destination}", sourceFilePath, destinationFolder);
            throw;
        }
    }

    // Escritura asíncrona del log en disco
    private async Task D3_WriteLogAsync(string content, string logPath)
    {
        try
        {
            var directory = Path.GetDirectoryName(logPath);
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory!);
            }
            await System.IO.File.WriteAllTextAsync(logPath, content);
            _logger.LogInformation("Log written to {LogPath}", logPath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to write log file.");
            throw;
        }
    }

    // Movimiento del log final a la carpeta de logs históricos
    private void D3_MoveLogToHistoric(string logPath, string historicLogsFolder)
    {
        try
        {
            if (!System.IO.File.Exists(logPath))
            {
                _logger.LogWarning("Log file does not exist: {LogPath}", logPath);
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
            _logger.LogInformation("Log file moved to historic logs: {DestinationFilePath}", destinationFilePath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to move log file to historic logs.");
            throw;
        }
    }

    // Movimiento de un log existente al iniciar el proceso
    private void MoveExistingLog(string logPath, string historicLogsFolder)
    {
        try
        {
            if (!Directory.Exists(historicLogsFolder))
            {
                Directory.CreateDirectory(historicLogsFolder);
            }
            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var destinationFilePath = Path.Combine(historicLogsFolder,
                Path.GetFileNameWithoutExtension(logPath) + $"_{timestamp}" + Path.GetExtension(logPath));
            System.IO.File.Move(logPath, destinationFilePath);
            _logger.LogInformation("Existing log moved to historic folder: {DestinationFilePath}", destinationFilePath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error moving existing log file: {Message}", ex.Message);
        }
    }
}
