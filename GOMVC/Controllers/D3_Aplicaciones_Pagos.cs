using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
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

    public async Task<IActionResult> D3_ProcessAplicacionPagos()
    {
        var logPath = Path.Combine(_logsFolder, _logFileName);
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - D3 process started.");
        _logger.LogInformation("D3 process started.");

        // Obtener archivos según el patrón
        var files = Directory.GetFiles(_filePath, "Aplicacion de pagos por fecha de Aplica*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No files found matching the pattern.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D3_WriteLog(logBuilder.ToString(), logPath);
            return NotFound(errorLog);
        }

        foreach (var file in files)
        {
            // Variables para rutas de archivos derivados
            string? convertedFilePath = null;
            string? sanitizedFilePath = null;
            logBuilder.AppendLine($"Processing file: {file}");

            try
            {
                // Conversión a UTF-8 con BOM
                convertedFilePath = D3_ConvertToUTF8WithBOM(file);
                logBuilder.AppendLine($"Converted file to UTF-8 with BOM: {convertedFilePath}");

                // Preprocesamiento: stopper y sanitización
                sanitizedFilePath = D3_PreprocessCsvFile(convertedFilePath, logBuilder);
                logBuilder.AppendLine($"Sanitized file: {sanitizedFilePath}");

                // Carga de datos a la tabla de staging
                await D3_LoadDataToStage(sanitizedFilePath, logBuilder);

                // Inserción de datos validados a la tabla final
                await D3_InsertValidatedData(logBuilder);

                // Movimiento de archivos en caso de éxito
                D3_MoveFile(file, _archiveFolder);
                if (convertedFilePath != null)
                    D3_MoveFile(convertedFilePath, _processedFolder);
                if (sanitizedFilePath != null)
                    D3_MoveFile(sanitizedFilePath, _processedFolder);

                logBuilder.AppendLine($"File {file} processed successfully.");
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}");

                // Movimiento de archivos relacionados a la carpeta de error
                D3_MoveFile(file, _errorFolder);
                if (convertedFilePath != null && System.IO.File.Exists(convertedFilePath))
                    D3_MoveFile(convertedFilePath, _errorFolder);
                if (sanitizedFilePath != null && System.IO.File.Exists(sanitizedFilePath))
                    D3_MoveFile(sanitizedFilePath, _errorFolder);

                await D3_WriteLog(logBuilder.ToString(), logPath);
                // Lanzar la excepción para indicar fallo en el proceso
                throw;
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - D3 process completed.");
        _logger.LogInformation("D3 process completed.");
        await D3_WriteLog(logBuilder.ToString(), logPath);

        // Mover el log final a la carpeta de logs históricos
        D3_MoveLogToHistoric(logPath, _historicLogsFolder);

        return Ok("D3 process completed successfully.");
    }

    private string D3_ConvertToUTF8WithBOM(string filePath)
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

    private string D3_PreprocessCsvFile(string inputFilePath, StringBuilder logBuilder)
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
                    // Escribir la cabecera sin validación
                    if (!headerProcessed)
                    {
                        writer.WriteLine(line);
                        headerProcessed = true;
                        continue;
                    }

                    // Verificar stopper: si la primera columna es "0", detener el procesamiento
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

    private async Task D3_LoadDataToStage(string csvFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    // Truncar la tabla de staging
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D3_Stage_Aplicacion_Pagos;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D3_Stage_Aplicacion_Pagos.");

                    // Cargar datos a la tabla de staging
                    var loadCommandText = "LOAD DATA LOCAL INFILE '" +
                        csvFilePath.Replace("\\", "\\\\") +
                        "' INTO TABLE D3_Stage_Aplicacion_Pagos " +
                        "FIELDS TERMINATED BY ',' " +
                        "ENCLOSED BY '\"' " +
                        "LINES TERMINATED BY '\\n' " +
                        "IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
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

    private async Task D3_InsertValidatedData(StringBuilder logBuilder)
    {
        var sqlInsertCommand = @"
            INSERT INTO D3_Aplicacion_Pagos (
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
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Inserted validated data into D3_Aplicacion_Pagos.");
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
            _logger.LogInformation($"File moved: {sourceFilePath} -> {destinationFilePath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Failed to move file: {sourceFilePath} -> {destinationFolder}");
            throw;
        }
    }

    private async Task D3_WriteLog(string content, string logPath)
    {
        try
        {
            var directory = Path.GetDirectoryName(logPath);
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

    private void D3_MoveLogToHistoric(string logPath, string historicLogsFolder)
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
            var destinationFilePath = Path.Combine(historicLogsFolder,
                Path.GetFileNameWithoutExtension(logPath) + $"_{timestamp}" + Path.GetExtension(logPath));
            System.IO.File.Move(logPath, destinationFilePath);
            _logger.LogInformation($"Existing log moved to historic folder: {destinationFilePath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error moving existing log file: {ex.Message}");
        }
    }
}
