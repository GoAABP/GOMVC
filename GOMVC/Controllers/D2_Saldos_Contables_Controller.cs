using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

public class D2_Saldos_Contables_Controller : Controller
{
    private readonly ILogger<D2_Saldos_Contables_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;

    // Directorios base para archivos y logs
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilesFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";
    private readonly string _logsFolder = @"C:\Users\Go Credit\Documents\DATA\LOGS";
    private readonly string _historicLogsFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC LOGS";

    // Carpetas de movimiento dentro de Historic Files
    private readonly string _archiveFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Archive";
    private readonly string _processedFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Processed";
    private readonly string _errorFilesFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Error";

    // Nombre dinámico del log basado en el controlador
    private readonly string _logFileName;

    // Timeout para comandos MySQL (en segundos). Ajusta según necesidad.
    private const int MySqlCommandTimeout = 3600;

    public D2_Saldos_Contables_Controller(ILogger<D2_Saldos_Contables_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        _logFileName = $"{nameof(D2_Saldos_Contables_Controller)}.log";

        MoveExistingLogIfNeeded();
    }

    private void MoveExistingLogIfNeeded()
    {
        var logPath = Path.Combine(_logsFolder, _logFileName);
        if (System.IO.File.Exists(logPath))
        {
            MoveExistingLog(logPath, _historicLogsFolder);
        }
    }

    [HttpPost]
    public async Task<IActionResult> D2_ProcessSaldosContables()
    {
        var logPath = Path.Combine(_logsFolder, _logFileName);
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.");
        _logger.LogInformation("Process started.");

        var files = Directory.GetFiles(_filePath, "SDOSCONT*.csv");
        if (files.Length == 0)
        {
            string errorLog = "No files matching 'SDOSCONT*.csv' found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await WriteLogAsync(logBuilder.ToString(), logPath);
            return NotFound(errorLog);
        }

        foreach (var file in files)
        {
            string? convertedFilePath = null;
            string? sanitizedFilePath = null;
            logBuilder.AppendLine($"Processing file: {file}");

            try
            {
                convertedFilePath = ConvertToUTF8WithBOM(file);
                logBuilder.AppendLine($"Converted file to UTF-8 with BOM: {convertedFilePath}");

                sanitizedFilePath = PreprocessCsvFile(convertedFilePath, logBuilder);
                logBuilder.AppendLine($"Sanitized file: {sanitizedFilePath}");

                await BulkInsertToStageAsync(sanitizedFilePath, logBuilder);
                await InsertToFinalTableAsync(logBuilder);

                // Movimiento de archivos en caso de éxito
                MoveFile(file, _archiveFolder, logBuilder);
                if (convertedFilePath != null)
                    MoveFile(convertedFilePath, _processedFolder, logBuilder);
                if (sanitizedFilePath != null)
                    MoveFile(sanitizedFilePath, _processedFolder, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}");

                // Movimiento de todos los archivos relacionados a la carpeta de error
                MoveFile(file, _errorFilesFolder, logBuilder);
                if (convertedFilePath != null && System.IO.File.Exists(convertedFilePath))
                    MoveFile(convertedFilePath, _errorFilesFolder, logBuilder);
                if (sanitizedFilePath != null && System.IO.File.Exists(sanitizedFilePath))
                    MoveFile(sanitizedFilePath, _errorFilesFolder, logBuilder);

                await WriteLogAsync(logBuilder.ToString(), logPath);
                throw;
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed.");
        _logger.LogInformation("Process completed.");
        await WriteLogAsync(logBuilder.ToString(), logPath);

        // Movimiento del log finalizado a la carpeta histórica de logs
        MoveLogToHistoric(logPath, _historicLogsFolder);

        return Ok("Files processed successfully.");
    }

    [HttpPost]
    public async Task<IActionResult> D2_ProcessHistoricSaldosContables()
    {
        var logPath = Path.Combine(_logsFolder, _logFileName);
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process started.");
        _logger.LogInformation("Historic process started.");

        var files = Directory.GetFiles(_filePath, "SDOSCONT_*.csv");
        if (files.Length == 0)
        {
            string errorLog = "No historic files matching 'SDOSCONT_*.csv' found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await WriteLogAsync(logBuilder.ToString(), logPath);
            return NotFound(errorLog);
        }

        foreach (var file in files)
        {
            string? convertedFilePath = null;
            string? sanitizedFilePath = null;
            logBuilder.AppendLine($"Processing file: {file}");

            try
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                var match = Regex.Match(fileName, @"SDOSCONT_(\d{2})(\d{2})(\d{4})(Morning|Afternoon|Night)");

                if (!match.Success)
                {
                    string errorLog = $"Invalid file name format: {fileName}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(errorLog);
                    MoveFile(file, _errorFilesFolder, logBuilder);
                    continue;
                }

                var day = int.Parse(match.Groups[1].Value);
                var month = int.Parse(match.Groups[2].Value);
                var year = int.Parse(match.Groups[3].Value);
                var period = match.Groups[4].Value;
                var parsedDate = new DateTime(year, month, day);
                var timeOffset = period switch
                {
                    "Morning" => new TimeSpan(8, 0, 0),
                    "Afternoon" => new TimeSpan(14, 0, 0),
                    "Night" => new TimeSpan(20, 0, 0),
                    _ => throw new InvalidOperationException("Unknown period.")
                };

                var fechaGenerado = parsedDate.Add(timeOffset);
                logBuilder.AppendLine($"Parsed FechaGenerado: {fechaGenerado} for file: {file}");

                convertedFilePath = ConvertToUTF8WithBOM(file);
                logBuilder.AppendLine($"Converted file to UTF-8 with BOM: {convertedFilePath}");

                sanitizedFilePath = PreprocessCsvFile(convertedFilePath, logBuilder);
                logBuilder.AppendLine($"Sanitized file: {sanitizedFilePath}");

                await BulkInsertToStageAsync(sanitizedFilePath, logBuilder);
                await InsertHistoricDataAsync(fechaGenerado, logBuilder);

                // Movimiento de archivos en caso de éxito
                MoveFile(file, _archiveFolder, logBuilder);
                if (convertedFilePath != null)
                    MoveFile(convertedFilePath, _processedFolder, logBuilder);
                if (sanitizedFilePath != null)
                    MoveFile(sanitizedFilePath, _processedFolder, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}");

                MoveFile(file, _errorFilesFolder, logBuilder);
                if (convertedFilePath != null && System.IO.File.Exists(convertedFilePath))
                    MoveFile(convertedFilePath, _errorFilesFolder, logBuilder);
                if (sanitizedFilePath != null && System.IO.File.Exists(sanitizedFilePath))
                    MoveFile(sanitizedFilePath, _errorFilesFolder, logBuilder);

                await WriteLogAsync(logBuilder.ToString(), logPath);
                throw;
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process completed.");
        _logger.LogInformation("Historic process completed.");
        await WriteLogAsync(logBuilder.ToString(), logPath);

        MoveLogToHistoric(logPath, _historicLogsFolder);
        return Ok("Historic files processed successfully.");
    }

    private async Task BulkInsertToStageAsync(string csvFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D2_Stage_Saldos_Contables;", connection, transaction)
                    {
                        CommandTimeout = MySqlCommandTimeout
                    };
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D2_Stage_Saldos_Contables.");

                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{csvFilePath.Replace("\\", "\\\\")}' " +
                                          "INTO TABLE D2_Stage_Saldos_Contables " +
                                          "FIELDS TERMINATED BY ',' " +
                                          "ENCLOSED BY '\"' " +
                                          "LINES TERMINATED BY '\\n' " +
                                          "IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction)
                    {
                        CommandTimeout = MySqlCommandTimeout
                    };
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Bulk inserted data into D2_Stage_Saldos_Contables.");

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

    private async Task InsertToFinalTableAsync(StringBuilder logBuilder)
    {
        var sqlInsertCommand = @"
            INSERT INTO D2_Saldos_Contables (
                Id_Credito, Referencia, Nombre, Id_Sucursal, Sucursal, Id_Convenio, Convenio, Financiamiento,
                Estatus_Inicial, Estatus_Final, Fecha_Apertura, Fecha_Terminacion, Importe, Dias_Atraso, 
                Cuotas_Atrasadas, Periodos_Atraso, Pagos_Sostenidos, Pago, Frecuencia, Fecha_Ultimo_Pago, 
                Importe_Ultimo_Pago, Saldo_Inicial_Capital, Otorgado, Pagos, Ajuste_Cargo_Capital, 
                Ajuste_Abono_Capital, Saldo_Final_Capital, Calculo, Diferencia, Capital_Vigente, 
                Capital_Vencido, Saldo_Inicial_Interes, Devengamiento, Pagos_Interes, Ajuste_Cargo_Interes, 
                Ajuste_Abono_Interes, Interes_No_Devengado, Saldo_Final_Interes, Calculo_Interes, 
                Diferencia_Interes, Interes_Devengado, IVA_Interes_Devengado, Interes_No_DevengadoB, 
                Fecha_Cartera_Vencida, Saldo_Contable, Saldo_Insoluto, Porc_Provision, Reserva, 
                nCAT, vOpTable, Status, FechaGenerado
            )
            SELECT 
                Id_Credito, Referencia, Nombre, Id_Sucursal, Sucursal, Id_Convenio, Convenio, Financiamiento,
                Estatus_Inicial, Estatus_Final, 
                STR_TO_DATE(NULLIF(Fecha_Apertura, ''), '%d/%m/%Y') AS Fecha_Apertura,
                STR_TO_DATE(NULLIF(Fecha_Terminacion, ''), '%d/%m/%Y') AS Fecha_Terminacion,
                Importe, Dias_Atraso, Cuotas_Atrasadas, Periodos_Atraso, Pagos_Sostenidos, Pago, Frecuencia,
                STR_TO_DATE(NULLIF(Fecha_Ultimo_Pago, ''), '%d/%m/%Y') AS Fecha_Ultimo_Pago,
                Importe_Ultimo_Pago, Saldo_Inicial_Capital, Otorgado, Pagos, Ajuste_Cargo_Capital, 
                Ajuste_Abono_Capital, Saldo_Final_Capital, Calculo, Diferencia, Capital_Vigente, 
                Capital_Vencido, Saldo_Inicial_Interes, Devengamiento, Pagos_Interes, Ajuste_Cargo_Interes, 
                Ajuste_Abono_Interes, Interes_No_Devengado, Saldo_Final_Interes, Calculo_Interes, 
                Diferencia_Interes, Interes_Devengado, IVA_Interes_Devengado, Interes_No_DevengadoB, 
                STR_TO_DATE(NULLIF(Fecha_Cartera_Vencida, ''), '%d/%m/%Y') AS Fecha_Cartera_Vencida,
                Saldo_Contable, Saldo_Insoluto, Porc_Provision, Reserva, nCAT, vOpTable, Status, NOW() AS FechaGenerado
            FROM D2_Stage_Saldos_Contables;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction)
                    {
                        CommandTimeout = MySqlCommandTimeout
                    };
                    int rowsAffected = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Inserted {rowsAffected} rows into D2_Saldos_Contables.");
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

    private async Task InsertHistoricDataAsync(DateTime fechaGenerado, StringBuilder logBuilder)
    {
        var sqlInsertCommand = @"
            INSERT INTO D2_Saldos_Contables (
                Id_Credito, Referencia, Nombre, Id_Sucursal, Sucursal, Id_Convenio, Convenio, Financiamiento, 
                Estatus_Inicial, Estatus_Final, Fecha_Apertura, Fecha_Terminacion, Importe, Dias_Atraso, 
                Cuotas_Atrasadas, Periodos_Atraso, Pagos_Sostenidos, Pago, Frecuencia, Fecha_Ultimo_Pago, 
                Importe_Ultimo_Pago, Saldo_Inicial_Capital, Otorgado, Pagos, Ajuste_Cargo_Capital, 
                Ajuste_Abono_Capital, Saldo_Final_Capital, Calculo, Diferencia, Capital_Vigente, 
                Capital_Vencido, Saldo_Inicial_Interes, Devengamiento, Pagos_Interes, Ajuste_Cargo_Interes, 
                Ajuste_Abono_Interes, Interes_No_Devengado, Saldo_Final_Interes, Calculo_Interes, 
                Diferencia_Interes, Interes_Devengado, IVA_Interes_Devengado, Interes_No_DevengadoB, 
                Fecha_Cartera_Vencida, Saldo_Contable, Saldo_Insoluto, Porc_Provision, Reserva, 
                nCAT, vOpTable, Status, FechaGenerado
            )
            SELECT 
                Id_Credito, Referencia, Nombre, Id_Sucursal, Sucursal, Id_Convenio, Convenio, Financiamiento, 
                Estatus_Inicial, Estatus_Final, 
                STR_TO_DATE(NULLIF(Fecha_Apertura, ''), '%d/%m/%Y') AS Fecha_Apertura,
                STR_TO_DATE(NULLIF(Fecha_Terminacion, ''), '%d/%m/%Y') AS Fecha_Terminacion,
                Importe, Dias_Atraso, Cuotas_Atrasadas, Periodos_Atraso, Pagos_Sostenidos, Pago, Frecuencia, 
                STR_TO_DATE(NULLIF(Fecha_Ultimo_Pago, ''), '%d/%m/%Y') AS Fecha_Ultimo_Pago,
                Importe_Ultimo_Pago, Saldo_Inicial_Capital, Otorgado, Pagos, Ajuste_Cargo_Capital, 
                Ajuste_Abono_Capital, Saldo_Final_Capital, Calculo, Diferencia, Capital_Vigente, 
                Capital_Vencido, Saldo_Inicial_Interes, Devengamiento, Pagos_Interes, Ajuste_Cargo_Interes, 
                Ajuste_Abono_Interes, Interes_No_Devengado, Saldo_Final_Interes, Calculo_Interes, 
                Diferencia_Interes, Interes_Devengado, IVA_Interes_Devengado, Interes_No_DevengadoB, 
                STR_TO_DATE(NULLIF(Fecha_Cartera_Vencida, ''), '%d/%m/%Y') AS Fecha_Cartera_Vencida,
                Saldo_Contable, Saldo_Insoluto, Porc_Provision, Reserva, nCAT, vOpTable, Status, @FechaGenerado
            FROM D2_Stage_Saldos_Contables;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction)
                    {
                        CommandTimeout = MySqlCommandTimeout
                    };
                    command.Parameters.AddWithValue("@FechaGenerado", fechaGenerado);
                    int rowsAffected = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Inserted {rowsAffected} rows into D2_Saldos_Contables (historic).");
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

    private string ConvertToUTF8WithBOM(string filePath)
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

    private string PreprocessCsvFile(string inputFilePath, StringBuilder logBuilder)
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
                    // Mantener la cabecera sin modificación
                    if (!headerProcessed)
                    {
                        writer.WriteLine(line);
                        headerProcessed = true;
                        continue;
                    }
                    var columns = line.Split(',');
                    // En caso de que la primera columna esté vacía o sea "0", se omite la línea
                    if (columns.Length == 0 || string.IsNullOrWhiteSpace(columns[0]) || columns[0].Trim() == "0")
                    {
                        logBuilder.AppendLine($"Skipped invalid or empty row: {line}");
                        continue;
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

    private void MoveFile(string sourceFilePath, string destinationFolder, StringBuilder logBuilder)
    {
        try
        {
            if (!Directory.Exists(destinationFolder))
                Directory.CreateDirectory(destinationFolder);

            var destinationFilePath = Path.Combine(destinationFolder, Path.GetFileName(sourceFilePath));
            if (System.IO.File.Exists(destinationFilePath))
                System.IO.File.Delete(destinationFilePath);

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

    private async Task WriteLogAsync(string content, string logPath)
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

    private void MoveLogToHistoric(string logPath, string historicLogsFolder)
    {
        try
        {
            if (!Directory.Exists(historicLogsFolder))
                Directory.CreateDirectory(historicLogsFolder);

            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var logFileName = Path.GetFileNameWithoutExtension(logPath) + $"_{timestamp}" + Path.GetExtension(logPath);
            var destinationFilePath = Path.Combine(historicLogsFolder, logFileName);

            System.IO.File.Move(logPath, destinationFilePath);
            _logger.LogInformation($"Moved log file to historic folder: {destinationFilePath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error moving log file to historic folder: {ex.Message}");
        }
    }

    private void MoveExistingLog(string logPath, string historicLogsFolder)
    {
        try
        {
            if (!Directory.Exists(historicLogsFolder))
                Directory.CreateDirectory(historicLogsFolder);

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
