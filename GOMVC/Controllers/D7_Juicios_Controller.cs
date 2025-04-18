using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using OfficeOpenXml;

public class D7_Juicios_Controller : Controller
{
    private readonly ILogger<D7_Juicios_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    
    // Rutas para archivos históricos y logs
    private readonly string _historicFilesPath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";
    private readonly string _historicLogsPath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC LOGS";
    private readonly string _logsPath = @"C:\Users\Go Credit\Documents\DATA\LOGS";

    public D7_Juicios_Controller(ILogger<D7_Juicios_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
    }

    [HttpPost]
    public async Task<IActionResult> D7_ProcessJuicios()
    {
        // Generación dinámica del nombre del log usando el nombre del controlador
        string controllerName = this.GetType().Name;
        string logFileName = controllerName + ".log";
        string logPath = Path.Combine(_logsPath, logFileName);

        // Si ya existe un log con el mismo nombre, moverlo a HISTORIC LOGS
        if (System.IO.File.Exists(logPath))
        {
            if (!Directory.Exists(_historicLogsPath))
                Directory.CreateDirectory(_historicLogsPath);

            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string historicLogPath = Path.Combine(_historicLogsPath, controllerName + "_" + timestamp + ".log");
            System.IO.File.Move(logPath, historicLogPath);
        }

        // Carpetas destino para archivos según el resultado del proceso
        var archiveFolder = Path.Combine(_historicFilesPath, "Archive");
        var processedFolder = Path.Combine(_historicFilesPath, "Processed");
        var errorFolder = Path.Combine(_historicFilesPath, "Error");

        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {controllerName} process started.");
        _logger.LogInformation("Process started.");

        // Se buscan archivos XLSX que inicien con "Re_Juicios"
        var files = Directory.GetFiles(_filePath, "Re_Juicios*.xlsx");
        if (files.Length == 0)
        {
            var errorLog = "No files matching 'Re_Juicios*.xlsx' found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D7_WriteLog(logBuilder.ToString(), logPath);
            return NotFound(errorLog);
        }

        logBuilder.AppendLine($"{files.Length} files matching 'Re_Juicios*.xlsx' found.");
        _logger.LogInformation($"{files.Length} files matching 'Re_Juicios*.xlsx' found.");

        // Se limpia la tabla de staging antes de insertar nuevos registros
        await D7_TruncateStagingTableAsync(logBuilder);

        try
        {
            foreach (var file in files)
            {
                logBuilder.AppendLine($"Processing file: {file}");
                _logger.LogInformation($"Processing file: {file}");

                try
                {
                    // Procesa el archivo XLSX en bloques
                    await D7_ProcessLargeXlsx(file, logBuilder);

                    // Movimiento de archivos en caso de éxito:
                    // Mover el archivo original a la carpeta Archive
                    D7_MoveFile(file, archiveFolder, logBuilder);

                    // En caso de existir archivos procesados (por ejemplo, convertidos o sanitizados),
                    // se mueven a la carpeta Processed.
                    string convertedFile = file.Replace(".xlsx", "_converted.xlsx");
                    if (System.IO.File.Exists(convertedFile))
                        D7_MoveFile(convertedFile, processedFolder, logBuilder);

                    string sanitizedFile = file.Replace(".xlsx", "_sanitized.xlsx");
                    if (System.IO.File.Exists(sanitizedFile))
                        D7_MoveFile(sanitizedFile, processedFolder, logBuilder);

                    // Mover los datos desde la tabla staging a la tabla final
                    await D7_MoveDataToFinalTableAsync(logBuilder);

                    logBuilder.AppendLine($"File {file} processed successfully.");
                    _logger.LogInformation($"File {file} processed successfully.");
                }
                catch (Exception ex)
                {
                    logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                    _logger.LogError(ex, $"Error processing file {file}");

                    // Movimiento de todos los archivos relacionados a la carpeta Error
                    D7_MoveFile(file, errorFolder, logBuilder);

                    string convertedFile = file.Replace(".xlsx", "_converted.xlsx");
                    if (System.IO.File.Exists(convertedFile))
                        D7_MoveFile(convertedFile, errorFolder, logBuilder);

                    string sanitizedFile = file.Replace(".xlsx", "_sanitized.xlsx");
                    if (System.IO.File.Exists(sanitizedFile))
                        D7_MoveFile(sanitizedFile, errorFolder, logBuilder);

                    // Se relanza la excepción para asegurar que el proceso no termine exitosamente
                    throw;
                }
            }
        }
        catch (Exception finalEx)
        {
            logBuilder.AppendLine($"Process terminated with error: {finalEx.Message}");
            _logger.LogError(finalEx, "Process terminated with error.");
            await D7_WriteLog(logBuilder.ToString(), logPath);
            // Se relanza la excepción para que el proceso global falle
            throw;
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.");
        _logger.LogInformation("Process completed successfully.");
        await D7_WriteLog(logBuilder.ToString(), logPath);
        // Opcional: Mover el log a una carpeta histórica (si se desea mantener un historial adicional)
        D7_MoveLogToHistoric(logPath, _historicLogsPath);

        return Ok("Process completed successfully.");
    }

    private async Task D7_TruncateStagingTableAsync(StringBuilder logBuilder)
    {
        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();
        var sqlTruncate = "TRUNCATE TABLE D7_Stage_Juicios;";
        using var command = new MySqlCommand(sqlTruncate, connection);
        await command.ExecuteNonQueryAsync();
        logBuilder.AppendLine("Truncated D7_Stage_Juicios table.");
        _logger.LogInformation("Truncated D7_Stage_Juicios table.");
    }

    private async Task D7_ProcessLargeXlsx(string filePath, StringBuilder logBuilder)
    {
        FileInfo fileInfo = new FileInfo(filePath);
        ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

        using ExcelPackage package = new ExcelPackage(fileInfo);
        ExcelWorksheet worksheet = package.Workbook.Worksheets[0];

        int totalRows = worksheet.Dimension.Rows;
        int chunkSize = 10000; // Procesar en bloques

        for (int startRow = 2; startRow <= totalRows; startRow += chunkSize) // Se salta el encabezado
        {
            int endRow = Math.Min(startRow + chunkSize - 1, totalRows);
            await D7_ProcessChunkAsync(worksheet, startRow, endRow, logBuilder);
        }
    }

    private async Task D7_ProcessChunkAsync(ExcelWorksheet worksheet, int startRow, int endRow, StringBuilder logBuilder)
    {
        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();
        using var transaction = await connection.BeginTransactionAsync();

        try
        {
            for (int row = startRow; row <= endRow; row++)
            {
                // Lectura y conversión de valores de cada columna
                var indice = int.TryParse(worksheet.Cells[row, 1].Text.Trim(), out var idx) ? idx : (int?)null;
                var creditoMc = int.TryParse(worksheet.Cells[row, 2].Text.Trim(), out var credMc) ? credMc : (int?)null;
                var decla = worksheet.Cells[row, 3].Text.Trim();
                var descripcionCierre = worksheet.Cells[row, 4].Text.Trim();
                var diasActivo = int.TryParse(worksheet.Cells[row, 5].Text.Trim(), out var dActivo) ? dActivo : (int?)null;
                var diasCaducar = int.TryParse(worksheet.Cells[row, 6].Text.Trim(), out var dCaducar) ? dCaducar : (int?)null;
                var estatus = worksheet.Cells[row, 7].Text.Trim();
                var etapaProcesal = worksheet.Cells[row, 8].Text.Trim();

                // Validación para Expediente
                var expedienteRaw = worksheet.Cells[row, 9].Text.Trim();
                var expediente = long.TryParse(expedienteRaw, out var expValue) ? expValue : (long?)null;

                var fechaActualizacion = worksheet.Cells[row, 10].Text.Trim();
                var fechaCargaInicial = worksheet.Cells[row, 11].Text.Trim();
                var fechaCierre = worksheet.Cells[row, 12].Text.Trim();
                var fechaUltimaAct = worksheet.Cells[row, 13].Text.Trim();
                var idJuicio = int.TryParse(worksheet.Cells[row, 14].Text.Trim(), out var idJ) ? idJ : (int?)null;
                var juzgado = worksheet.Cells[row, 15].Text.Trim();
                var motivoCierre = worksheet.Cells[row, 16].Text.Trim();
                var productoMc = worksheet.Cells[row, 17].Text.Trim();
                var tipoJuicio = worksheet.Cells[row, 18].Text.Trim();
                var validarCierre = worksheet.Cells[row, 19].Text.Trim();

                // Inserción en la tabla staging
                var sqlInsertCommand = @"
                    INSERT INTO D7_Stage_Juicios (
                        Indice, Credito_MC, Decla, Descripcion_Cierre, Dias_Activo, Dias_Caducar, Estatus,
                        Etapa_Procesal, Expediente, Fecha_Actualizacion, Fecha_Carga_Inicial, Fecha_Cierre,
                        Fecha_Ultima_Act, Id_Juicio, Juzgado, Motivo_Cierre, Producto_MC, Tipo_Juicio, Validar_Cierre
                    )
                    VALUES (
                        @Indice, @Credito_MC, @Decla, @Descripcion_Cierre, @Dias_Activo, @Dias_Caducar, @Estatus,
                        @Etapa_Procesal, @Expediente, @Fecha_Actualizacion, @Fecha_Carga_Inicial, @Fecha_Cierre,
                        @Fecha_Ultima_Act, @Id_Juicio, @Juzgado, @Motivo_Cierre, @Producto_MC, @Tipo_Juicio, @Validar_Cierre
                    );";

                using var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                command.Parameters.AddWithValue("@Indice", indice.HasValue ? (object)indice.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Credito_MC", creditoMc.HasValue ? (object)creditoMc.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Decla", decla);
                command.Parameters.AddWithValue("@Descripcion_Cierre", descripcionCierre);
                command.Parameters.AddWithValue("@Dias_Activo", diasActivo.HasValue ? (object)diasActivo.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Dias_Caducar", diasCaducar.HasValue ? (object)diasCaducar.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Estatus", estatus);
                command.Parameters.AddWithValue("@Etapa_Procesal", etapaProcesal);
                command.Parameters.AddWithValue("@Expediente", expediente.HasValue ? (object)expediente.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Fecha_Actualizacion", fechaActualizacion);
                command.Parameters.AddWithValue("@Fecha_Carga_Inicial", fechaCargaInicial);
                command.Parameters.AddWithValue("@Fecha_Cierre", fechaCierre);
                command.Parameters.AddWithValue("@Fecha_Ultima_Act", fechaUltimaAct);
                command.Parameters.AddWithValue("@Id_Juicio", idJuicio.HasValue ? (object)idJuicio.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Juzgado", juzgado);
                command.Parameters.AddWithValue("@Motivo_Cierre", motivoCierre);
                command.Parameters.AddWithValue("@Producto_MC", productoMc);
                command.Parameters.AddWithValue("@Tipo_Juicio", tipoJuicio);
                command.Parameters.AddWithValue("@Validar_Cierre", validarCierre);

                await command.ExecuteNonQueryAsync();
            }

            await transaction.CommitAsync();
            logBuilder.AppendLine($"Inserted {endRow - startRow + 1} records into D7_Stage_Juicios.");
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            logBuilder.AppendLine($"Error inserting chunk data into D7_Stage_Juicios: {ex.Message}");
            _logger.LogError(ex, "Error inserting chunk data into D7_Stage_Juicios.");
            throw;
        }
    }

    private object D7_ParseDate(string rawDate)
    {
        if (string.IsNullOrWhiteSpace(rawDate)) return DBNull.Value;

        if (DateTime.TryParseExact(rawDate, new[] { "MM/dd/yy", "MM/dd/yyyy", "yyyy-MM-dd" },
                                System.Globalization.CultureInfo.InvariantCulture,
                                System.Globalization.DateTimeStyles.None, out var parsedDate))
        {
            return parsedDate;
        }

        return DBNull.Value;
    }

    private async Task D7_MoveDataToFinalTableAsync(StringBuilder logBuilder)
    {
        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();
        using var transaction = await connection.BeginTransactionAsync();
        
        try
        {
            // Truncar la tabla final antes de insertar nuevos datos
            var truncateCommand = new MySqlCommand("TRUNCATE TABLE D7_Juicios;", connection, transaction);
            await truncateCommand.ExecuteNonQueryAsync();
            logBuilder.AppendLine("Truncated D7_Juicios table.");
            _logger.LogInformation("Truncated D7_Juicios table.");

            var sqlInsertCommand = @"
                INSERT INTO D7_Juicios (
                    Credito_MC, Decla, Descripcion_Cierre, Dias_Activo, Dias_Caducar, Estatus,
                    Etapa_Procesal, Expediente, Fecha_Actualizacion, Fecha_Carga_Inicial, Fecha_Cierre,
                    Fecha_Ultima_Act, Id_Juicio, Juzgado, Motivo_Cierre, Producto_MC, Tipo_Juicio, Validar_Cierre
                )
                SELECT 
                    s.Credito_MC, s.Decla, s.Descripcion_Cierre, s.Dias_Activo, s.Dias_Caducar, s.Estatus,
                    s.Etapa_Procesal,
                    CASE WHEN s.Expediente REGEXP '^[0-9]+$' THEN s.Expediente ELSE NULL END AS Expediente,
                    CASE 
                        WHEN s.Fecha_Actualizacion REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{2}:[0-9]{2}$' 
                        THEN STR_TO_DATE(s.Fecha_Actualizacion, '%m/%d/%y %H:%i')
                        WHEN s.Fecha_Actualizacion REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{2}:[0-9]{2}$' 
                        THEN STR_TO_DATE(s.Fecha_Actualizacion, '%d/%m/%Y %H:%i')
                        ELSE NULL
                    END AS Fecha_Actualizacion,
                    CASE 
                        WHEN s.Fecha_Carga_Inicial REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{2}:[0-9]{2}$' 
                        THEN STR_TO_DATE(s.Fecha_Carga_Inicial, '%m/%d/%y %H:%i')
                        WHEN s.Fecha_Carga_Inicial REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{2}:[0-9]{2}$' 
                        THEN STR_TO_DATE(s.Fecha_Carga_Inicial, '%d/%m/%Y %H:%i')
                        ELSE NULL
                    END AS Fecha_Carga_Inicial,
                    CASE 
                        WHEN s.Fecha_Cierre REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' 
                        THEN STR_TO_DATE(s.Fecha_Cierre, '%d/%m/%Y')
                        ELSE NULL
                    END AS Fecha_Cierre,
                    CASE 
                        WHEN s.Fecha_Ultima_Act REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' 
                        THEN STR_TO_DATE(s.Fecha_Ultima_Act, '%d/%m/%Y')
                        ELSE NULL
                    END AS Fecha_Ultima_Act,
                    s.Id_Juicio, s.Juzgado, s.Motivo_Cierre, s.Producto_MC, s.Tipo_Juicio, s.Validar_Cierre
                FROM D7_Stage_Juicios s;";

            using var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
            await command.ExecuteNonQueryAsync();

            logBuilder.AppendLine("Inserted data into D7_Juicios.");
            _logger.LogInformation("Inserted data into D7_Juicios.");
            await transaction.CommitAsync();
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            logBuilder.AppendLine($"Error moving data to D7_Juicios: {ex.Message}");
            _logger.LogError(ex, "Error moving data to D7_Juicios.");
            throw;
        }
    }

    private async Task D7_WriteLog(string content, string logPath)
    {
        await System.IO.File.WriteAllTextAsync(logPath, content);
        _logger.LogInformation($"Log written to {logPath}");
    }

    private void D7_MoveFile(string sourceFilePath, string destinationFolder, StringBuilder logBuilder)
    {
        try
        {
            if (!Directory.Exists(destinationFolder))
            {
                Directory.CreateDirectory(destinationFolder);
            }

            string fileName = Path.GetFileName(sourceFilePath);
            string destinationFilePath = Path.Combine(destinationFolder, fileName);

            // Si el archivo ya existe en el destino, se agrega un timestamp para evitar conflictos
            if (System.IO.File.Exists(destinationFilePath))
            {
                string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                string fileExtension = Path.GetExtension(fileName);
                string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
                destinationFilePath = Path.Combine(destinationFolder, $"{fileNameWithoutExtension}_{timestamp}{fileExtension}");
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

    private void D7_MoveLogToHistoric(string logPath, string historicLogsFolder)
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
        }
    }
}
