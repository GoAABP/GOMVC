using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace GOMVC.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class D5_Gestiones_Controller : ControllerBase
    {
        private readonly ILogger<D5_Gestiones_Controller> _logger;
        private readonly IConfiguration _configuration;
        private readonly string _connectionString;

        // Rutas base configuradas (se recomienda externalizarlas en appsettings.json)
        private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
        private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";
        private readonly string _logsFolder = @"C:\Users\Go Credit\Documents\DATA\LOGS";
        private readonly string _historicLogsFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC LOGS";
        // Separamos las carpetas para archivos originales y procesados
        private readonly string _archiveFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Archive";
        private readonly string _processedFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Processed";
        private readonly string _errorFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Error";

        // Nombre dinámico del log basado en el controlador
        private readonly string _logFileName;

        public D5_Gestiones_Controller(ILogger<D5_Gestiones_Controller> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

            _logFileName = $"{nameof(D5_Gestiones_Controller)}.log";
            var logPath = Path.Combine(_logsFolder, _logFileName);
            if (System.IO.File.Exists(logPath))
            {
                MoveExistingLog(logPath, _historicLogsFolder);
            }
        }

        /// <summary>
        /// Proceso completo para D5 Gestiones, mejorado con:
        /// - Middleware de Correlation ID
        /// - Medición de rendimiento
        /// - Manejo de excepciones y logging enriquecido
        /// - Movimiento diferenciado de archivos originales y procesados
        /// </summary>
        [HttpPost("D5_ProcessGestiones")]
        public async Task<IActionResult> D5_ProcessGestiones()
        {
            // Verificar si existe el contexto HTTP y extraer el Correlation ID o generar uno nuevo
            string correlationId = (Request?.Headers != null && Request.Headers.ContainsKey("X-Correlation-ID"))
                ? Request.Headers["X-Correlation-ID"].ToString()
                : Guid.NewGuid().ToString();

            var stopwatch = Stopwatch.StartNew();

            var logPath = Path.Combine(_logsFolder, _logFileName);
            var logBuilder = new StringBuilder();
            bool hasErrors = false;

            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} [{correlationId}] - D5 process started.");
            _logger.LogInformation("[{CorrelationId}] D5 process started.", correlationId);

            try
            {
                // 1. Identificar archivos Excel según el patrón.
                var files = Directory.GetFiles(_filePath, "Re_GestionesRO_*.xlsx");
                if (files.Length == 0)
                {
                    string errorLog = $"[{correlationId}] No se encontraron archivos que cumplan el patrón 'Re_GestionesRO_*.xlsx'.";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(errorLog);
                    await WriteLogAsync(logBuilder.ToString(), logPath);
                    return NotFound(errorLog);
                }

                // 2. Truncar la tabla de staging.
                await TruncateStagingTableAsync(logBuilder, correlationId);

                // Procesar cada archivo.
                foreach (var file in files)
                {
                    string? csvFilePath = null;
                    logBuilder.AppendLine($"[{correlationId}] Processing file: {file}");
                    try
                    {
                        if (!System.IO.File.Exists(file))
                        {
                            throw new FileNotFoundException($"Archivo no encontrado: {file}");
                        }

                        // 3. Procesar el XLSX en chunks.
                        csvFilePath = await ProcessXlsxToCsvAsync(file, logBuilder, correlationId);

                        // 4. Cargar el CSV a la tabla de staging.
                        await BulkInsertToStageAsync(csvFilePath, logBuilder, correlationId);

                        // 5. Normalizar datos en la tabla de staging.
                        await NormalizeStageDataAsync(logBuilder, correlationId);

                        // 6. Insertar solo registros nuevos a la tabla final.
                        await InsertToFinalTableAsync(logBuilder, correlationId);

                        // Mover archivo original y procesado.
                        MoveFile(file, _archiveFolder, logBuilder, correlationId);
                        if (csvFilePath != null)
                            MoveFile(csvFilePath, _processedFolder, logBuilder, correlationId);
                    }
                    catch (Exception ex)
                    {
                        logBuilder.AppendLine($"[{correlationId}] Error processing file {file}: {ex.Message}");
                        _logger.LogError(ex, "[{CorrelationId}] Error processing file {File}", correlationId, file);
                        hasErrors = true;
                        MoveFile(file, _errorFolder, logBuilder, correlationId);
                        if (csvFilePath != null && System.IO.File.Exists(csvFilePath))
                            MoveFile(csvFilePath, _errorFolder, logBuilder, correlationId);
                        await WriteLogAsync(logBuilder.ToString(), logPath);
                        throw new ApplicationException($"[{correlationId}] Error processing file {file}", ex);
                    }
                }

                logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} [{correlationId}] - D5 process completed.");
                _logger.LogInformation("[{CorrelationId}] D5 process completed.", correlationId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[{CorrelationId}] Process failed.", correlationId);
                await WriteLogAsync(logBuilder.ToString(), logPath);
                return StatusCode(500, $"[{correlationId}] D5 process failed. Check the log for details.");
            }
            finally
            {
                stopwatch.Stop();
                logBuilder.AppendLine($"[{correlationId}] Total process time: {stopwatch.ElapsedMilliseconds} ms");
                _logger.LogInformation("[{CorrelationId}] Total process time: {Elapsed} ms", correlationId, stopwatch.ElapsedMilliseconds);
                await WriteLogAsync(logBuilder.ToString(), logPath);
                MoveLogToHistoric(logPath, _historicLogsFolder);
            }

            return hasErrors
                ? StatusCode(500, $"[{correlationId}] D5 process completed with errors. Check the log for details.")
                : Ok($"[{correlationId}] D5 process completed successfully.");
        }

        #region Métodos de Procesamiento de XLSX a CSV

        /// <summary>
        /// Procesa el archivo XLSX en chunks y genera un CSV delimitado por '|' con encoding UTF-8 con BOM.
        /// Incluye validaciones y logging con Correlation ID.
        /// </summary>
        private async Task<string> ProcessXlsxToCsvAsync(string xlsxFilePath, StringBuilder logBuilder, string correlationId)
        {
            // Se genera el CSV temporal en la carpeta temporal del sistema.
            string tempCsvPath = Path.Combine(Path.GetTempPath(), $"{Path.GetFileNameWithoutExtension(xlsxFilePath)}_processed.csv");

            using (var package = new ExcelPackage(new FileInfo(xlsxFilePath)))
            {
                var worksheet = package.Workbook.Worksheets[0];
                int totalRows = worksheet.Dimension.Rows;
                int totalCols = worksheet.Dimension.Columns;
                int chunkSize = 10000; // Ajustable según rendimiento

                using (var writer = new StreamWriter(tempCsvPath, false, new UTF8Encoding(true)))
                {
                    // Escribir encabezados
                    var headers = new List<string>();
                    for (int col = 1; col <= totalCols; col++)
                    {
                        headers.Add(CleanCell(worksheet.Cells[1, col].Text));
                    }
                    await writer.WriteLineAsync(string.Join("|", headers));

                    // Procesar filas en chunks
                    for (int startRow = 2; startRow <= totalRows; startRow += chunkSize)
                    {
                        int endRow = Math.Min(startRow + chunkSize - 1, totalRows);
                        logBuilder.AppendLine($"[{correlationId}] Processing rows {startRow} to {endRow}.");

                        for (int row = startRow; row <= endRow; row++)
                        {
                            var cells = new List<string>();
                            for (int col = 1; col <= totalCols; col++)
                            {
                                var cell = worksheet.Cells[row, col];
                                string cellValue;
                                if (cell.Value is DateTime dt)
                                {
                                    cellValue = dt.ToString("yyyy-MM-dd");
                                }
                                else
                                {
                                    cellValue = CleanCell(cell.Text);
                                    if (IsDateCell(worksheet.Cells[1, col].Text))
                                    {
                                        cellValue = ConvertToMySqlDate(cellValue);
                                    }
                                }
                                cells.Add(cellValue);
                            }
                            await writer.WriteLineAsync(string.Join("|", cells));
                        }
                    }
                }
            }
            logBuilder.AppendLine($"[{correlationId}] CSV file generated: {tempCsvPath}");
            return tempCsvPath;
        }

        /// <summary>
        /// Determina si el encabezado indica que la columna es de fecha.
        /// </summary>
        private bool IsDateCell(string header)
        {
            return header.Trim().ToLower().Contains("fecha");
        }

        /// <summary>
        /// Convierte una cadena de fecha a un formato MySQL (yyyy-MM-dd).
        /// </summary>
        private string ConvertToMySqlDate(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
                return "";
            string[] formats = new string[]
            {
                "dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy HH:mm", "dd/MM/yyyy", "d/M/yyyy",
                "MM/dd/yyyy HH:mm:ss", "MM/dd/yyyy", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd",
                "dd-MM-yyyy", "dd.MM.yyyy"
            };
            if (DateTime.TryParseExact(input, formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dt) ||
                DateTime.TryParse(input, out dt))
            {
                return dt.ToString("yyyy-MM-dd");
            }
            else
            {
                _logger.LogWarning("No se pudo convertir la fecha: '{0}'", input);
                return "";
            }
        }

        /// <summary>
        /// Limpia el contenido de una celda, eliminando saltos de línea y el carácter '|' y recortando espacios.
        /// </summary>
        private string CleanCell(string input)
        {
            if (string.IsNullOrEmpty(input))
                return "";
            string result = input.Replace("\r", " ").Replace("\n", " ");
            return result.Replace("|", "").Trim();
        }

        #endregion

        #region Métodos de Carga a Base de Datos

        /// <summary>
        /// Trunca la tabla de staging D5_Stage_Gestiones.
        /// </summary>
        private async Task TruncateStagingTableAsync(StringBuilder logBuilder, string correlationId)
        {
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                string truncateSql = "TRUNCATE TABLE D5_Stage_Gestiones;";
                using (var cmd = new MySqlCommand(truncateSql, connection))
                {
                    await cmd.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"[{correlationId}] Truncated table D5_Stage_Gestiones.");
                    _logger.LogInformation("[{CorrelationId}] Truncated table D5_Stage_Gestiones.", correlationId);
                }
            }
        }

        /// <summary>
        /// Carga el CSV generado a la tabla de staging mediante LOAD DATA LOCAL INFILE dentro de una transacción.
        /// </summary>
        private async Task BulkInsertToStageAsync(string csvFilePath, StringBuilder logBuilder, string correlationId)
        {
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = await connection.BeginTransactionAsync())
                {
                    try
                    {
                        string loadSql = $"LOAD DATA LOCAL INFILE '{csvFilePath.Replace("\\", "\\\\")}' " +
                                         "INTO TABLE D5_Stage_Gestiones " +
                                         "FIELDS TERMINATED BY '|' " +
                                         "OPTIONALLY ENCLOSED BY '\"' " +
                                         "LINES TERMINATED BY '\\n' " +
                                         "IGNORE 1 LINES;";
                        using (var cmd = new MySqlCommand(loadSql, connection, transaction))
                        {
                            cmd.CommandTimeout = 3600;
                            await cmd.ExecuteNonQueryAsync();
                            logBuilder.AppendLine($"[{correlationId}] Data loaded into D5_Stage_Gestiones.");
                        }
                        await transaction.CommitAsync();
                    }
                    catch (Exception ex)
                    {
                        await transaction.RollbackAsync();
                        logBuilder.AppendLine($"[{correlationId}] Bulk insert error: {ex.Message}");
                        throw new ApplicationException($"[{correlationId}] Error during bulk insert", ex);
                    }
                }
            }
        }

        /// <summary>
        /// Normaliza los datos en la tabla de staging.
        /// </summary>
        private async Task NormalizeStageDataAsync(StringBuilder logBuilder, string correlationId)
        {
            string sqlUpdate = @"
                UPDATE D5_Stage_Gestiones
                SET
                    Agencia_Registro = NULLIF(TRIM(REPLACE(REPLACE(Agencia_Registro, '\r', ' '), '\n', ' ')), ''),
                    Causa_No_Pago = NULLIF(TRIM(REPLACE(REPLACE(Causa_No_Pago, '\r', ' '), '\n', ' ')), ''),
                    Causa_No_Domiciliacion = NULLIF(TRIM(REPLACE(REPLACE(Causa_No_Domiciliacion, '\r', ' '), '\n', ' ')), ''),
                    Codigo_Accion = NULLIF(TRIM(REPLACE(REPLACE(Codigo_Accion, '\r', ' '), '\n', ' ')), ''),
                    Codigo_Resultado = NULLIF(TRIM(REPLACE(REPLACE(Codigo_Resultado, '\r', ' '), '\n', ' ')), ''),
                    Comentarios = NULLIF(TRIM(REPLACE(REPLACE(Comentarios, '\r', ' '), '\n', ' ')), ''),
                    Contacto_Generado = NULLIF(TRIM(REPLACE(REPLACE(Contacto_Generado, '\r', ' '), '\n', ' ')), ''),
                    Coordenadas = NULLIF(TRIM(REPLACE(REPLACE(Coordenadas, '\r', ' '), '\n', ' ')), ''),
                    Credito = NULLIF(TRIM(REPLACE(REPLACE(Credito, '\r', ' '), '\n', ' ')), ''),
                    Estatus_Promesa = NULLIF(TRIM(REPLACE(REPLACE(Estatus_Promesa, '\r', ' '), '\n', ' ')), ''),
                    Fecha_Actividad = CASE 
                        WHEN TRIM(Fecha_Actividad) = '' THEN NULL
                        ELSE DATE_FORMAT(STR_TO_DATE(Fecha_Actividad, '%Y-%m-%d %H:%i:%s'), '%Y-%m-%d')
                     END,
                    Fecha_Promesa = CASE 
                        WHEN TRIM(Fecha_Promesa) = '' THEN NULL
                        ELSE DATE_FORMAT(STR_TO_DATE(Fecha_Promesa, '%Y-%m-%d'), '%Y-%m-%d')
                    END,
                    Monto_Promesa = NULLIF(TRIM(REPLACE(REPLACE(Monto_Promesa, '\r', ' '), '\n', ' ')), ''),
                    Origen = NULLIF(TRIM(REPLACE(REPLACE(Origen, '\r', ' '), '\n', ' ')), ''),
                    Producto = NULLIF(TRIM(REPLACE(REPLACE(Producto, '\r', ' '), '\n', ' ')), ''),
                    Resultado = NULLIF(TRIM(REPLACE(REPLACE(Resultado, '\r', ' '), '\n', ' ')), ''),
                    Telefono = NULLIF(TRIM(REPLACE(REPLACE(Telefono, '\r', ' '), '\n', ' ')), ''),
                    Tipo_Pago = NULLIF(TRIM(REPLACE(REPLACE(Tipo_Pago, '\r', ' '), '\n', ' ')), ''),
                    Usuario_Registro = NULLIF(TRIM(REPLACE(REPLACE(Usuario_Registro, '\r', ' '), '\n', ' ')), '')
                ;";
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var cmd = new MySqlCommand(sqlUpdate, connection))
                {
                    cmd.CommandTimeout = 3600;
                    int rowsAffected = await cmd.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"[{correlationId}] Normalized {rowsAffected} rows in D5_Stage_Gestiones.");
                }
            }
        }

        /// <summary>
        /// Inserta solo los registros nuevos de D5_Stage_Gestiones en la tabla final D5_Gestiones, controlando duplicados.
        /// </summary>
        private async Task InsertToFinalTableAsync(StringBuilder logBuilder, string correlationId)
        {
            string sqlInsert = @"
                INSERT INTO D5_Gestiones (
                    Agencia_Registro, 
                    Causa_No_Pago, 
                    Causa_No_Domiciliacion, 
                    Codigo_Accion, 
                    Codigo_Resultado,
                    Comentarios, 
                    Contacto_Generado, 
                    Coordenadas, 
                    Id_Credito, 
                    Estatus_Promesa,
                    Fecha_Actividad, 
                    Fecha_Promesa, 
                    Monto_Promesa, 
                    Origen, 
                    Producto, 
                    Resultado, 
                    Telefono, 
                    Tipo_Pago, 
                    Usuario_Registro
                )
                SELECT 
                    s.Agencia_Registro, 
                    s.Causa_No_Pago, 
                    s.Causa_No_Domiciliacion, 
                    s.Codigo_Accion, 
                    s.Codigo_Resultado,
                    s.Comentarios, 
                    s.Contacto_Generado, 
                    s.Coordenadas, 
                    s.Credito, 
                    s.Estatus_Promesa,
                    s.Fecha_Actividad,
                    s.Fecha_Promesa,
                    s.Monto_Promesa, 
                    s.Origen, 
                    s.Producto, 
                    s.Resultado, 
                    s.Telefono, 
                    s.Tipo_Pago, 
                    s.Usuario_Registro
                FROM D5_Stage_Gestiones s
                LEFT JOIN D5_Gestiones f
                  ON CONCAT_WS('|',
                        TRIM(COALESCE(s.Agencia_Registro, '')),
                        TRIM(COALESCE(s.Causa_No_Pago, '')),
                        TRIM(COALESCE(s.Causa_No_Domiciliacion, '')),
                        TRIM(COALESCE(s.Codigo_Accion, '')),
                        TRIM(COALESCE(s.Codigo_Resultado, '')),
                        TRIM(COALESCE(s.Comentarios, '')),
                        TRIM(COALESCE(s.Contacto_Generado, '')),
                        TRIM(COALESCE(s.Coordenadas, '')),
                        TRIM(COALESCE(s.Credito, '')),
                        TRIM(COALESCE(s.Estatus_Promesa, '')),
                        TRIM(COALESCE(s.Fecha_Actividad, '')),
                        TRIM(COALESCE(s.Fecha_Promesa, '')),
                        TRIM(COALESCE(s.Monto_Promesa, '')),
                        TRIM(COALESCE(s.Origen, '')),
                        TRIM(COALESCE(s.Producto, '')),
                        TRIM(COALESCE(s.Resultado, '')),
                        TRIM(COALESCE(s.Telefono, '')),
                        TRIM(COALESCE(s.Tipo_Pago, '')),
                        TRIM(COALESCE(s.Usuario_Registro, ''))
                     )
                     =
                     CONCAT_WS('|',
                        TRIM(COALESCE(f.Agencia_Registro, '')),
                        TRIM(COALESCE(f.Causa_No_Pago, '')),
                        TRIM(COALESCE(f.Causa_No_Domiciliacion, '')),
                        TRIM(COALESCE(f.Codigo_Accion, '')),
                        TRIM(COALESCE(f.Codigo_Resultado, '')),
                        TRIM(COALESCE(f.Comentarios, '')),
                        TRIM(COALESCE(f.Contacto_Generado, '')),
                        TRIM(COALESCE(f.Coordenadas, '')),
                        TRIM(COALESCE(f.Id_Credito, '')),
                        TRIM(COALESCE(f.Estatus_Promesa, '')),
                        TRIM(COALESCE(DATE_FORMAT(f.Fecha_Actividad, '%Y-%m-%d'), '')),
                        TRIM(COALESCE(DATE_FORMAT(f.Fecha_Promesa, '%Y-%m-%d'), '')),
                        TRIM(COALESCE(f.Monto_Promesa, '')),
                        TRIM(COALESCE(f.Origen, '')),
                        TRIM(COALESCE(f.Producto, '')),
                        TRIM(COALESCE(f.Resultado, '')),
                        TRIM(COALESCE(f.Telefono, '')),
                        TRIM(COALESCE(f.Tipo_Pago, '')),
                        TRIM(COALESCE(f.Usuario_Registro, ''))
                     )
                WHERE f.Id_Credito IS NULL;";
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = await connection.BeginTransactionAsync())
                {
                    try
                    {
                        using (var cmd = new MySqlCommand(sqlInsert, connection, transaction))
                        {
                            cmd.CommandTimeout = 3600;
                            int rowsAffected = await cmd.ExecuteNonQueryAsync();
                            logBuilder.AppendLine($"[{correlationId}] Inserted {rowsAffected} new rows into D5_Gestiones.");
                        }
                        await transaction.CommitAsync();
                    }
                    catch (Exception ex)
                    {
                        await transaction.RollbackAsync();
                        logBuilder.AppendLine($"[{correlationId}] Final insert error: {ex.Message}");
                        throw new ApplicationException($"[{correlationId}] Error inserting into final table", ex);
                    }
                }
            }
        }

        #endregion

        #region Métodos Genéricos de Log y Movimiento de Archivos

        /// <summary>
        /// Escribe el log en el archivo especificado.
        /// </summary>
        private async Task WriteLogAsync(string content, string logPath)
        {
            try
            {
                var directory = Path.GetDirectoryName(logPath);
                if (!Directory.Exists(directory))
                    Directory.CreateDirectory(directory!);
                await System.IO.File.WriteAllTextAsync(logPath, content);
                _logger.LogInformation("Log written to: {LogPath}", logPath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error writing log to {LogPath}", logPath);
                throw;
            }
        }

        /// <summary>
        /// Mueve el archivo de origen a la carpeta de destino.
        /// </summary>
        private void MoveFile(string sourceFilePath, string destinationFolder, StringBuilder logBuilder, string correlationId)
        {
            try
            {
                if (!Directory.Exists(destinationFolder))
                    Directory.CreateDirectory(destinationFolder);
                var destinationFilePath = Path.Combine(destinationFolder, Path.GetFileName(sourceFilePath));
                if (System.IO.File.Exists(destinationFilePath))
                    System.IO.File.Delete(destinationFilePath);
                System.IO.File.Move(sourceFilePath, destinationFilePath);
                logBuilder.AppendLine($"[{correlationId}] Moved file: {sourceFilePath} -> {destinationFilePath}");
                _logger.LogInformation("[{CorrelationId}] Moved file: {Source} -> {Destination}", correlationId, sourceFilePath, destinationFilePath);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"[{correlationId}] Error moving file {sourceFilePath}: {ex.Message}");
                _logger.LogError(ex, "[{CorrelationId}] Error moving file {File}", correlationId, sourceFilePath);
                throw;
            }
        }

        /// <summary>
        /// Mueve el log actual a la carpeta histórica.
        /// </summary>
        private void MoveLogToHistoric(string logPath, string historicLogsFolder)
        {
            try
            {
                if (!System.IO.File.Exists(logPath))
                {
                    _logger.LogWarning("Log file does not exist: {LogPath}", logPath);
                    return;
                }
                if (!Directory.Exists(historicLogsFolder))
                    Directory.CreateDirectory(historicLogsFolder);
                var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                var newLogName = Path.GetFileNameWithoutExtension(logPath) + $"_{timestamp}" + Path.GetExtension(logPath);
                var destPath = Path.Combine(historicLogsFolder, newLogName);
                System.IO.File.Move(logPath, destPath);
                _logger.LogInformation("Log moved to historic folder: {DestPath}", destPath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error moving log file to historic folder.");
                throw;
            }
        }

        /// <summary>
        /// Mueve un log existente a la carpeta histórica.
        /// </summary>
        private void MoveExistingLog(string logPath, string historicLogsFolder)
        {
            try
            {
                if (!Directory.Exists(historicLogsFolder))
                    Directory.CreateDirectory(historicLogsFolder);
                var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                var destFilePath = Path.Combine(historicLogsFolder,
                    Path.GetFileNameWithoutExtension(logPath) + $"_{timestamp}" + Path.GetExtension(logPath));
                System.IO.File.Move(logPath, destFilePath);
                _logger.LogInformation("Existing log moved: {DestFilePath}", destFilePath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error moving existing log: {ErrorMessage}", ex.Message);
            }
        }

        #endregion
    }
}
