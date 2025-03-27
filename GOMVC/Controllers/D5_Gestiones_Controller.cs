using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
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

        // Rutas base para archivos y logs
        private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
        private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";
        private readonly string _logsFolder = @"C:\Users\Go Credit\Documents\DATA\LOGS";
        private readonly string _historicLogsFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC LOGS";
        private readonly string _archiveFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Archive";
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
        /// Proceso completo para D5 Gestiones:
        /// 1. Identifica archivos Excel que cumplen con el patrón "Re_GestionesRO_*.xlsx".
        /// 2. Procesa el XLSX en chunks, eliminando saltos de línea y delimitadores no deseados, y convirtiendo correctamente las fechas.
        /// 3. Genera un CSV delimitado por '|' (UTF-8 con BOM).
        /// 4. Carga el CSV en la tabla de staging (D5_Stage_Gestiones).
        /// 5. Inserta en la tabla final (D5_Gestiones) solo los registros nuevos mediante LEFT JOIN con pseudo-concat.
        /// </summary>
        [HttpPost("D5_ProcessGestiones")]
        public async Task<IActionResult> D5_ProcessGestiones()
        {
            var logPath = Path.Combine(_logsFolder, _logFileName);
            var logBuilder = new StringBuilder();
            bool hasErrors = false;

            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - D5 process started.");
            _logger.LogInformation("D5 process started.");

            // 1. Identificar archivos Excel según el patrón.
            var files = Directory.GetFiles(_filePath, "Re_GestionesRO_*.xlsx");
            if (files.Length == 0)
            {
                string errorLog = "No se encontraron archivos que cumplan el patrón 'Re_GestionesRO_*.xlsx'.";
                logBuilder.AppendLine(errorLog);
                _logger.LogError(errorLog);
                await WriteLogAsync(logBuilder.ToString(), logPath);
                return NotFound(errorLog);
            }

            // 2. Truncar la tabla de staging.
            await TruncateStagingTableAsync(logBuilder);

            // Procesar cada archivo.
            foreach (var file in files)
            {
                string? csvFilePath = null;
                logBuilder.AppendLine($"Processing file: {file}");
                try
                {
                    // 3. Procesar el XLSX en chunks: limpiar saltos de línea y delimitadores, y convertir fechas.
                    csvFilePath = await ProcessXlsxToCsvAsync(file, logBuilder);

                    // 4. Cargar el CSV a la tabla de staging.
                    await BulkInsertToStageAsync(csvFilePath, logBuilder);

                    // 5. Insertar solo registros nuevos a la tabla final.
                    await InsertToFinalTableAsync(logBuilder);

                    // Mover archivos a carpeta Archive.
                    MoveFile(file, _archiveFolder, logBuilder);
                    if (csvFilePath != null)
                        MoveFile(csvFilePath, _archiveFolder, logBuilder);
                }
                catch (Exception ex)
                {
                    logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                    _logger.LogError(ex, $"Error processing file {file}");
                    hasErrors = true;
                    // Mover archivos problemáticos a carpeta Error.
                    MoveFile(file, _errorFolder, logBuilder);
                    if (csvFilePath != null && System.IO.File.Exists(csvFilePath))
                        MoveFile(csvFilePath, _errorFolder, logBuilder);
                    await WriteLogAsync(logBuilder.ToString(), logPath);
                    throw;
                }
            }

            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - D5 process completed.");
            _logger.LogInformation("D5 process completed.");
            await WriteLogAsync(logBuilder.ToString(), logPath);
            MoveLogToHistoric(logPath, _historicLogsFolder);

            return hasErrors
                ? StatusCode(500, "D5 process completed with errors. Check the log for details.")
                : Ok("D5 process completed successfully.");
        }

        #region Métodos de Procesamiento de XLSX a CSV

        /// <summary>
        /// Procesa el archivo XLSX en chunks y genera un CSV delimitado por '|' con encoding UTF-8 con BOM.
        /// Se limpia cada celda (eliminando saltos de línea y '|' ) y se convierte el contenido de celdas de fecha usando su valor nativo.
        /// </summary>
        private async Task<string> ProcessXlsxToCsvAsync(string xlsxFilePath, StringBuilder logBuilder)
        {
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
                        logBuilder.AppendLine($"Processing rows {startRow} to {endRow}.");

                        for (int row = startRow; row <= endRow; row++)
                        {
                            var cells = new List<string>();
                            for (int col = 1; col <= totalCols; col++)
                            {
                                var cell = worksheet.Cells[row, col];
                                string cellValue;
                                if (cell.Value is DateTime dt)
                                {
                                    // Formateamos a "yyyy-MM-dd" ya que en MySQL se espera DATE.
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
            logBuilder.AppendLine($"CSV file generated: {tempCsvPath}");
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
        /// Intenta convertir una cadena de fecha a un formato MySQL datetime aceptable (solo la parte de la fecha).
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
        private async Task TruncateStagingTableAsync(StringBuilder logBuilder)
        {
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                string truncateSql = "TRUNCATE TABLE D5_Stage_Gestiones;";
                using (var cmd = new MySqlCommand(truncateSql, connection))
                {
                    await cmd.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D5_Stage_Gestiones.");
                    _logger.LogInformation("Truncated table D5_Stage_Gestiones.");
                }
            }
        }

        /// <summary>
        /// Carga el CSV generado a la tabla de staging D5_Stage_Gestiones mediante LOAD DATA LOCAL INFILE.
        /// </summary>
        private async Task BulkInsertToStageAsync(string csvFilePath, StringBuilder logBuilder)
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
                            logBuilder.AppendLine("Data loaded into D5_Stage_Gestiones.");
                        }
                        await transaction.CommitAsync();
                    }
                    catch (Exception ex)
                    {
                        await transaction.RollbackAsync();
                        logBuilder.AppendLine($"Bulk insert error: {ex.Message}");
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Inserta solo los registros nuevos de D5_Stage_Gestiones en la tabla final D5_Gestiones.
        /// Se utiliza CONCAT_WS para generar una clave única en ambas tablas y LEFT JOIN para filtrar registros ya existentes.
        /// Además, se convierte Fecha_Actividad y Fecha_Promesa con STR_TO_DATE.
        /// </summary>
        private async Task InsertToFinalTableAsync(StringBuilder logBuilder)
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
                    DATE(STR_TO_DATE(NULLIF(s.Fecha_Actividad, ''), '%Y-%m-%d %H:%i:%s')) AS Fecha_Actividad,
                    STR_TO_DATE(NULLIF(s.Fecha_Promesa, ''), '%Y-%m-%d') AS Fecha_Promesa,
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
                        s.Agencia_Registro, s.Causa_No_Pago, s.Causa_No_Domiciliacion, s.Codigo_Accion, s.Codigo_Resultado,
                        s.Comentarios, s.Contacto_Generado, s.Coordenadas, s.Credito, s.Estatus_Promesa,
                        s.Fecha_Actividad, s.Fecha_Promesa, s.Monto_Promesa, s.Origen, s.Producto,
                        s.Resultado, s.Telefono, s.Tipo_Pago, s.Usuario_Registro
                     )
                     =
                     CONCAT_WS('|',
                        f.Agencia_Registro, f.Causa_No_Pago, f.Causa_No_Domiciliacion, f.Codigo_Accion, f.Codigo_Resultado,
                        f.Comentarios, f.Contacto_Generado, f.Coordenadas, f.Id_Credito, f.Estatus_Promesa,
                        f.Fecha_Actividad, f.Fecha_Promesa, f.Monto_Promesa, f.Origen, f.Producto,
                        f.Resultado, f.Telefono, f.Tipo_Pago, f.Usuario_Registro
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
                            logBuilder.AppendLine($"Inserted {rowsAffected} new rows into D5_Gestiones.");
                        }
                        await transaction.CommitAsync();
                    }
                    catch (Exception ex)
                    {
                        await transaction.RollbackAsync();
                        logBuilder.AppendLine($"Final insert error: {ex.Message}");
                        throw;
                    }
                }
            }
        }

        #endregion

        #region Métodos Genéricos de Log y Movimiento de Archivos

        private async Task WriteLogAsync(string content, string logPath)
        {
            try
            {
                var directory = Path.GetDirectoryName(logPath);
                if (!Directory.Exists(directory))
                    Directory.CreateDirectory(directory!);
                await System.IO.File.WriteAllTextAsync(logPath, content);
                _logger.LogInformation($"Log written to: {logPath}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error writing log to {logPath}");
                throw;
            }
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
                logBuilder.AppendLine($"Error moving file {sourceFilePath}: {ex.Message}");
                _logger.LogError(ex, $"Error moving file {sourceFilePath}");
                throw;
            }
        }

        private void MoveLogToHistoric(string logPath, string historicLogsFolder)
        {
            try
            {
                if (!System.IO.File.Exists(logPath))
                {
                    _logger.LogWarning($"Log file does not exist: {logPath}");
                    return;
                }
                if (!Directory.Exists(historicLogsFolder))
                    Directory.CreateDirectory(historicLogsFolder);
                var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                var newLogName = Path.GetFileNameWithoutExtension(logPath) + $"_{timestamp}" + Path.GetExtension(logPath);
                var destPath = Path.Combine(historicLogsFolder, newLogName);
                System.IO.File.Move(logPath, destPath);
                _logger.LogInformation($"Log moved to historic folder: {destPath}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error moving log file to historic folder.");
                throw;
            }
        }

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
                _logger.LogInformation($"Existing log moved: {destFilePath}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error moving existing log: {ex.Message}");
            }
        }

        #endregion
    }
}
