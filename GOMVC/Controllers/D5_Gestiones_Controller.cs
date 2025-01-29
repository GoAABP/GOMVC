using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using OfficeOpenXml;
using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;

public class D5_Gestiones_Controller : Controller
{
    private readonly ILogger<D5_Gestiones_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

    public D5_Gestiones_Controller(ILogger<D5_Gestiones_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
    }

    [HttpPost]
    public async Task<IActionResult> D5_ProcessGestiones()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D5_Gestiones.log";
        var historicLogsFolder = Path.Combine(_historicFilePath, "Logs");
        var archiveFolder = Path.Combine(_historicFilePath, "Archive");
        var processedFolder = Path.Combine(_historicFilePath, "Processed");
        var errorFolder = Path.Combine(_historicFilePath, "Error");
        var logBuilder = new StringBuilder();
        var hasErrors = false;

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - D5 process started.");
        _logger.LogInformation("D5 process started.");

        var files = Directory.GetFiles(_filePath, "Re_GestionesRO_*.xlsx");
        if (files.Length == 0)
        {
            var errorLog = "No files matching 'Re_GestionesRO_*.xlsx' found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D5_WriteLog(logBuilder.ToString(), logPath);
            return NotFound(errorLog);
        }

        logBuilder.AppendLine($"{files.Length} files matching 'Re_GestionesRO_*.xlsx' found.");
        _logger.LogInformation($"{files.Length} files matching 'Re_GestionesRO_*.xlsx' found.");

        // Step 1: Truncate the staging table before processing any files
        await D5_TruncateStagingTableAsync(logBuilder);

        foreach (var file in files)
        {
            logBuilder.AppendLine($"Processing file: {file}");
            _logger.LogInformation($"Processing file: {file}");

            try
            {
                // Step 2: Process the Excel file in chunks
                await D5_ProcessLargeXlsx(file, logBuilder);

                // Step 3: Move files to archive/processed folders
                D5_MoveFile(file, archiveFolder, logBuilder);

                // Step 4: Move data from staging to final table
                await D5_MoveDataToFinalTableAsync(logBuilder);

                logBuilder.AppendLine($"File {file} processed successfully.");
                _logger.LogInformation($"File {file} processed successfully.");
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}");
                hasErrors = true;

                // Move problematic files to error folder
                D5_MoveFile(file, errorFolder, logBuilder);
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - D5 process completed.");
        _logger.LogInformation("D5 process completed.");
        await D5_WriteLog(logBuilder.ToString(), logPath);

        // Move the log to the historic folder
        D5_MoveLogToHistoric(logPath, historicLogsFolder);

        return hasErrors
            ? StatusCode(500, "D5 process completed with errors. Check the log for details.")
            : Ok("D5 process completed successfully.");
    }
    
    private async Task D5_TruncateStagingTableAsync(StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            var sqlTruncate = "TRUNCATE TABLE D5_Stage_Gestiones;";
            using (var command = new MySqlCommand(sqlTruncate, connection))
            {
                await command.ExecuteNonQueryAsync();
                logBuilder.AppendLine("Truncated D5_Stage_Gestiones table.");
                _logger.LogInformation("Truncated D5_Stage_Gestiones table.");
            }
        }
    }

    private async Task D5_ProcessLargeXlsx(string filePath, StringBuilder logBuilder)
    {
        FileInfo fileInfo = new FileInfo(filePath);
        using (ExcelPackage package = new ExcelPackage(fileInfo))
        {
            ExcelWorksheet worksheet = package.Workbook.Worksheets[0];
            int totalRows = worksheet.Dimension.Rows;
            int chunkSize = 10000; // Process 10,000 rows at a time

            for (int startRow = 2; startRow <= totalRows; startRow += chunkSize) // Start from 2 to skip header
            {
                int endRow = Math.Min(startRow + chunkSize - 1, totalRows);
                await D5_ProcessChunkAsync(worksheet, startRow, endRow, logBuilder);
            }
        }
    }

    private async Task D5_ProcessChunkAsync(ExcelWorksheet worksheet, int startRow, int endRow, StringBuilder logBuilder)
    {
        for (int row = startRow; row <= endRow; row++)
        {
            // Read values from the worksheet based on the provided headers
            var indice = int.TryParse(worksheet.Cells[row, 1].Text.Trim(), out var ind) ? ind : (int?)null;
            var agenciaRegistro = worksheet.Cells[row, 2].Text.Trim();
            var causaNoPago = worksheet.Cells[row, 3].Text.Trim();
            var causaNoDomiciliacion = worksheet.Cells[row, 4].Text.Trim();
            var codigoAccion = worksheet.Cells[row, 5].Text.Trim();
            var codigoResultado = worksheet.Cells[row, 6].Text.Trim();
            var comentarios = worksheet.Cells[row, 7].Text.Trim();
            var contactoGenerado = worksheet.Cells[row, 8].Text.Trim();
            var coordenadas = worksheet.Cells[row, 9].Text.Trim();
            var credito = int.TryParse(worksheet.Cells[row, 10].Text.Trim(), out var cred) ? cred : (int?)null;
            var estatusPromesa = worksheet.Cells[row, 11].Text.Trim();
            var fechaActividadStr = worksheet.Cells[row, 12].Text.Trim(); // Keep as string
            var fechaPromesaStr = worksheet.Cells[row, 13].Text.Trim(); // Keep as string
            var montoPromesa = decimal.TryParse(worksheet.Cells[row, 14].Text.Trim(), out var monto) ? monto : (decimal?)null;
            var origen = worksheet.Cells[row, 15].Text.Trim();
            var producto = worksheet.Cells[row, 16].Text.Trim();
            var resultado = worksheet.Cells[row, 17].Text.Trim();
            var telefono = worksheet.Cells[row, 18].Text.Trim();
            var tipoPago = worksheet.Cells[row, 19].Text.Trim();
            var usuarioRegistro = worksheet.Cells[row, 20].Text.Trim();

            // Log the raw values for debugging
            logBuilder.AppendLine($"Row {row}: Raw Fecha Actividad='{fechaActividadStr}', Raw Fecha Promesa='{fechaPromesaStr}'");

            // Save to database as strings
            await D5_SaveToDatabaseAsync(indice, agenciaRegistro, causaNoPago, causaNoDomiciliacion, codigoAccion, codigoResultado,
                comentarios, contactoGenerado, coordenadas, credito, estatusPromesa, fechaActividadStr, fechaPromesaStr,
                montoPromesa, origen, producto, resultado, telefono, tipoPago, usuarioRegistro, logBuilder);
        }
    }

    private async Task D5_SaveToDatabaseAsync(int? indice, string agenciaRegistro, string causaNoPago, string causaNoDomiciliacion,
    string codigoAccion, string codigoResultado, string comentarios, string contactoGenerado, string coordenadas,
    int? credito, string estatusPromesa, string fechaActividadStr, string fechaPromesaStr, // Change to string
    decimal? montoPromesa, string origen, string producto, string resultado, string telefono, string tipoPago, string usuarioRegistro,
    StringBuilder logBuilder)
    {
        var sqlInsertCommand = @"
            INSERT INTO D5_Stage_Gestiones (
                Indice, Agencia_Registro, Causa_No_Pago, Causa_No_Domiciliacion, Codigo_Accion, Codigo_Resultado,
                Comentarios, Contacto_Generado, Coordenadas, Credito, Estatus_Promesa, Fecha_Actividad,
                Fecha_Promesa, Monto_Promesa, Origen, Producto, Resultado, Telefono, Tipo_Pago, Usuario_Registro
            )
            VALUES (
                @Indice, @AgenciaRegistro, @CausaNoPago, @CausaNoDomiciliacion, @CodigoAccion, @CodigoResultado,
                @Comentarios, @ContactoGenerado, @Coordenadas, @Credito, @EstatusPromesa, 
                @FechaActividad, @FechaPromesa, @MontoPromesa, @Origen, @Producto, @Resultado, 
                @Telefono, @TipoPago, @UsuarioRegistro
            );";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    command.Parameters.AddWithValue("@Indice", indice.HasValue ? (object)indice.Value : DBNull.Value);
                    command.Parameters.AddWithValue("@AgenciaRegistro", agenciaRegistro);
                    command.Parameters.AddWithValue("@CausaNoPago", causaNoPago);
                    command.Parameters.AddWithValue("@CausaNoDomiciliacion", causaNoDomiciliacion);
                    command.Parameters.AddWithValue("@CodigoAccion", codigoAccion);
                    command.Parameters.AddWithValue("@CodigoResultado", codigoResultado);
                    command.Parameters.AddWithValue("@Comentarios", comentarios);
                    command.Parameters.AddWithValue("@ContactoGenerado", contactoGenerado);
                    command.Parameters.AddWithValue("@Coordenadas", coordenadas);
                    command.Parameters.AddWithValue("@Credito", credito.HasValue ? (object)credito.Value : DBNull.Value);
                    command.Parameters.AddWithValue("@EstatusPromesa", estatusPromesa);
                    command.Parameters.AddWithValue("@FechaActividad", fechaActividadStr); // Use string directly
                    command.Parameters.AddWithValue("@FechaPromesa", fechaPromesaStr); // Use string directly
                    command.Parameters.AddWithValue("@MontoPromesa", montoPromesa.HasValue ? (object)montoPromesa.Value : DBNull.Value);
                    command.Parameters.AddWithValue("@Origen", origen);
                    command.Parameters.AddWithValue("@Producto", producto);
                    command.Parameters.AddWithValue("@Resultado", resultado);
                    command.Parameters.AddWithValue("@Telefono", telefono);
                    command.Parameters.AddWithValue("@TipoPago", tipoPago);
                    command.Parameters.AddWithValue("@UsuarioRegistro", usuarioRegistro);

                    await command.ExecuteNonQueryAsync();
                    await transaction.CommitAsync();

                    logBuilder.AppendLine($"Inserted record into D5_Stage_Gestiones: {agenciaRegistro}, {causaNoPago}, {credito}");
                    _logger.LogInformation($"Inserted record into D5_Stage_Gestiones: {agenciaRegistro}, {causaNoPago}, {credito}");
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during insert into D5_Stage_Gestiones: {ex.Message}");
                    _logger.LogError(ex, "Error during insert into D5_Stage_Gestiones.");
                    throw;
                }
            }
        }
    }

    [HttpPost]
    public async Task<IActionResult> D5_ProcessDataToFinalTable()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D5_Gestiones.log";
        var logBuilder = new StringBuilder();

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process to move data to final table started.");
        _logger.LogInformation("Process to move data to final table started.");

        try
        {
            // Call the method to move data to the final table
            await D5_MoveDataToFinalTableAsync(logBuilder);

            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Data moved to final table successfully.");
            _logger.LogInformation("Data moved to final table successfully.");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error moving data to final table: {ex.Message}");
            _logger.LogError(ex, "Error moving data to final table.");
            return StatusCode(500, "Error moving data to final table. Check the log for details.");
        }
        finally
        {
            // Write the log to the specified log file
            await D5_WriteLog(logBuilder.ToString(), logPath);
        }

        return Ok("Data moved to final table successfully.");
    }
   
    private async Task D5_MoveDataToFinalTableAsync(StringBuilder logBuilder)
    {
        var sqlInsertCommand = @"
            -- Step 1: Create a temporary table for transformed data
            CREATE TEMPORARY TABLE Temp_D5_Stage AS
            SELECT 
                s.Agencia_Registro, s.Causa_No_Pago, s.Causa_No_Domiciliacion, s.Codigo_Accion, s.Codigo_Resultado,
                s.Comentarios, s.Contacto_Generado, s.Coordenadas, s.Credito, s.Estatus_Promesa,

                -- Ensure empty strings are NULL before conversion
                CASE 
                    WHEN TRIM(NULLIF(s.Fecha_Actividad, '')) IS NULL THEN NULL
                    
                    -- MM/dd/yy H:mm (Single-Digit Hour)
                    WHEN s.Fecha_Actividad REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{1}:[0-9]{2}$' 
                    THEN STR_TO_DATE(s.Fecha_Actividad, '%m/%d/%y %k:%i')

                    -- MM/dd/yy HH:mm (Two-Digit Hour)
                    WHEN s.Fecha_Actividad REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{2}:[0-9]{2}$' 
                    THEN STR_TO_DATE(s.Fecha_Actividad, '%m/%d/%y %H:%i')

                    -- MM/dd/yy H:mm:ss (Single-Digit Hour with Seconds)
                    WHEN s.Fecha_Actividad REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{1}:[0-9]{2}:[0-9]{2}$' 
                    THEN STR_TO_DATE(s.Fecha_Actividad, '%m/%d/%y %k:%i:%s')

                    -- MM/dd/yy HH:mm:ss (Two-Digit Hour with Seconds)
                    WHEN s.Fecha_Actividad REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' 
                    THEN STR_TO_DATE(s.Fecha_Actividad, '%m/%d/%y %H:%i:%s')

                    -- dd/MM/yyyy H:mm (Single-Digit Hour)
                    WHEN s.Fecha_Actividad REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{1}:[0-9]{2}$' 
                    THEN STR_TO_DATE(s.Fecha_Actividad, '%d/%m/%Y %k:%i')

                    -- dd/MM/yyyy HH:mm (Two-Digit Hour)
                    WHEN s.Fecha_Actividad REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}$' 
                    THEN STR_TO_DATE(s.Fecha_Actividad, '%d/%m/%Y %H:%i')

                    -- dd/MM/yyyy (Defaults to 00:00:00 Time)
                    WHEN s.Fecha_Actividad REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' 
                    THEN STR_TO_DATE(CONCAT(s.Fecha_Actividad, ' 00:00:00'), '%d/%m/%Y %H:%i:%s')

                    ELSE NULL
                END AS Fecha_Actividad,

                -- Ensure Fecha_Promesa is correctly parsed
                CASE 
                    WHEN TRIM(NULLIF(s.Fecha_Promesa, '')) IS NULL THEN NULL
                    WHEN s.Fecha_Promesa REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' 
                    THEN STR_TO_DATE(s.Fecha_Promesa, '%d/%m/%Y')
                    ELSE NULL
                END AS Fecha_Promesa,

                s.Monto_Promesa, s.Origen, s.Producto, s.Resultado, s.Telefono, s.Tipo_Pago, s.Usuario_Registro
            FROM D5_Stage_Gestiones s;

            -- Step 2: Insert processed records into D5_Gestiones
            INSERT INTO D5_Gestiones (
                Agencia_Registro, Causa_No_Pago, Causa_No_Domiciliacion, Codigo_Accion, Codigo_Resultado,
                Comentarios, Contacto_Generado, Coordenadas, Id_Credito, Estatus_Promesa,
                Fecha_Actividad, Fecha_Promesa, Monto_Promesa, Origen, Producto, 
                Resultado, Telefono, Tipo_Pago, Usuario_Registro
            )
            SELECT 
                t.Agencia_Registro, t.Causa_No_Pago, t.Causa_No_Domiciliacion, t.Codigo_Accion, t.Codigo_Resultado,
                t.Comentarios, t.Contacto_Generado, t.Coordenadas, t.Credito, t.Estatus_Promesa,
                t.Fecha_Actividad, t.Fecha_Promesa, t.Monto_Promesa, t.Origen, t.Producto, 
                t.Resultado, t.Telefono, t.Tipo_Pago, t.Usuario_Registro
            FROM Temp_D5_Stage t;

            -- Step 3: Drop the temporary table
            DROP TEMPORARY TABLE IF EXISTS Temp_D5_Stage;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var command = new MySqlCommand(sqlInsertCommand, connection))
            {
                var affectedRows = await command.ExecuteNonQueryAsync();
                logBuilder.AppendLine($"Inserted {affectedRows} new records into D5_Gestiones.");
            }
        }
    }
    
    private void D5_MoveFile(string sourceFilePath, string destinationFolder, StringBuilder logBuilder)
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
            logBuilder.AppendLine($"Moved file: {sourceFilePath} -> {destinationFilePath}");
            _logger.LogInformation($"Moved file: {sourceFilePath} -> {destinationFilePath}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error moving file {sourceFilePath} to {destinationFolder}: {ex.Message}");
            _logger.LogError(ex, $"Error moving file {sourceFilePath} to {destinationFolder}");
        }
    }

    private void D5_MoveLogToHistoric(string logPath, string historicLogsFolder)
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

    private async Task D5_WriteLog(string content, string logPath)
    {
        try
        {
            if (!Directory.Exists(Path.GetDirectoryName(logPath)))
            {
                Directory.CreateDirectory(Path.GetDirectoryName(logPath)!);
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
}