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

public class D8_Sistema_Controller : Controller
{
    private readonly ILogger<D8_Sistema_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

    public D8_Sistema_Controller(ILogger<D8_Sistema_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
    }

    [HttpPost]
    public async Task<IActionResult> D8_ProcessSistema()
    {
        string controllerName = this.GetType().Name;
        string logFileName = controllerName + ".log";
        string logPath = Path.Combine(@"C:\Users\Go Credit\Documents\DATA\LOGS", logFileName);
        RotateLogIfNeeded(logPath, @"C:\Users\Go Credit\Documents\DATA\HISTORIC LOGS");

        string archiveFolder = Path.Combine(_historicFilePath, "Archive");
        string processedFolder = Path.Combine(_historicFilePath, "Processed");
        string errorFolder = Path.Combine(_historicFilePath, "Error");

        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {controllerName} process started.");
        _logger.LogInformation("Process started.");

        var files = Directory.GetFiles(_filePath, "Re_Sistema*.xlsx");
        if (files.Length == 0)
        {
            string errorMsg = "ERR001: No XLSX file found.";
            logBuilder.AppendLine(errorMsg);
            _logger.LogError(errorMsg);
            await D8_WriteLogAsync(logBuilder.ToString(), logPath);
            return NotFound(errorMsg);
        }

        await D8_TruncateStageTableAsync(logBuilder);

        try
        {
            foreach (var file in files)
            {
                logBuilder.AppendLine($"Processing file: {file}");
                _logger.LogInformation($"Processing file: {file}");

                await D8_ProcessLargeXlsx(file, logBuilder);
                await D8_InsertToFinalTable(logBuilder);
                D8_MoveFile(file, archiveFolder, logBuilder);
                MoveOptionalFile(file, "_converted.xlsx", processedFolder, logBuilder);
                MoveOptionalFile(file, "_sanitized.xlsx", processedFolder, logBuilder);
            }
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"ERR002: Error during processing - {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            // En caso de error, mover todos los archivos a la carpeta de Error
            foreach (var file in files)
            {
                D8_MoveFile(file, errorFolder, logBuilder);
                MoveOptionalFile(file, "_converted.xlsx", errorFolder, logBuilder);
                MoveOptionalFile(file, "_sanitized.xlsx", errorFolder, logBuilder);
            }
            await D8_WriteLogAsync(logBuilder.ToString(), logPath);
            return StatusCode(500, "ERR002: Error during bulk process - Check the log for details.");
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {controllerName} process completed.");
        _logger.LogInformation("Process completed.");
        await D8_WriteLogAsync(logBuilder.ToString(), logPath);
        return Ok("SUCCESS: Process completed successfully");
    }

    private void RotateLogIfNeeded(string logPath, string historicLogsFolder)
    {
        if (System.IO.File.Exists(logPath))
        {
            if (!Directory.Exists(historicLogsFolder))
                Directory.CreateDirectory(historicLogsFolder);
            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string historicLogPath = Path.Combine(historicLogsFolder, $"{Path.GetFileNameWithoutExtension(logPath)}_{timestamp}{Path.GetExtension(logPath)}");
            System.IO.File.Move(logPath, historicLogPath);
        }
    }

    private async Task D8_TruncateStageTableAsync(StringBuilder logBuilder)
    {
        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();
        using var command = new MySqlCommand("TRUNCATE TABLE D8_Stage_Sistema;", connection);
        await command.ExecuteNonQueryAsync();
        logBuilder.AppendLine("Truncated table D8_Stage_Sistema.");
        _logger.LogInformation("Truncated table D8_Stage_Sistema.");
    }

    private async Task D8_ProcessLargeXlsx(string filePath, StringBuilder logBuilder)
    {
        FileInfo fileInfo = new FileInfo(filePath);
        ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
        using var package = new ExcelPackage(fileInfo);
        ExcelWorksheet worksheet = package.Workbook.Worksheets[0];
        int totalRows = worksheet.Dimension.Rows;
        const int chunkSize = 10000; // Procesar 10,000 filas a la vez

        for (int startRow = 2; startRow <= totalRows; startRow += chunkSize)
        {
            int endRow = Math.Min(startRow + chunkSize - 1, totalRows);
            await D8_ProcessChunkAsync(worksheet, startRow, endRow, logBuilder);
        }
    }

    private async Task D8_ProcessChunkAsync(ExcelWorksheet worksheet, int startRow, int endRow, StringBuilder logBuilder)
    {
        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();
        using var transaction = await connection.BeginTransactionAsync();

        try
        {
            for (int row = startRow; row <= endRow; row++)
            {
                // Se leen todas las columnas en el nuevo orden (incluyendo "Indice" en la columna 1)
                var data = new
                {
                    Indice = worksheet.Cells[row, 1].Text.Trim(),
                    Agencia_Asignada_MC = worksheet.Cells[row, 2].Text.Trim(),
                    Agencia_MC = worksheet.Cells[row, 3].Text.Trim(),
                    Bandera_PP_Juicio = worksheet.Cells[row, 4].Text.Trim(),
                    Codigo_MC = worksheet.Cells[row, 5].Text.Trim(),
                    Credito_MC = worksheet.Cells[row, 6].Text.Trim(),
                    Cuenta_Al_Corriente = worksheet.Cells[row, 7].Text.Trim(),
                    No_Gestiones = worksheet.Cells[row, 8].Text.Trim(),
                    No_Visitas = worksheet.Cells[row, 9].Text.Trim(),
                    Nombre_Agencia_MC = worksheet.Cells[row, 10].Text.Trim(),
                    Nombre_Del_Deudor_MC = worksheet.Cells[row, 11].Text.Trim(),
                    Nombre_Instancia_MC = worksheet.Cells[row, 12].Text.Trim(),
                    Producto_MC = worksheet.Cells[row, 13].Text.Trim(),
                    Quita_Exclusiva = worksheet.Cells[row, 14].Text.Trim(),
                    Resultado_MC = worksheet.Cells[row, 15].Text.Trim(),
                    Resultado_Visita_MC = worksheet.Cells[row, 16].Text.Trim(),
                    Saldo_Menor = worksheet.Cells[row, 17].Text.Trim(),
                    Semaforo_Gestion = worksheet.Cells[row, 18].Text.Trim(),
                    Ult_Causa_No_Domiciliacion = worksheet.Cells[row, 19].Text.Trim(),
                    Ult_Causa_No_Pago = worksheet.Cells[row, 20].Text.Trim(),
                    Usuario_Asignado = worksheet.Cells[row, 21].Text.Trim(),
                    Usuario_Asignado_Extrajudicial = worksheet.Cells[row, 22].Text.Trim(),
                    Dias_en_la_instancia_actual = worksheet.Cells[row, 23].Text.Trim(),
                    Dias_Para_Siguiente_Pago = worksheet.Cells[row, 24].Text.Trim(),
                    Estatus_MC = worksheet.Cells[row, 25].Text.Trim(),
                    Estrategia = worksheet.Cells[row, 26].Text.Trim(),
                    Excepciones_MC = worksheet.Cells[row, 27].Text.Trim(),
                    Fecha_de_Asignacion_CallCenter = worksheet.Cells[row, 28].Text.Trim(),
                    Fecha_de_Asignacion_Visita = worksheet.Cells[row, 29].Text.Trim(),
                    Fecha_De_Captura_de_Juicio = worksheet.Cells[row, 30].Text.Trim(),
                    Fecha_de_Ultima_Visita = worksheet.Cells[row, 31].Text.Trim(),
                    Fecha_Promesa_MC = worksheet.Cells[row, 32].Text.Trim(),
                    Fecha_Ult_Gestion_MC = worksheet.Cells[row, 33].Text.Trim(),
                    Importe_Pago_X2 = worksheet.Cells[row, 34].Text.Trim(),
                    Importe_Pago_X3 = worksheet.Cells[row, 35].Text.Trim(),
                    Importe_Pago_X4 = worksheet.Cells[row, 36].Text.Trim(),
                    Importe_Pago_X6 = worksheet.Cells[row, 37].Text.Trim(),
                    Monto_Promesa_MC = worksheet.Cells[row, 38].Text.Trim()
                };

                var sqlInsert = @"
                    INSERT INTO D8_Stage_Sistema (
                        Indice,
                        Agencia_Asignada_MC,
                        Agencia_MC,
                        Bandera_PP_Juicio,
                        Codigo_MC,
                        Credito_MC,
                        Cuenta_Al_Corriente,
                        No_Gestiones,
                        No_Visitas,
                        Nombre_Agencia_MC,
                        Nombre_Del_Deudor_MC,
                        Nombre_Instancia_MC,
                        Producto_MC,
                        Quita_Exclusiva,
                        Resultado_MC,
                        Resultado_Visita_MC,
                        Saldo_Menor,
                        Semaforo_Gestion,
                        Ult_Causa_No_Domiciliacion,
                        Ult_Causa_No_Pago,
                        Usuario_Asignado,
                        Usuario_Asignado_Extrajudicial,
                        Dias_en_la_instancia_actual,
                        Dias_Para_Siguiente_Pago,
                        Estatus_MC,
                        Estrategia,
                        Excepciones_MC,
                        Fecha_de_Asignacion_CallCenter,
                        Fecha_de_Asignacion_Visita,
                        Fecha_De_Captura_de_Juicio,
                        Fecha_de_Ultima_Visita,
                        Fecha_Promesa_MC,
                        Fecha_Ult_Gestion_MC,
                        Importe_Pago_X2,
                        Importe_Pago_X3,
                        Importe_Pago_X4,
                        Importe_Pago_X6,
                        Monto_Promesa_MC
                    ) VALUES (
                        @Indice,
                        @Agencia_Asignada_MC,
                        @Agencia_MC,
                        @Bandera_PP_Juicio,
                        @Codigo_MC,
                        @Credito_MC,
                        @Cuenta_Al_Corriente,
                        @No_Gestiones,
                        @No_Visitas,
                        @Nombre_Agencia_MC,
                        @Nombre_Del_Deudor_MC,
                        @Nombre_Instancia_MC,
                        @Producto_MC,
                        @Quita_Exclusiva,
                        @Resultado_MC,
                        @Resultado_Visita_MC,
                        @Saldo_Menor,
                        @Semaforo_Gestion,
                        @Ult_Causa_No_Domiciliacion,
                        @Ult_Causa_No_Pago,
                        @Usuario_Asignado,
                        @Usuario_Asignado_Extrajudicial,
                        @Dias_en_la_instancia_actual,
                        @Dias_Para_Siguiente_Pago,
                        @Estatus_MC,
                        @Estrategia,
                        @Excepciones_MC,
                        @Fecha_de_Asignacion_CallCenter,
                        @Fecha_de_Asignacion_Visita,
                        @Fecha_De_Captura_de_Juicio,
                        @Fecha_de_Ultima_Visita,
                        @Fecha_Promesa_MC,
                        @Fecha_Ult_Gestion_MC,
                        @Importe_Pago_X2,
                        @Importe_Pago_X3,
                        @Importe_Pago_X4,
                        @Importe_Pago_X6,
                        @Monto_Promesa_MC
                    );";

                using var command = new MySqlCommand(sqlInsert, connection, transaction);
                AddParameter(command, "@Indice", data.Indice, ParseNullableInt);
                AddParameter(command, "@Agencia_Asignada_MC", data.Agencia_Asignada_MC);
                AddParameter(command, "@Agencia_MC", data.Agencia_MC, ParseNullableInt);
                AddParameter(command, "@Bandera_PP_Juicio", data.Bandera_PP_Juicio, ParseNullableInt);
                AddParameter(command, "@Codigo_MC", data.Codigo_MC, ParseNullableInt);
                AddParameter(command, "@Credito_MC", data.Credito_MC, ParseNullableInt);
                AddParameter(command, "@Cuenta_Al_Corriente", data.Cuenta_Al_Corriente);
                AddParameter(command, "@No_Gestiones", data.No_Gestiones, ParseNullableInt);
                AddParameter(command, "@No_Visitas", data.No_Visitas, ParseNullableInt);
                AddParameter(command, "@Nombre_Agencia_MC", data.Nombre_Agencia_MC);
                AddParameter(command, "@Nombre_Del_Deudor_MC", data.Nombre_Del_Deudor_MC);
                AddParameter(command, "@Nombre_Instancia_MC", data.Nombre_Instancia_MC);
                AddParameter(command, "@Producto_MC", data.Producto_MC);
                AddParameter(command, "@Quita_Exclusiva", data.Quita_Exclusiva);
                AddParameter(command, "@Resultado_MC", data.Resultado_MC);
                AddParameter(command, "@Resultado_Visita_MC", data.Resultado_Visita_MC);
                AddParameter(command, "@Saldo_Menor", data.Saldo_Menor, ParseNullableDecimal);
                AddParameter(command, "@Semaforo_Gestion", data.Semaforo_Gestion);
                AddParameter(command, "@Ult_Causa_No_Domiciliacion", data.Ult_Causa_No_Domiciliacion);
                AddParameter(command, "@Ult_Causa_No_Pago", data.Ult_Causa_No_Pago);
                AddParameter(command, "@Usuario_Asignado", data.Usuario_Asignado);
                AddParameter(command, "@Usuario_Asignado_Extrajudicial", data.Usuario_Asignado_Extrajudicial);
                AddParameter(command, "@Dias_en_la_instancia_actual", data.Dias_en_la_instancia_actual, ParseNullableInt);
                AddParameter(command, "@Dias_Para_Siguiente_Pago", data.Dias_Para_Siguiente_Pago, ParseNullableInt);
                AddParameter(command, "@Estatus_MC", data.Estatus_MC);
                AddParameter(command, "@Estrategia", data.Estrategia);
                AddParameter(command, "@Excepciones_MC", data.Excepciones_MC);
                AddParameter(command, "@Fecha_de_Asignacion_CallCenter", data.Fecha_de_Asignacion_CallCenter);
                AddParameter(command, "@Fecha_de_Asignacion_Visita", data.Fecha_de_Asignacion_Visita);
                AddParameter(command, "@Fecha_De_Captura_de_Juicio", data.Fecha_De_Captura_de_Juicio);
                AddParameter(command, "@Fecha_de_Ultima_Visita", data.Fecha_de_Ultima_Visita);
                AddParameter(command, "@Fecha_Promesa_MC", data.Fecha_Promesa_MC);
                AddParameter(command, "@Fecha_Ult_Gestion_MC", data.Fecha_Ult_Gestion_MC);
                AddParameter(command, "@Importe_Pago_X2", data.Importe_Pago_X2, ParseNullableDecimal);
                AddParameter(command, "@Importe_Pago_X3", data.Importe_Pago_X3, ParseNullableDecimal);
                AddParameter(command, "@Importe_Pago_X4", data.Importe_Pago_X4, ParseNullableDecimal);
                AddParameter(command, "@Importe_Pago_X6", data.Importe_Pago_X6, ParseNullableDecimal);
                AddParameter(command, "@Monto_Promesa_MC", data.Monto_Promesa_MC, ParseNullableDecimal);

                await command.ExecuteNonQueryAsync();
            }
            await transaction.CommitAsync();
            logBuilder.AppendLine($"Inserted {endRow - startRow + 1} records into D8_Stage_Sistema.");
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            logBuilder.AppendLine($"Error inserting chunk data into D8_Stage_Sistema: {ex.Message}");
            _logger.LogError(ex, "Error inserting chunk data into D8_Stage_Sistema.");
            throw;
        }
    }

    private void AddParameter<T>(MySqlCommand command, string parameterName, string value, Func<string, T?> parseFunc) where T : struct
    {
        var parsedValue = parseFunc(value);
        command.Parameters.AddWithValue(parameterName, parsedValue.HasValue ? (object)parsedValue.Value : DBNull.Value);
    }

    private void AddParameter(MySqlCommand command, string parameterName, string value)
    {
        command.Parameters.AddWithValue(parameterName, string.IsNullOrWhiteSpace(value) ? DBNull.Value : (object)value);
    }

    private int? ParseNullableInt(string input)
    {
        return int.TryParse(input, out int result) ? result : (int?)null;
    }

    private decimal? ParseNullableDecimal(string input)
    {
        return decimal.TryParse(input, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal result) ? result : (decimal?)null;
    }

    private void D8_MoveFile(string sourceFilePath, string destinationFolder, StringBuilder logBuilder)
    {
        try
        {
            if (!Directory.Exists(destinationFolder))
                Directory.CreateDirectory(destinationFolder);
            string fileName = Path.GetFileName(sourceFilePath);
            string destinationFilePath = Path.Combine(destinationFolder, fileName);
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

    private void MoveOptionalFile(string originalFile, string suffix, string destinationFolder, StringBuilder logBuilder)
    {
        string relatedFile = originalFile.Replace(".xlsx", suffix);
        if (System.IO.File.Exists(relatedFile))
            D8_MoveFile(relatedFile, destinationFolder, logBuilder);
    }

    private async Task D8_WriteLogAsync(string content, string logPath)
    {
        try
        {
            string logDir = Path.GetDirectoryName(logPath)!;
            if (!Directory.Exists(logDir))
                Directory.CreateDirectory(logDir);
            await System.IO.File.WriteAllTextAsync(logPath, content, Encoding.UTF8);
            _logger.LogInformation($"Log written to: {logPath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error writing log to {logPath}");
            throw;
        }
    }

    private async Task D8_InsertToFinalTable(StringBuilder logBuilder)
    {
        var sqlInsert = @"
            INSERT INTO D8_Sistema (
                Agencia_Asignada_MC,
                Agencia_MC,
                Bandera_PP_Juicio,
                Codigo_MC,
                Credito_MC,
                Cuenta_Al_Corriente,
                Dias_en_la_instancia_actual,
                Dias_Para_Siguiente_Pago,
                Estatus_MC,
                Estrategia,
                Excepciones_MC,
                Fecha_de_Asignacion_CallCenter,
                Fecha_de_Asignacion_Visita,
                Fecha_De_Captura_de_Juicio,
                Fecha_de_Ultima_Visita,
                Fecha_Promesa_MC,
                Fecha_Ult_Gestion_MC,
                Importe_Pago_X2,
                Importe_Pago_X3,
                Importe_Pago_X4,
                Importe_Pago_X6,
                Monto_Promesa_MC,
                No_Gestiones,
                No_Visitas,
                Nombre_Agencia_MC,
                Nombre_Del_Deudor_MC,
                Nombre_Instancia_MC,
                Producto_MC,
                Quita_Exclusiva,
                Resultado_MC,
                Resultado_Visita_MC,
                Saldo_Menor,
                Semaforo_Gestion,
                Ult_Causa_No_Domiciliacion,
                Ult_Causa_No_Pago,
                Usuario_Asignado,
                Usuario_Asignado_Extrajudicial
            )
            SELECT
                Agencia_Asignada_MC,
                Agencia_MC,
                Bandera_PP_Juicio,
                Codigo_MC,
                Credito_MC,
                Cuenta_Al_Corriente,
                Dias_en_la_instancia_actual,
                Dias_Para_Siguiente_Pago,
                Estatus_MC,
                Estrategia,
                Excepciones_MC,
                CASE 
                    WHEN TRIM(Fecha_de_Asignacion_CallCenter) = '' THEN NULL
                    WHEN Fecha_de_Asignacion_CallCenter REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{2}:[0-9]{2}$'
                         THEN STR_TO_DATE(Fecha_de_Asignacion_CallCenter, '%m/%d/%y %H:%i')
                    WHEN Fecha_de_Asignacion_CallCenter REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                         THEN STR_TO_DATE(Fecha_de_Asignacion_CallCenter, '%d/%m/%Y')
                    ELSE NULL
                END,
                CASE 
                    WHEN TRIM(Fecha_de_Asignacion_Visita) = '' THEN NULL
                    WHEN Fecha_de_Asignacion_Visita REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{2}:[0-9]{2}$'
                         THEN STR_TO_DATE(Fecha_de_Asignacion_Visita, '%m/%d/%y %H:%i')
                    WHEN Fecha_de_Asignacion_Visita REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                         THEN STR_TO_DATE(Fecha_de_Asignacion_Visita, '%d/%m/%Y')
                    ELSE NULL
                END,
                CASE 
                    WHEN TRIM(Fecha_De_Captura_de_Juicio) = '' THEN NULL
                    WHEN Fecha_De_Captura_de_Juicio REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{2}:[0-9]{2}$'
                         THEN STR_TO_DATE(Fecha_De_Captura_de_Juicio, '%m/%d/%y %H:%i')
                    WHEN Fecha_De_Captura_de_Juicio REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                         THEN STR_TO_DATE(Fecha_De_Captura_de_Juicio, '%d/%m/%Y')
                    ELSE NULL
                END,
                CASE 
                    WHEN TRIM(Fecha_de_Ultima_Visita) = '' THEN NULL
                    WHEN Fecha_de_Ultima_Visita REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{2}:[0-9]{2}$'
                         THEN STR_TO_DATE(Fecha_de_Ultima_Visita, '%m/%d/%y %H:%i')
                    WHEN Fecha_de_Ultima_Visita REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                         THEN STR_TO_DATE(Fecha_de_Ultima_Visita, '%d/%m/%Y')
                    ELSE NULL
                END,
                CASE 
                    WHEN TRIM(Fecha_Promesa_MC) = '' THEN NULL
                    WHEN Fecha_Promesa_MC REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{2}:[0-9]{2}$'
                         THEN STR_TO_DATE(Fecha_Promesa_MC, '%m/%d/%y %H:%i')
                    WHEN Fecha_Promesa_MC REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                         THEN STR_TO_DATE(Fecha_Promesa_MC, '%d/%m/%Y')
                    ELSE NULL
                END,
                CASE 
                    WHEN TRIM(Fecha_Ult_Gestion_MC) = '' THEN NULL
                    WHEN Fecha_Ult_Gestion_MC REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{2}:[0-9]{2}$'
                         THEN STR_TO_DATE(Fecha_Ult_Gestion_MC, '%m/%d/%y %H:%i')
                    WHEN Fecha_Ult_Gestion_MC REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                         THEN STR_TO_DATE(Fecha_Ult_Gestion_MC, '%d/%m/%Y')
                    ELSE NULL
                END,
                Importe_Pago_X2,
                Importe_Pago_X3,
                Importe_Pago_X4,
                Importe_Pago_X6,
                Monto_Promesa_MC,
                No_Gestiones,
                No_Visitas,
                Nombre_Agencia_MC,
                Nombre_Del_Deudor_MC,
                Nombre_Instancia_MC,
                Producto_MC,
                Quita_Exclusiva,
                Resultado_MC,
                Resultado_Visita_MC,
                Saldo_Menor,
                Semaforo_Gestion,
                Ult_Causa_No_Domiciliacion,
                Ult_Causa_No_Pago,
                Usuario_Asignado,
                Usuario_Asignado_Extrajudicial
            FROM D8_Stage_Sistema;";

        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();
        using var transaction = await connection.BeginTransactionAsync();
        try
        {
            using var command = new MySqlCommand(sqlInsert, connection, transaction);
            int rowsAffected = await command.ExecuteNonQueryAsync();
            logBuilder.AppendLine($"Inserted {rowsAffected} rows into D8_Sistema.");
            await transaction.CommitAsync();
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            logBuilder.AppendLine($"Error inserting data into D8_Sistema: {ex.Message}");
            _logger.LogError(ex, "Error inserting data into D8_Sistema.");
            throw;
        }
    }
}
