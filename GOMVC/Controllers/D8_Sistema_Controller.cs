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
        // Generación dinámica del nombre del log usando el nombre del controlador
        string controllerName = this.GetType().Name;
        string logFileName = controllerName + ".log";
        string logPath = Path.Combine(@"C:\Users\Go Credit\Documents\DATA\LOGS", logFileName);

        // Si ya existe un log con el mismo nombre, moverlo a HISTORIC LOGS
        string historicLogsFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC LOGS";
        if (System.IO.File.Exists(logPath))
        {
            if (!Directory.Exists(historicLogsFolder))
                Directory.CreateDirectory(historicLogsFolder);
            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string historicLogPath = Path.Combine(historicLogsFolder, controllerName + "_" + timestamp + ".log");
            System.IO.File.Move(logPath, historicLogPath);
        }

        // Definición de carpetas destino para archivos según el resultado del proceso
        string archiveFolder = Path.Combine(_historicFilePath, "Archive");
        string processedFolder = Path.Combine(_historicFilePath, "Processed");
        // Para errores se mueve a la carpeta "Error" en HISTORIC FILES
        string errorFolder = Path.Combine(_historicFilePath, "Error");

        var logBuilder = new StringBuilder();
        bool hasErrors = false;

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {controllerName} process started.");
        _logger.LogInformation("Process started.");

        // Se buscan archivos XLSX con el patrón "Re_Sistema*.xlsx"
        var files = Directory.GetFiles(_filePath, "Re_Sistema*.xlsx");
        if (files.Length == 0)
        {
            var errorMsg = "ERR001: No XLSX file found.";
            logBuilder.AppendLine(errorMsg);
            _logger.LogError(errorMsg);
            await D8_WriteLog(logBuilder.ToString(), logPath);
            return NotFound(errorMsg);
        }

        // Truncar la tabla de staging antes de procesar nuevos datos
        await D8_TruncateStageTableAsync(logBuilder);

        foreach (var file in files)
        {
            logBuilder.AppendLine($"Processing file: {file}");
            _logger.LogInformation($"Processing file: {file}");
            try
            {
                await D8_ProcessLargeXlsx(file, logBuilder);
                // Después de cargar en staging, se inserta en la tabla final
                await D8_InsertToFinalTable(logBuilder);

                // Movimiento de archivos en caso de Éxito:
                // Mover el archivo original a Archive
                D8_MoveFile(file, archiveFolder, logBuilder);
                // Si existen archivos relacionados, moverlos a Processed
                string convertedFile = file.Replace(".xlsx", "_converted.xlsx");
                if (System.IO.File.Exists(convertedFile))
                    D8_MoveFile(convertedFile, processedFolder, logBuilder);
                string sanitizedFile = file.Replace(".xlsx", "_sanitized.xlsx");
                if (System.IO.File.Exists(sanitizedFile))
                    D8_MoveFile(sanitizedFile, processedFolder, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"ERR002: Error processing XLSX file {file} - {ex.Message}");
                _logger.LogError(ex, $"ERR002: Error processing XLSX file {file}");
                hasErrors = true;

                // Movimiento de todos los archivos relacionados a la carpeta de Error
                D8_MoveFile(file, errorFolder, logBuilder);
                string convertedFile = file.Replace(".xlsx", "_converted.xlsx");
                if (System.IO.File.Exists(convertedFile))
                    D8_MoveFile(convertedFile, errorFolder, logBuilder);
                string sanitizedFile = file.Replace(".xlsx", "_sanitized.xlsx");
                if (System.IO.File.Exists(sanitizedFile))
                    D8_MoveFile(sanitizedFile, errorFolder, logBuilder);

                // Se relanza la excepción para que el proceso global falle
                throw;
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {controllerName} process completed.");
        _logger.LogInformation("Process completed.");
        await D8_WriteLog(logBuilder.ToString(), logPath);

        return hasErrors
            ? StatusCode(500, "ERR002: Error during bulk process - Check the log for details.")
            : Ok("SUCCESS: Process completed successfully");
    }

    private async Task D8_TruncateStageTableAsync(StringBuilder logBuilder)
    {
        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();
        var sqlTruncate = "TRUNCATE TABLE D8_Stage_Sistema;";
        using var command = new MySqlCommand(sqlTruncate, connection);
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
        int chunkSize = 10000; // Procesar 10,000 filas a la vez

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
                // Mapeo de columnas del Excel a columnas de la tabla staging.
                // Se omite la primera columna "Indice".
                string agenciaAsignadaMC = worksheet.Cells[row, 2].Text.Trim();
                string agenciaMCStr = worksheet.Cells[row, 3].Text.Trim();
                string banderaPPJuicioStr = worksheet.Cells[row, 4].Text.Trim();
                string codigoMCStr = worksheet.Cells[row, 5].Text.Trim();
                string creditoMCStr = worksheet.Cells[row, 6].Text.Trim();
                string cuentaAlCorriente = worksheet.Cells[row, 7].Text.Trim();
                string diasInstanciaActualStr = worksheet.Cells[row, 8].Text.Trim();
                string diasParaSiguientePagoStr = worksheet.Cells[row, 9].Text.Trim();
                string estatusMC = worksheet.Cells[row, 10].Text.Trim();
                string estrategia = worksheet.Cells[row, 11].Text.Trim();
                string excepcionesMC = worksheet.Cells[row, 12].Text.Trim();
                string fechaAsignacionCallCenter = worksheet.Cells[row, 13].Text.Trim();
                string fechaAsignacionVisita = worksheet.Cells[row, 14].Text.Trim();
                string fechaCapturaJuicio = worksheet.Cells[row, 15].Text.Trim();
                string fechaUltimaVisita = worksheet.Cells[row, 16].Text.Trim();
                string fechaPromesaMC = worksheet.Cells[row, 17].Text.Trim();
                string fechaUltGestionMC = worksheet.Cells[row, 18].Text.Trim();
                string importePagoX2Str = worksheet.Cells[row, 19].Text.Trim();
                string importePagoX3Str = worksheet.Cells[row, 20].Text.Trim();
                string importePagoX4Str = worksheet.Cells[row, 21].Text.Trim();
                string importePagoX6Str = worksheet.Cells[row, 22].Text.Trim();
                string montoPromesaMCStr = worksheet.Cells[row, 23].Text.Trim();
                string noGestionesStr = worksheet.Cells[row, 24].Text.Trim();
                string noVisitasStr = worksheet.Cells[row, 25].Text.Trim();
                string nombreAgenciaMC = worksheet.Cells[row, 26].Text.Trim();
                string nombreDelDeudorMC = worksheet.Cells[row, 27].Text.Trim();
                string nombreInstanciaMC = worksheet.Cells[row, 28].Text.Trim();
                string productoMC = worksheet.Cells[row, 29].Text.Trim();
                string quitaExclusiva = worksheet.Cells[row, 30].Text.Trim();
                string resultadoMC = worksheet.Cells[row, 31].Text.Trim();
                string resultadoVisitaMC = worksheet.Cells[row, 32].Text.Trim();
                string saldoMenorStr = worksheet.Cells[row, 33].Text.Trim();
                string semaforoGestion = worksheet.Cells[row, 34].Text.Trim();
                string ultCausaNoDomiciliacion = worksheet.Cells[row, 35].Text.Trim();
                string ultCausaNoPago = worksheet.Cells[row, 36].Text.Trim();
                string usuarioAsignado = worksheet.Cells[row, 37].Text.Trim();
                string usuarioAsignadoExtrajudicial = worksheet.Cells[row, 38].Text.Trim();

                // Parseo de valores numéricos con métodos auxiliares
                int? agenciaMC = ParseNullableInt(agenciaMCStr);
                int? banderaPPJuicio = ParseNullableInt(banderaPPJuicioStr);
                int? codigoMC = ParseNullableInt(codigoMCStr);
                int? creditoMC = ParseNullableInt(creditoMCStr);
                int? diasInstanciaActual = ParseNullableInt(diasInstanciaActualStr);
                int? diasParaSiguientePago = ParseNullableInt(diasParaSiguientePagoStr);
                decimal? importePagoX2 = ParseNullableDecimal(importePagoX2Str);
                decimal? importePagoX3 = ParseNullableDecimal(importePagoX3Str);
                decimal? importePagoX4 = ParseNullableDecimal(importePagoX4Str);
                decimal? importePagoX6 = ParseNullableDecimal(importePagoX6Str);
                decimal? montoPromesaMC = ParseNullableDecimal(montoPromesaMCStr);
                int? noGestiones = ParseNullableInt(noGestionesStr);
                int? noVisitas = ParseNullableInt(noVisitasStr);
                decimal? saldoMenor = ParseNullableDecimal(saldoMenorStr);

                var sqlInsert = @"
                    INSERT INTO D8_Stage_Sistema (
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
                    ) VALUES (
                        @Agencia_Asignada_MC,
                        @Agencia_MC,
                        @Bandera_PP_Juicio,
                        @Codigo_MC,
                        @Credito_MC,
                        @Cuenta_Al_Corriente,
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
                        @Monto_Promesa_MC,
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
                        @Usuario_Asignado_Extrajudicial
                    );";

                using var command = new MySqlCommand(sqlInsert, connection, transaction);
                command.Parameters.AddWithValue("@Agencia_Asignada_MC", agenciaAsignadaMC);
                command.Parameters.AddWithValue("@Agencia_MC", agenciaMC.HasValue ? (object)agenciaMC.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Bandera_PP_Juicio", banderaPPJuicio.HasValue ? (object)banderaPPJuicio.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Codigo_MC", codigoMC.HasValue ? (object)codigoMC.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Credito_MC", creditoMC.HasValue ? (object)creditoMC.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Cuenta_Al_Corriente", cuentaAlCorriente);
                command.Parameters.AddWithValue("@Dias_en_la_instancia_actual", diasInstanciaActual.HasValue ? (object)diasInstanciaActual.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Dias_Para_Siguiente_Pago", diasParaSiguientePago.HasValue ? (object)diasParaSiguientePago.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Estatus_MC", estatusMC);
                command.Parameters.AddWithValue("@Estrategia", estrategia);
                command.Parameters.AddWithValue("@Excepciones_MC", excepcionesMC);
                command.Parameters.AddWithValue("@Fecha_de_Asignacion_CallCenter", fechaAsignacionCallCenter);
                command.Parameters.AddWithValue("@Fecha_de_Asignacion_Visita", fechaAsignacionVisita);
                command.Parameters.AddWithValue("@Fecha_De_Captura_de_Juicio", fechaCapturaJuicio);
                command.Parameters.AddWithValue("@Fecha_de_Ultima_Visita", fechaUltimaVisita);
                command.Parameters.AddWithValue("@Fecha_Promesa_MC", fechaPromesaMC);
                command.Parameters.AddWithValue("@Fecha_Ult_Gestion_MC", fechaUltGestionMC);
                command.Parameters.AddWithValue("@Importe_Pago_X2", importePagoX2.HasValue ? (object)importePagoX2.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Importe_Pago_X3", importePagoX3.HasValue ? (object)importePagoX3.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Importe_Pago_X4", importePagoX4.HasValue ? (object)importePagoX4.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Importe_Pago_X6", importePagoX6.HasValue ? (object)importePagoX6.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Monto_Promesa_MC", montoPromesaMC.HasValue ? (object)montoPromesaMC.Value : DBNull.Value);
                command.Parameters.AddWithValue("@No_Gestiones", noGestiones.HasValue ? (object)noGestiones.Value : DBNull.Value);
                command.Parameters.AddWithValue("@No_Visitas", noVisitas.HasValue ? (object)noVisitas.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Nombre_Agencia_MC", nombreAgenciaMC);
                command.Parameters.AddWithValue("@Nombre_Del_Deudor_MC", nombreDelDeudorMC);
                command.Parameters.AddWithValue("@Nombre_Instancia_MC", nombreInstanciaMC);
                command.Parameters.AddWithValue("@Producto_MC", productoMC);
                command.Parameters.AddWithValue("@Quita_Exclusiva", quitaExclusiva);
                command.Parameters.AddWithValue("@Resultado_MC", resultadoMC);
                command.Parameters.AddWithValue("@Resultado_Visita_MC", resultadoVisitaMC);
                command.Parameters.AddWithValue("@Saldo_Menor", saldoMenor.HasValue ? (object)saldoMenor.Value : DBNull.Value);
                command.Parameters.AddWithValue("@Semaforo_Gestion", semaforoGestion);
                command.Parameters.AddWithValue("@Ult_Causa_No_Domiciliacion", ultCausaNoDomiciliacion);
                command.Parameters.AddWithValue("@Ult_Causa_No_Pago", ultCausaNoPago);
                command.Parameters.AddWithValue("@Usuario_Asignado", usuarioAsignado);
                command.Parameters.AddWithValue("@Usuario_Asignado_Extrajudicial", usuarioAsignadoExtrajudicial);

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

            // Si el archivo ya existe en el destino, se elimina para evitar conflictos
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

    private async Task D8_WriteLog(string content, string logPath)
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
                END AS Fecha_de_Asignacion_CallCenter,
                CASE 
                    WHEN TRIM(Fecha_de_Asignacion_Visita) = '' THEN NULL
                    WHEN Fecha_de_Asignacion_Visita REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{2}:[0-9]{2}$'
                        THEN STR_TO_DATE(Fecha_de_Asignacion_Visita, '%m/%d/%y %H:%i')
                    WHEN Fecha_de_Asignacion_Visita REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                        THEN STR_TO_DATE(Fecha_de_Asignacion_Visita, '%d/%m/%Y')
                    ELSE NULL
                END AS Fecha_de_Asignacion_Visita,
                CASE 
                    WHEN TRIM(Fecha_De_Captura_de_Juicio) = '' THEN NULL
                    WHEN Fecha_De_Captura_de_Juicio REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{2}:[0-9]{2}$'
                        THEN STR_TO_DATE(Fecha_De_Captura_de_Juicio, '%m/%d/%y %H:%i')
                    WHEN Fecha_De_Captura_de_Juicio REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                        THEN STR_TO_DATE(Fecha_De_Captura_de_Juicio, '%d/%m/%Y')
                    ELSE NULL
                END AS Fecha_De_Captura_de_Juicio,
                CASE 
                    WHEN TRIM(Fecha_de_Ultima_Visita) = '' THEN NULL
                    WHEN Fecha_de_Ultima_Visita REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{2}:[0-9]{2}$'
                        THEN STR_TO_DATE(Fecha_de_Ultima_Visita, '%m/%d/%y %H:%i')
                    WHEN Fecha_de_Ultima_Visita REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                        THEN STR_TO_DATE(Fecha_de_Ultima_Visita, '%d/%m/%Y')
                    ELSE NULL
                END AS Fecha_de_Ultima_Visita,
                CASE 
                    WHEN TRIM(Fecha_Promesa_MC) = '' THEN NULL
                    WHEN Fecha_Promesa_MC REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{2}:[0-9]{2}$'
                        THEN STR_TO_DATE(Fecha_Promesa_MC, '%m/%d/%y %H:%i')
                    WHEN Fecha_Promesa_MC REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                        THEN STR_TO_DATE(Fecha_Promesa_MC, '%d/%m/%Y')
                    ELSE NULL
                END AS Fecha_Promesa_MC,
                CASE 
                    WHEN TRIM(Fecha_Ult_Gestion_MC) = '' THEN NULL
                    WHEN Fecha_Ult_Gestion_MC REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2} [0-9]{2}:[0-9]{2}$'
                        THEN STR_TO_DATE(Fecha_Ult_Gestion_MC, '%m/%d/%y %H:%i')
                    WHEN Fecha_Ult_Gestion_MC REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                        THEN STR_TO_DATE(Fecha_Ult_Gestion_MC, '%d/%m/%Y')
                    ELSE NULL
                END AS Fecha_Ult_Gestion_MC,
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
