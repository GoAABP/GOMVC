using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

#region Catálogo de Errores
public static class ErrorCatalog
{
    public const int ArchivoNoEncontrado = 1;              // "No se encontró el archivo"
    public const int ErrorConversionArchivo = 2;           // "No se pudo convertir el archivo"
    public const int ErrorBulkInsert = 3;                  // "No se pudo proceder con el bulk insert"
    public const int ErrorInsercionTablaFinal = 4;         // "No se pudo insertar a la tabla final"
    public const int ErrorMoverArchivo = 5;                // "No se pudo mover el archivo"
    public const int ErrorExportacionArchivo = 6;          // "No se pudo exportar el archivo"
    public const int ErrorGeneracionLog = 7;               // "No se pudo generar el log"
    public const int ErrorMoverLog = 8;                    // "No se pudo mover el log"
}

public static class ErrorMessages
{
    public const string ArchivoNoEncontrado = "No se encontró el archivo";
    public const string ErrorConversionArchivo = "No se pudo convertir el archivo";
    public const string ErrorBulkInsert = "No se pudo proceder con el bulk insert";
    public const string ErrorInsercionTablaFinal = "No se pudo insertar a la tabla final";
    public const string ErrorMoverArchivo = "No se pudo mover el archivo";
    public const string ErrorExportacionArchivo = "No se pudo exportar el archivo";
    public const string ErrorGeneracionLog = "No se pudo generar el log";
    public const string ErrorMoverLog = "No se pudo mover el log";
}
#endregion

public class D1_Saldos_Cartera_Controller : Controller
{
    private readonly ILogger<D1_Saldos_Cartera_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

    public D1_Saldos_Cartera_Controller(ILogger<D1_Saldos_Cartera_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
    }

    [HttpPost]
    public async Task<IActionResult> D1_ProcessSaldosCartera()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D1_Saldos_Cartera.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.");
        _logger.LogInformation("Process started.");

        var files = Directory.GetFiles(_filePath, "SaldosCarteraXConvenio*.csv");
        if (files.Length == 0)
        {
            // Error Código 1: Archivo No Encontrado
            var errorLog = $"{ErrorMessages.ArchivoNoEncontrado} (Código {ErrorCatalog.ArchivoNoEncontrado})";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D1_WriteLog(logBuilder.ToString(), logPath);
            return NotFound(errorLog);
        }

        // Carpetas de destino según las normas
        var archiveFolder = Path.Combine(_historicFilePath, "Archive");
        var processedFolder = Path.Combine(_historicFilePath, "Processed");
        var errorFolder = Path.Combine(_historicFilePath, "Error");

        foreach (var file in files)
        {
            logBuilder.AppendLine($"Processing file: {file}");
            try
            {
                // Operaciones comunes: conversión, sanitización y bulk insert
                var (convertedFilePath, sanitizedFilePath) = await ProcessFileCommon(file, logBuilder);

                // Inserción de datos en la tabla final
                await D1_InsertToFinalTable(logBuilder);

                // Movimiento de archivos según normas:
                // - Archivo original a Archive
                // - Archivos convertidos y sanitizados a Processed
                D1_MoveFile(file, archiveFolder, logBuilder);
                D1_MoveFile(convertedFilePath, processedFolder, logBuilder);
                D1_MoveFile(sanitizedFilePath, processedFolder, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}");
                // Al ocurrir error, mover el archivo a la carpeta Error (norma 2)
                D1_MoveFile(file, errorFolder, logBuilder);
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed.");
        _logger.LogInformation("Process completed.");
        await D1_WriteLog(logBuilder.ToString(), logPath);

        // Mover log a Historic Logs (norma 7)
        var historicLogsFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC LOGS";
        D1_MoveLogToHistoric(logPath, historicLogsFolder);

        return Ok("Files processed successfully.");
    }

    [HttpPost]
    public async Task<IActionResult> D1_ProcessHistoricSaldosCartera()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D1_Historic_Saldos_Cartera.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process started.");
        _logger.LogInformation("Historic process started.");

        var files = Directory.GetFiles(_filePath, "SaldosCarteraXConvenio_*.csv");
        if (files.Length == 0)
        {
            // Error Código 1: Archivo No Encontrado
            var errorLog = $"{ErrorMessages.ArchivoNoEncontrado} (Código {ErrorCatalog.ArchivoNoEncontrado})";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D1_WriteLog(logBuilder.ToString(), logPath);
            return NotFound(errorLog);
        }

        // Carpetas de destino según las normas
        var archiveFolder = Path.Combine(_historicFilePath, "Archive");
        var processedFolder = Path.Combine(_historicFilePath, "Processed");
        var errorFolder = Path.Combine(_historicFilePath, "Error");

        foreach (var file in files)
        {
            logBuilder.AppendLine($"Processing file: {file}");
            try
            {
                // Validación del formato del nombre del archivo
                var fileName = Path.GetFileNameWithoutExtension(file);
                var match = Regex.Match(fileName, @"SaldosCarteraXConvenio_(\d{2})(\d{2})(\d{4})(Morning|Afternoon|Night)");
                if (!match.Success)
                {
                    var errorLog = $"Invalid file name format: {fileName}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(errorLog);
                    D1_MoveFile(file, errorFolder, logBuilder);
                    continue;
                }

                // Parseo de fecha y período
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

                // Operaciones comunes: conversión, sanitización y bulk insert
                var (convertedFilePath, sanitizedFilePath) = await ProcessFileCommon(file, logBuilder);

                // Inserción de datos históricos utilizando la fecha generada
                await D1_InsertHistoricData(fechaGenerado, logBuilder);

                // Movimiento de archivos según normas:
                // - Archivo original a Archive
                // - Archivos convertidos y sanitizados a Processed
                D1_MoveFile(file, archiveFolder, logBuilder);
                D1_MoveFile(convertedFilePath, processedFolder, logBuilder);
                D1_MoveFile(sanitizedFilePath, processedFolder, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}");
                D1_MoveFile(file, errorFolder, logBuilder);
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process completed.");
        _logger.LogInformation("Historic process completed.");
        await D1_WriteLog(logBuilder.ToString(), logPath);

        var historicLogsFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC LOGS";
        D1_MoveLogToHistoric(logPath, historicLogsFolder);

        return Ok("Historic files processed successfully.");
    }

    /// <summary>
    /// Ejecuta operaciones comunes: conversión a UTF-8 con BOM, sanitización del CSV y bulk insert a la tabla de stage.
    /// </summary>
    /// <param name="file">Ruta del archivo original.</param>
    /// <param name="logBuilder">Acumulador de logs.</param>
    /// <returns>Tuple con la ruta del archivo convertido y la ruta del archivo sanitizado.</returns>
    private async Task<(string convertedFilePath, string sanitizedFilePath)> ProcessFileCommon(string file, StringBuilder logBuilder)
    {
        string convertedFilePath, sanitizedFilePath;

        try
        {
            // Conversión a UTF-8 con BOM
            convertedFilePath = D1_ConvertToUTF8WithBOM(file);
            logBuilder.AppendLine($"Converted file to UTF-8 with BOM: {convertedFilePath}");
        }
        catch (Exception ex)
        {
            // Error Código 2: Error en Conversión de Archivo
            logBuilder.AppendLine($"{ErrorMessages.ErrorConversionArchivo} (Código {ErrorCatalog.ErrorConversionArchivo}): {ex.Message}");
            _logger.LogError(ex, $"{ErrorMessages.ErrorConversionArchivo} (Código {ErrorCatalog.ErrorConversionArchivo})");
            throw;
        }

        try
        {
            // Sanitización del CSV
            sanitizedFilePath = D1_PreprocessCsvFile(convertedFilePath, logBuilder);
            logBuilder.AppendLine($"Sanitized file: {sanitizedFilePath}");
        }
        catch (Exception ex)
        {
            // Propagar error si falla sanitización
            throw;
        }

        try
        {
            // Bulk insert a la tabla de stage
            await D1_BulkInsertToStage(sanitizedFilePath, logBuilder);
        }
        catch (Exception ex)
        {
            // Error Código 3: Error en Bulk Insert
            logBuilder.AppendLine($"{ErrorMessages.ErrorBulkInsert} (Código {ErrorCatalog.ErrorBulkInsert}): {ex.Message}");
            _logger.LogError(ex, $"{ErrorMessages.ErrorBulkInsert} (Código {ErrorCatalog.ErrorBulkInsert})");
            throw;
        }

        return (convertedFilePath, sanitizedFilePath);
    }

    private string D1_ConvertToUTF8WithBOM(string filePath)
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

    private string D1_PreprocessCsvFile(string inputFilePath, StringBuilder logBuilder)
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
                    if (!headerProcessed)
                    {
                        writer.WriteLine(line); // Escribir cabecera sin cambios
                        headerProcessed = true;
                        continue;
                    }

                    var columns = line.Split(',');
                    if (columns[0].Trim() == "0" || string.IsNullOrWhiteSpace(columns[0]))
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
            // Error en sanitización (se puede mapear al código de Bulk Insert o personalizar)
            logBuilder.AppendLine($"{ErrorMessages.ErrorConversionArchivo} (Proceso de sanitización) - {ex.Message}");
            throw;
        }

        return sanitizedFilePath;
    }

    private async Task D1_BulkInsertToStage(string csvFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D1_Stage_Saldos_Cartera;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D1_Stage_Saldos_Cartera.");

                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{csvFilePath.Replace("\\", "\\\\")}' " +
                                          "INTO TABLE D1_Stage_Saldos_Cartera " +
                                          "FIELDS TERMINATED BY ',' " +
                                          "ENCLOSED BY '\"' " +
                                          "LINES TERMINATED BY '\\n' " +
                                          "IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Bulk inserted data into D1_Stage_Saldos_Cartera.");

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    // Error Código 3: Error en Bulk Insert
                    logBuilder.AppendLine($"{ErrorMessages.ErrorBulkInsert} (Código {ErrorCatalog.ErrorBulkInsert}): {ex.Message}");
                    _logger.LogError(ex, $"{ErrorMessages.ErrorBulkInsert} (Código {ErrorCatalog.ErrorBulkInsert})");
                    throw;
                }
            }
        }
    }

    private async Task D1_InsertToFinalTable(StringBuilder logBuilder)
    {
        var sqlInsertCommand = @"
            INSERT INTO D1_Saldos_Cartera (
                Id_Solicitud, Id_Credito, Id_Persona, Referencia, Afiliado, Nombre, Monto, Comision, 
                Intereses_Totales, Monto_Total, Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, 
                IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, Comision_Pagada, 
                Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
                Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, 
                Id_Convenio, Dependencia, Primer_Pago_Teorico, Ultimo_Pago, Tipo_Financiamiento, 
                Capital_Vigente, Capital_Vencido, Intereses_Vencidos, Vencido, Sdo_Insoluto, 
                Sdo_Total_c_ListasCobro, Sdo_Vencido_c_ListCobro, Estatus_Cartera, Estatus, 
                Sucursal, Fecha_Desembolso, Frecuencia, Primer_Pago_Real, Ultimo_Pago_c_ListaCobro, 
                Ultimo_Pago_Aplicado, Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, Periodos_Atraso, 
                Pago, Monto_Ultimo_Pago, Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, 
                Com_Vigente, Com_Vencida, Clabe, Sig_Pago, Monto_Sig_Pago, vFondeador, Valida_Domi, 
                vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm, RFC, vMotiveExt, iPeriodsExt, 
                vCommentExt, nRetencion, nJoPay, iMaxDays, vMaxDate, nLiquidate, nLiqPrin, nLiqInt, 
                nLiqMor, nLiqCha, nLiqPrinTran, nLiqIntTran, nLiqMorTran, nLiqChaTran, nLiqRetTran, 
                vScoreBuro, vCollectStatus, nCAT, vOpTable, FechaGenerado
            )
            SELECT 
                Id_Solicitud, Id_Credito, Id_Persona, Referencia, Afiliado, Nombre, Monto, Comision, 
                Intereses_Totales, Monto_Total, Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, 
                IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, Comision_Pagada, 
                Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
                Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, 
                Id_Convenio, Dependencia, 
                STR_TO_DATE(NULLIF(NULLIF(Primer_Pago_Teorico, ''), '0.00'), '%d/%m/%Y') AS Primer_Pago_Teorico,
                STR_TO_DATE(NULLIF(NULLIF(Ultimo_Pago, ''), '0.00'), '%d/%m/%Y') AS Ultimo_Pago,
                Tipo_Financiamiento, Capital_Vigente, Capital_Vencido, Intereses_Vencidos, Vencido, Sdo_Insoluto, 
                Sdo_Total_c_ListasCobro, Sdo_Vencido_c_ListCobro, Estatus_Cartera, Estatus, Sucursal, 
                STR_TO_DATE(NULLIF(NULLIF(Fecha_Desembolso, ''), '0.00'), '%d/%m/%Y') AS Fecha_Desembolso,
                Frecuencia, 
                STR_TO_DATE(NULLIF(NULLIF(Primer_Pago_Real, ''), '0.00'), '%d/%m/%Y') AS Primer_Pago_Real,
                STR_TO_DATE(NULLIF(NULLIF(Ultimo_Pago_c_ListaCobro, ''), '0.00'), '%d/%m/%Y') AS Ultimo_Pago_c_ListaCobro,
                STR_TO_DATE(NULLIF(NULLIF(Ultimo_Pago_Aplicado, ''), '0.00'), '%d/%m/%Y') AS Ultimo_Pago_Aplicado,
                Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, Periodos_Atraso, Pago, Monto_Ultimo_Pago, 
                Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, Com_Vigente, Com_Vencida, Clabe, 
                STR_TO_DATE(NULLIF(NULLIF(Sig_Pago, ''), '0.00'), '%d/%m/%Y') AS Sig_Pago, Monto_Sig_Pago, vFondeador, Valida_Domi, 
                vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm, RFC, vMotiveExt, iPeriodsExt, vCommentExt, 
                nRetencion, nJoPay, iMaxDays, STR_TO_DATE(NULLIF(NULLIF(vMaxDate, ''), '0.00'), '%d/%m/%Y') AS vMaxDate, 
                nLiquidate, nLiqPrin, nLiqInt, nLiqMor, nLiqCha, nLiqPrinTran, nLiqIntTran, nLiqMorTran, 
                nLiqChaTran, nLiqRetTran, vScoreBuro, vCollectStatus, nCAT, vOpTable, NOW() AS FechaGenerado
            FROM D1_Stage_Saldos_Cartera;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    var rowsAffected = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Inserted {rowsAffected} rows into D1_Saldos_Cartera.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    // Error Código 4: Error en Inserción en la Tabla Final
                    logBuilder.AppendLine($"{ErrorMessages.ErrorInsercionTablaFinal} (Código {ErrorCatalog.ErrorInsercionTablaFinal}): {ex.Message}");
                    _logger.LogError(ex, $"{ErrorMessages.ErrorInsercionTablaFinal} (Código {ErrorCatalog.ErrorInsercionTablaFinal})");
                    throw;
                }
            }
        }
    }

    private async Task D1_InsertHistoricData(DateTime fechaGenerado, StringBuilder logBuilder)
    {
        var sqlInsertCommand = @"
        INSERT INTO D1_Saldos_Cartera (
            Id_Solicitud, Id_Credito, Id_Persona, Referencia, Afiliado, Nombre, Monto, Comision, 
            Intereses_Totales, Monto_Total, Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, 
            IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, Comision_Pagada, 
            Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
            Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, 
            Id_Convenio, Dependencia, Primer_Pago_Teorico, Ultimo_Pago, Tipo_Financiamiento, 
            Capital_Vigente, Capital_Vencido, Intereses_Vencidos, Vencido, Sdo_Insoluto, 
            Sdo_Total_c_ListasCobro, Sdo_Vencido_c_ListCobro, Estatus_Cartera, Estatus, 
            Sucursal, Fecha_Desembolso, Frecuencia, Primer_Pago_Real, Ultimo_Pago_c_ListaCobro, 
            Ultimo_Pago_Aplicado, Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, Periodos_Atraso, 
            Pago, Monto_Ultimo_Pago, Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, 
            Com_Vigente, Com_Vencida, Clabe, Sig_Pago, Monto_Sig_Pago, vFondeador, Valida_Domi, 
            vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm, RFC, vMotiveExt, iPeriodsExt, 
            vCommentExt, nRetencion, nJoPay, iMaxDays, vMaxDate, nLiquidate, nLiqPrin, nLiqInt, 
            nLiqMor, nLiqCha, nLiqPrinTran, nLiqIntTran, nLiqMorTran, nLiqChaTran, nLiqRetTran, 
            vScoreBuro, vCollectStatus, nCAT, vOpTable, FechaGenerado
        )
        SELECT 
            Id_Solicitud, Id_Credito, Id_Persona, Referencia, Afiliado, Nombre, Monto, Comision, 
            Intereses_Totales, Monto_Total, Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, 
            IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, Comision_Pagada, 
            Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
            Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, 
            Id_Convenio, Dependencia, 
            CASE 
                WHEN Primer_Pago_Teorico REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN Primer_Pago_Teorico
                ELSE STR_TO_DATE(NULLIF(NULLIF(Primer_Pago_Teorico, ''), '0.00'), '%d/%m/%Y')
            END AS Primer_Pago_Teorico,
            CASE 
                WHEN Ultimo_Pago REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN Ultimo_Pago
                ELSE STR_TO_DATE(NULLIF(NULLIF(Ultimo_Pago, ''), '0.00'), '%d/%m/%Y')
            END AS Ultimo_Pago,
            Tipo_Financiamiento, Capital_Vigente, Capital_Vencido, Intereses_Vencidos, Vencido, Sdo_Insoluto, 
            Sdo_Total_c_ListasCobro, Sdo_Vencido_c_ListCobro, Estatus_Cartera, Estatus, Sucursal, 
            CASE 
                WHEN Fecha_Desembolso REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN Fecha_Desembolso
                ELSE STR_TO_DATE(NULLIF(NULLIF(Fecha_Desembolso, ''), '0.00'), '%d/%m/%Y')
            END AS Fecha_Desembolso,
            Frecuencia, 
            CASE 
                WHEN Primer_Pago_Real REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN Primer_Pago_Real
                ELSE STR_TO_DATE(NULLIF(NULLIF(Primer_Pago_Real, ''), '0.00'), '%d/%m/%Y')
            END AS Primer_Pago_Real,
            CASE 
                WHEN Ultimo_Pago_c_ListaCobro REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN Ultimo_Pago_c_ListaCobro
                ELSE STR_TO_DATE(NULLIF(NULLIF(Ultimo_Pago_c_ListaCobro, ''), '0.00'), '%d/%m/%Y')
            END AS Ultimo_Pago_c_ListaCobro,
            CASE 
                WHEN Ultimo_Pago_Aplicado REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN Ultimo_Pago_Aplicado
                ELSE STR_TO_DATE(NULLIF(NULLIF(Ultimo_Pago_Aplicado, ''), '0.00'), '%d/%m/%Y')
            END AS Ultimo_Pago_Aplicado,
            Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, Periodos_Atraso, Pago, Monto_Ultimo_Pago, 
            Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, Com_Vigente, Com_Vencida, Clabe, 
            CASE 
                WHEN Sig_Pago REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN Sig_Pago
                ELSE STR_TO_DATE(NULLIF(NULLIF(Sig_Pago, ''), '0.00'), '%d/%m/%Y')
            END AS Sig_Pago, 
            Monto_Sig_Pago, vFondeador, Valida_Domi, 
            vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm, RFC, vMotiveExt, iPeriodsExt, vCommentExt, 
            nRetencion, nJoPay, iMaxDays, vMaxDate, nLiquidate, nLiqPrin, nLiqInt, nLiqMor, nLiqCha, 
            nLiqPrinTran, nLiqIntTran, nLiqMorTran, nLiqChaTran, nLiqRetTran, vScoreBuro, vCollectStatus, 
            nCAT, vOpTable, @FechaGenerado
        FROM D1_Stage_Saldos_Cartera;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    command.Parameters.AddWithValue("@FechaGenerado", fechaGenerado);
                    var rowsAffected = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Inserted {rowsAffected} rows into D1_Saldos_Cartera.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    // Error Código 4: Error en Inserción en la Tabla Final
                    logBuilder.AppendLine($"{ErrorMessages.ErrorInsercionTablaFinal} (Código {ErrorCatalog.ErrorInsercionTablaFinal}): {ex.Message}");
                    _logger.LogError(ex, $"{ErrorMessages.ErrorInsercionTablaFinal} (Código {ErrorCatalog.ErrorInsercionTablaFinal})");
                    throw;
                }
            }
        }
    }

    private void D1_MoveFile(string sourceFilePath, string destinationFolder, StringBuilder logBuilder)
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
            logBuilder.AppendLine($"Moved file: {sourceFilePath} -> {destinationFilePath}");
            _logger.LogInformation($"Moved file: {sourceFilePath} -> {destinationFilePath}");
        }
        catch (Exception ex)
        {
            // Error Código 5: Error al Mover el Archivo
            logBuilder.AppendLine($"{ErrorMessages.ErrorMoverArchivo} (Código {ErrorCatalog.ErrorMoverArchivo}) for file {sourceFilePath}: {ex.Message}");
            _logger.LogError(ex, $"{ErrorMessages.ErrorMoverArchivo} (Código {ErrorCatalog.ErrorMoverArchivo}) for file {sourceFilePath}");
        }
    }

    private async Task D1_WriteLog(string content, string logPath)
    {
        try
        {
            await System.IO.File.WriteAllTextAsync(logPath, content);
            _logger.LogInformation($"Log written to: {logPath}");
        }
        catch (Exception ex)
        {
            // Error Código 7: Error en la Generación del Log
            _logger.LogError(ex, $"{ErrorMessages.ErrorGeneracionLog} (Código {ErrorCatalog.ErrorGeneracionLog}) while writing log to {logPath}");
        }
    }

    private void D1_MoveLogToHistoric(string logPath, string historicLogsFolder)
    {
        try
        {
            if (!Directory.Exists(historicLogsFolder))
            {
                Directory.CreateDirectory(historicLogsFolder);
            }

            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var logFileName = Path.GetFileNameWithoutExtension(logPath) + $"_{timestamp}" + Path.GetExtension(logPath);
            var destinationFilePath = Path.Combine(historicLogsFolder, logFileName);

            System.IO.File.Move(logPath, destinationFilePath);
            _logger.LogInformation($"Moved log file to historic folder: {destinationFilePath}");
        }
        catch (Exception ex)
        {
            // Error Código 8: Error al Mover el Log
            _logger.LogError(ex, $"{ErrorMessages.ErrorMoverLog} (Código {ErrorCatalog.ErrorMoverLog}): {ex.Message}");
        }
    }
}
