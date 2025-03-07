using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

public class D1_Saldos_Cartera_Controller : Controller
{
    private readonly ILogger<D1_Saldos_Cartera_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    // Directorios base para archivos y logs
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilesFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";
    private readonly string _logsFolder = @"C:\Users\Go Credit\Documents\DATA\LOGS";
    private readonly string _historicLogsFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC LOGS";

    // Carpetas de movimiento para archivos dentro de Historic Files
    private readonly string _archiveFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Archive";
    private readonly string _processedFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Processed";
    private readonly string _errorFilesFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Error";

    // Nombre dinámico del log basado en el controlador
    private readonly string _logFileName;
    
    public D1_Saldos_Cartera_Controller(ILogger<D1_Saldos_Cartera_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        _logFileName = $"{nameof(D1_Saldos_Cartera_Controller)}.log";

        // Si existe un log con el mismo nombre, lo movemos a la carpeta histórica de logs
        var logPath = Path.Combine(_logsFolder, _logFileName);
        if (System.IO.File.Exists(logPath))
        {
            MoveExistingLog(logPath, _historicLogsFolder);
        }
    }

    [HttpPost]
    public async Task<IActionResult> D1_ProcessSaldosCartera()
    {
        var logPath = Path.Combine(_logsFolder, _logFileName);
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.");
        _logger.LogInformation("Process started.");

        var files = Directory.GetFiles(_filePath, "SaldosCarteraXConvenio*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No files matching 'SaldosCarteraXConvenio*.csv' found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D1_WriteLog(logBuilder.ToString(), logPath);
            return NotFound(errorLog);
        }

        foreach (var file in files)
        {
            string? convertedFilePath = null;
            string? sanitizedFilePath = null;
            logBuilder.AppendLine($"Processing file: {file}");

            try
            {
                convertedFilePath = D1_ConvertToUTF8WithBOM(file);
                logBuilder.AppendLine($"Converted file to UTF-8 with BOM: {convertedFilePath}");

                sanitizedFilePath = D1_PreprocessCsvFile(convertedFilePath, logBuilder);
                logBuilder.AppendLine($"Sanitized file: {sanitizedFilePath}");

                await D1_BulkInsertToStage(sanitizedFilePath, logBuilder);
                await D1_InsertToFinalTable(logBuilder);

                // Movimiento de archivos en caso de éxito
                D1_MoveFile(file, _archiveFolder, logBuilder);
                if (convertedFilePath != null)
                    D1_MoveFile(convertedFilePath, _processedFolder, logBuilder);
                if (sanitizedFilePath != null)
                    D1_MoveFile(sanitizedFilePath, _processedFolder, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}");

                // Movimiento de todos los archivos relacionados a la carpeta de error
                D1_MoveFile(file, _errorFilesFolder, logBuilder);
                if (convertedFilePath != null && System.IO.File.Exists(convertedFilePath))
                    D1_MoveFile(convertedFilePath, _errorFilesFolder, logBuilder);
                if (sanitizedFilePath != null && System.IO.File.Exists(sanitizedFilePath))
                    D1_MoveFile(sanitizedFilePath, _errorFilesFolder, logBuilder);

                await D1_WriteLog(logBuilder.ToString(), logPath);
                // Relanzar la excepción para detener el proceso
                throw;
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed.");
        _logger.LogInformation("Process completed.");
        await D1_WriteLog(logBuilder.ToString(), logPath);

        // Movimiento del log finalizado a la carpeta histórica de logs
        D1_MoveLogToHistoric(logPath, _historicLogsFolder);

        return Ok("Files processed successfully.");
    }

    [HttpPost]
    public async Task<IActionResult> D1_ProcessHistoricSaldosCartera()
    {
        var logPath = Path.Combine(_logsFolder, _logFileName);
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process started.");
        _logger.LogInformation("Historic process started.");

        var files = Directory.GetFiles(_filePath, "SaldosCarteraXConvenio_*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No historic files matching 'SaldosCarteraXConvenio_*.csv' found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D1_WriteLog(logBuilder.ToString(), logPath);
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
                var match = Regex.Match(fileName, @"SaldosCarteraXConvenio_(\d{2})(\d{2})(\d{4})(Morning|Afternoon|Night)");

                if (!match.Success)
                {
                    var errorLog = $"Invalid file name format: {fileName}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(errorLog);
                    D1_MoveFile(file, _errorFilesFolder, logBuilder);
                    continue;
                }

                // Parseo de la fecha y determinación del período
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

                convertedFilePath = D1_ConvertToUTF8WithBOM(file);
                logBuilder.AppendLine($"Converted file to UTF-8 with BOM: {convertedFilePath}");

                sanitizedFilePath = D1_PreprocessCsvFile(convertedFilePath, logBuilder);
                logBuilder.AppendLine($"Sanitized file: {sanitizedFilePath}");

                await D1_BulkInsertToStage(sanitizedFilePath, logBuilder);
                await D1_InsertHistoricData(fechaGenerado, logBuilder);

                // Movimiento de archivos en caso de éxito
                D1_MoveFile(file, _archiveFolder, logBuilder);
                if (convertedFilePath != null)
                    D1_MoveFile(convertedFilePath, _processedFolder, logBuilder);
                if (sanitizedFilePath != null)
                    D1_MoveFile(sanitizedFilePath, _processedFolder, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}");

                // Movimiento de todos los archivos relacionados a la carpeta de error
                D1_MoveFile(file, _errorFilesFolder, logBuilder);
                if (convertedFilePath != null && System.IO.File.Exists(convertedFilePath))
                    D1_MoveFile(convertedFilePath, _errorFilesFolder, logBuilder);
                if (sanitizedFilePath != null && System.IO.File.Exists(sanitizedFilePath))
                    D1_MoveFile(sanitizedFilePath, _errorFilesFolder, logBuilder);

                await D1_WriteLog(logBuilder.ToString(), logPath);
                throw;
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process completed.");
        _logger.LogInformation("Historic process completed.");
        await D1_WriteLog(logBuilder.ToString(), logPath);

        // Movimiento del log finalizado a la carpeta histórica de logs
        D1_MoveLogToHistoric(logPath, _historicLogsFolder);

        return Ok("Historic files processed successfully.");
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
                    logBuilder.AppendLine($"Error during bulk insert: {ex.Message}");
                    _logger.LogError(ex, "Error during bulk insert.");
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
                    logBuilder.AppendLine($"Error during insert: {ex.Message}");
                    _logger.LogError(ex, "Error during insert.");
                    throw;
                }
            }
        }
    }

    private async Task D1_InsertHistoricData(DateTime fechaGenerado, StringBuilder logBuilder)
    {
        // Lista de columnas de fecha en la tabla stage que se insertarán en la tabla final:
        // Primer_Pago_Teorico, Ultimo_Pago, Fecha_Desembolso, Primer_Pago_Real, 
        // Ultimo_Pago_c_ListaCobro, Ultimo_Pago_Aplicado, Sig_Pago, vMaxDate

        var sqlPurgeCommand = @"
            UPDATE D1_Stage_Saldos_Cartera
            SET 
                Primer_Pago_Teorico = NULLIF(Primer_Pago_Teorico, ''),
                Ultimo_Pago = NULLIF(Ultimo_Pago, ''),
                Fecha_Desembolso = NULLIF(Fecha_Desembolso, ''),
                Primer_Pago_Real = NULLIF(Primer_Pago_Real, ''),
                Ultimo_Pago_c_ListaCobro = NULLIF(Ultimo_Pago_c_ListaCobro, ''),
                Ultimo_Pago_Aplicado = NULLIF(Ultimo_Pago_Aplicado, ''),
                Sig_Pago = NULLIF(Sig_Pago, ''),
                vMaxDate = NULLIF(vMaxDate, '')
        ;
        ";

        var sqlInsertCommand = @"
            INSERT INTO D1_Saldos_Cartera (
                Id_Solicitud, Id_Credito, Id_Persona, Referencia, Afiliado, Nombre, Monto, Comision, 
                Intereses_Totales, Monto_Total, Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, 
                IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, Comision_Pagada, 
                Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
                Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, 
                Id_Convenio, Dependencia, 
                Primer_Pago_Teorico,
                Ultimo_Pago,
                Tipo_Financiamiento, Capital_Vigente, Capital_Vencido, Intereses_Vencidos, Vencido, Sdo_Insoluto, 
                Sdo_Total_c_ListasCobro, Sdo_Vencido_c_ListCobro, Estatus_Cartera, Estatus, Sucursal, 
                Fecha_Desembolso,
                Frecuencia, 
                Primer_Pago_Real,
                Ultimo_Pago_c_ListaCobro,
                Ultimo_Pago_Aplicado,
                Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, Periodos_Atraso, Pago, Monto_Ultimo_Pago, 
                Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, Com_Vigente, Com_Vencida, Clabe, 
                Sig_Pago,
                Monto_Sig_Pago, vFondeador, Valida_Domi, vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm, 
                RFC, vMotiveExt, iPeriodsExt, vCommentExt, nRetencion, nJoPay, iMaxDays, 
                vMaxDate,
                nLiquidate, nLiqPrin, nLiqInt, nLiqMor, nLiqCha, nLiqPrinTran, nLiqIntTran, 
                nLiqMorTran, nLiqChaTran, nLiqRetTran, vScoreBuro, vCollectStatus, nCAT, vOpTable, FechaGenerado
            )
            SELECT 
                Id_Solicitud, Id_Credito, Id_Persona, Referencia, Afiliado, Nombre, Monto, Comision, 
                Intereses_Totales, Monto_Total, Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, 
                IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, Comision_Pagada, 
                Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
                Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, 
                Id_Convenio, Dependencia, 
                Primer_Pago_Teorico, Ultimo_Pago, Tipo_Financiamiento, Capital_Vigente, Capital_Vencido, 
                Intereses_Vencidos, Vencido, Sdo_Insoluto, Sdo_Total_c_ListasCobro, Sdo_Vencido_c_ListCobro, 
                Estatus_Cartera, Estatus, Sucursal, Fecha_Desembolso, Frecuencia, Primer_Pago_Real, 
                Ultimo_Pago_c_ListaCobro, Ultimo_Pago_Aplicado, Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, 
                Periodos_Atraso, Pago, Monto_Ultimo_Pago, Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, 
                Com_Vigente, Com_Vencida, Clabe, 
                Sig_Pago, 
                Monto_Sig_Pago, vFondeador, Valida_Domi, vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm, 
                RFC, vMotiveExt, iPeriodsExt, vCommentExt, nRetencion, nJoPay, iMaxDays, 
                vMaxDate, nLiquidate, nLiqPrin, nLiqInt, nLiqMor, nLiqCha, nLiqPrinTran, nLiqIntTran, 
                nLiqMorTran, nLiqChaTran, nLiqRetTran, vScoreBuro, vCollectStatus, nCAT, vOpTable, @FechaGenerado
            FROM D1_Stage_Saldos_Cartera;
        ";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    // Primero purgar los campos de fecha vacíos en la tabla stage.
                    var purgeCommand = new MySqlCommand(sqlPurgeCommand, connection, transaction);
                    int purgeRows = await purgeCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Purge stage table: {purgeRows} rows updated.");

                    // Luego, realizar el INSERT desde la tabla stage a la tabla final.
                    var insertCommand = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    insertCommand.Parameters.AddWithValue("@FechaGenerado", fechaGenerado);
                    int rowsAffected = await insertCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Inserted {rowsAffected} rows into D1_Saldos_Cartera.");
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
                        writer.WriteLine(line); // Escribir el encabezado tal como está
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
            logBuilder.AppendLine($"Error during file sanitization: {ex.Message}");
            throw;
        }

        return sanitizedFilePath;
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
            logBuilder.AppendLine($"Error moving file {sourceFilePath} to {destinationFolder}: {ex.Message}");
            _logger.LogError(ex, $"Error moving file {sourceFilePath} to {destinationFolder}");
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
            _logger.LogError(ex, $"Error writing log to {logPath}");
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
            _logger.LogError(ex, $"Error moving log file to historic folder: {ex.Message}");
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
