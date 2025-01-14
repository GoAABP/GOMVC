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
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

    public D1_Saldos_Cartera_Controller(ILogger<D1_Saldos_Cartera_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
    }

    public async Task D1_ProcessSaldosCartera()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadSaldosCartera.log";
        var logBuilder = new StringBuilder();
        var todayDate = DateTime.Now.ToString("yyyy-MM-dd");
        var startLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.";
        logBuilder.AppendLine(startLog);
        _logger.LogInformation(startLog);
        
        var files = Directory.GetFiles(_filePath, "SaldosCarteraXConvenio*.csv");
        if (files.Length == 0)
        {
            var errorLog = "File not found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D1_WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }
        var file = files[0];
        logBuilder.AppendLine("File found.");
        try
        {
            var textFilePath = await D1_ConvertCsvToText(file, logBuilder);
            await D1_BulkInsertSaldosCarteraData(textFilePath, logBuilder);
            await D1_ExecuteSaldosCarteraInsert(logBuilder, logPath);
            D1_MoveFilesToHistoric(file, textFilePath, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await D1_WriteLog(logBuilder.ToString(), logPath);
            throw;
        }
        var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.";
        logBuilder.AppendLine(endLog);
        _logger.LogInformation(endLog);
        await D1_WriteLog(logBuilder.ToString(), logPath);
    }

    private async Task<string> D1_ConvertCsvToText(string csvFilePath, StringBuilder logBuilder)
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        var textFilePath = Path.ChangeExtension(csvFilePath, ".txt");
        var sb = new StringBuilder();

        try
        {
            using (var reader = new StreamReader(csvFilePath, Encoding.GetEncoding("windows-1252")))
            {
                string line;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    var processedLine = line.Normalize(NormalizationForm.FormC);
                    processedLine = Regex.Replace(processedLine, ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", "\n");
                    processedLine = Regex.Replace(processedLine, @"(\d{2})/(\d{2})/(\d{4})", "$3-$2-$ 1");

                    sb.AppendLine(processedLine);
                }
            }

            using (var writer = new StreamWriter(textFilePath, false, Encoding.UTF8))
            {
                await writer.WriteAsync(sb.ToString());
            }

            var logMessage = $"Converted CSV to text for SaldosCartera: {textFilePath}";
            logBuilder.AppendLine(logMessage);
            _logger.LogInformation(logMessage);
        }
        catch (Exception ex)
        {
            var errorLog = $"Error during conversion: {ex.Message}";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(ex, errorLog);
            throw;
        }

        return textFilePath;
    }

    private async Task D1_BulkInsertSaldosCarteraData(string textFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE Stage_Saldos_Cartera;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    var logMessage = "Truncated table Stage_Saldos_Cartera.";
                    logBuilder.AppendLine(logMessage);
                    _logger.LogInformation(logMessage);
                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{textFilePath.Replace("\\", "\\\\")}' " +
                                        "INTO TABLE Stage_Saldos_Cartera " +
                                        "FIELDS TERMINATED BY '|' " +
                                        "ENCLOSED BY '\"' " +
                                        "LINES TERMINATED BY '\\n' " +
                                        "IGNORE 1 LINES;"; // Ensure to ignore the header line
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logMessage = "Bulk inserted data into Stage_Saldos_Cartera.";
                    logBuilder.AppendLine(logMessage);
                    _logger.LogInformation(logMessage);
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error during bulk insert for SaldosCartera: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    throw;
                }
            }
        }
    }

    private async Task D1_ExecuteSaldosCarteraInsert(StringBuilder logBuilder, string logPath)
    {
        // Define the SQL insert command directly in the method
        var sqlInsertCommand = @"
        INSERT INTO D1_Saldos_Cartera (
            Id_Solicitud, Id_Credito, Id_Persona, Referencia, Afiliado, Nombre, Monto, Comision, Intereses_Totales, Monto_Total, 
            Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, 
            Comision_Pagada, Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
            Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, Id_Convenio, Dependencia, 
            Primer_Pago_Teorico, Ultimo_Pago, Tipo_Financiamiento, Capital_Vigente, Capital_Vencido, Intereses_Vencidos, Vencido, 
            Sdo_Insoluto, Sdo_Total_c_ListasCobro, Sdo_Vencido_c_ListCobro, Estatus_Cartera, Estatus, Sucursal, Fecha_Desembolso, 
            Frecuencia, Primer_Pago_Real, Ultimo_Pago_c_ListaCobro, Ultimo_Pago_Aplicado, Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, 
            Periodos_Atraso, Pago, Monto_Ultimo_Pago, Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, Com_Vigente, Com_Vencida, 
            Clabe, Sig_Pago, Monto_Sig_Pago, vFondeador, Valida_Domi, vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm, RFC, vMotiveExt, 
            iPeriodsExt, vCommentExt, nRetencion, nJoPay, iMaxDays, vMaxDate, nLiquidate, nLiqPrin, nLiqInt, nLiqMor, nLiqCha, 
            nLiqPrinTran, nLiqIntTran, nLiqMorTran, nLiqChaTran, nLiqRetTran, vScoreBuro, vCollectStatus, nCAT, vOpTable, FechaGenerado
        )
        SELECT 
            Id_Solicitud, Id_Credito, Id_Persona, Referencia, Afiliado, Nombre, Monto, Comision, Intereses_Totales, Monto_Total, 
            Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, 
            Comision_Pagada, Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
            Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, Id_Convenio, Dependencia, 
            CASE 
                WHEN Primer_Pago_Teorico = '0000-00-00' OR Primer_Pago_Teorico = '' OR Primer_Pago_Teorico = '0.00' THEN NULL 
                ELSE STR_TO_DATE(Primer_Pago_Teorico, '%Y-%m-%d') 
            END AS ```csharp
    Primer_Pago_Teorico, 
            CASE 
                WHEN Ultimo_Pago = '0000-00-00' OR Ultimo_Pago = '' OR Ultimo_Pago = '0.00' THEN NULL 
                ELSE STR_TO_DATE(Ultimo_Pago, '%Y-%m-%d') 
            END AS Ultimo_Pago, 
            Tipo_Financiamiento, Capital_Vigente, Capital_Vencido, Intereses_Vencidos, Vencido, 
            Sdo_Insoluto, Sdo_Total_c_ListasCobro, Sdo_Vencido_c_ListCobro, Estatus_Cartera, Estatus, Sucursal, 
            CASE 
                WHEN Fecha_Desembolso = '0000-00-00' OR Fecha_Desembolso = '' THEN NULL 
                ELSE STR_TO_DATE(Fecha_Desembolso, '%Y-%m-%d') 
            END AS Fecha_Desembolso, 
            Frecuencia, 
            CASE 
                WHEN Primer_Pago_Real = '0000-00-00' OR Primer_Pago_Real = '' THEN NULL 
                ELSE STR_TO_DATE(Primer_Pago_Real, '%Y-%m-%d') 
            END AS Primer_Pago_Real, 
            CASE 
                WHEN Ultimo_Pago_c_ListaCobro = '0000-00-00' OR Ultimo_Pago_c_ListaCobro = '' THEN NULL 
                ELSE STR_TO_DATE(Ultimo_Pago_c_ListaCobro, '%Y-%m-%d') 
            END AS Ultimo_Pago_c_ListaCobro, 
            CASE 
                WHEN Ultimo_Pago_Aplicado = '0000-00-00' OR Ultimo_Pago_Aplicado = '' THEN NULL 
                ELSE STR_TO_DATE(Ultimo_Pago_Aplicado, '%Y-%m-%d') 
            END AS Ultimo_Pago_Aplicado, 
            Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, Periodos_Atraso, Pago, Monto_Ultimo_Pago, 
            Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, Com_Vigente, Com_Vencida, 
            Clabe, 
            CASE 
                WHEN Sig_Pago = '0000-00-00' OR Sig_Pago = '' THEN NULL 
                ELSE STR_TO_DATE(Sig_Pago, '%Y-%m-%d') 
            END AS Sig_Pago, 
            Monto_Sig_Pago, vFondeador, Valida_Domi, vAfiliateIdO, vAfiliateO, 
            Saldo_Retencion_Adm, RFC, vMotiveExt, iPeriodsExt, vCommentExt, nRetencion, nJoPay, 
            iMaxDays, 
            CASE 
                WHEN vMaxDate = '0000-00-00' OR vMaxDate = '' THEN NULL 
                ELSE STR_TO_DATE(vMaxDate, '%Y-%m-%d') 
            END AS vMaxDate, 
            nLiquidate, nLiqPrin, nLiqInt, nLiqMor, nLiqCha, 
            nLiqPrinTran, nLiqIntTran, nLiqMorTran, nLiqChaTran, nLiqRetTran, 
            vScoreBuro, vCollectStatus, nCAT, vOpTable, NOW() AS FechaGenerado
        FROM D1_Stage_Saldos_Cartera
        WHERE NOT EXISTS (
            SELECT 1 FROM D1_Saldos_Cartera WHERE D1_Saldos_Cartera.Id_Credito = D1_Stage_Saldos_Cartera.Id_Credito
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
                    var logMessageSuccess = "Executed SQL insert successfully.";
                    logBuilder.AppendLine(logMessageSuccess);
                    _logger.LogInformation(logMessageSuccess);
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error executing SQL insert: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    await D1_WriteLog(logBuilder.ToString(), logPath);
                    throw;
                }
            }
        }
    }

    private string GetCurrentPeriod()
    {
        var now = DateTime.Now.TimeOfDay;
        if (now >= TimeSpan.Parse("00:00:00") && now <= TimeSpan.Parse("07:00:00"))
        {
            return "07:00:00";
        }
        else if (now >= TimeSpan.Parse("07:01:00") && now <= TimeSpan.Parse("18:00:00"))
        {
            return "18:00:00";
        }
        else
        {
            return "23:59:59";
        }
    }

    private void D1_MoveFilesToHistoric(string originalFilePath, string textFilePath, StringBuilder logBuilder)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");

        // Move original file
        var originalFileName = Path.GetFileNameWithoutExtension(originalFilePath);
        var originalExtension = Path.GetExtension(originalFilePath);
        var newOriginalFileName = $"{originalFileName}_{timestamp}{originalExtension}";
        var newOriginalFilePath = Path.Combine(_historicFilePath, newOriginalFileName);

        System.IO.File.Move(originalFilePath, newOriginalFilePath);
        var logMessage = $"Moved original file to historic folder: {newOriginalFilePath}";
        logBuilder.AppendLine(logMessage);
        _logger.LogInformation(logMessage);

        // Move converted file
        var textFileName = Path.GetFileNameWithoutExtension(textFilePath);
        var textExtension = Path.GetExtension(textFilePath);
        var newTextFileName = $"{textFileName}_{timestamp}{textExtension}";
        var newTextFilePath = Path.Combine(_historicFilePath, newTextFileName);

        System.IO.File.Move(textFilePath, newTextFilePath);
        logMessage = $"Moved converted file to historic folder: {newTextFilePath}";
        logBuilder.AppendLine(logMessage);
        _logger.LogInformation(logMessage);
    }

    private async Task D1_WriteLog(string logContent, string logPath)
    {
        var historicLogPath = Path.Combine(_historicFilePath, $"BulkLoad_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.log");
        if (System.IO.File.Exists(logPath))
        {
            // Move the existing log file to the historic folder
            System.IO.File.Move(logPath, historicLogPath);
        }
        // Write the new log content
        await System.IO.File.WriteAllTextAsync(logPath, logContent);
    }
}