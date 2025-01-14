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
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

    public D2_Saldos_Contables_Controller(ILogger<D2_Saldos_Contables_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
    }

    public async Task D2_ProcessSaldosContables()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadSaldosContables.log";
        var logBuilder = new StringBuilder();
        var todayDate = DateTime.Now.ToString("yyyy-MM-dd");
        var startLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.";
        logBuilder.AppendLine(startLog);
        _logger.LogInformation(startLog);

        // Finding the CSV file
        var files = Directory.GetFiles(_filePath, "SDOSCONT*.CSV");
        if (files.Length == 0)
        {
            var errorLog = "File not found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D2_WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }
        var file = files[0]; // First file found
        logBuilder.AppendLine("File found.");
        try
        {
            // Convert CSV to text
            var textFilePath = await D2_ConvertCsvToText(file, logBuilder);
            await D2_BulkInsertSaldosContablesData(textFilePath, logBuilder);
            await D2_ExecuteSaldosContablesInsert(logBuilder, logPath);
            D2_MoveFilesToHistoric(file, textFilePath, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await D2_WriteLog(logBuilder.ToString(), logPath);
            throw;
        }
        var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.";
        logBuilder.AppendLine(endLog);
        _logger.LogInformation(endLog);
        await D2_WriteLog(logBuilder.ToString(), logPath);
    }

    private async Task<string> D2_ConvertCsvToText(string csvFilePath, StringBuilder logBuilder)
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
                    processedLine = Regex.Replace(processedLine, @"(\d{2})/(\d{2})/(\d{4})", "$3-$2-$1");

                    sb.AppendLine(processedLine);
                }
            }

            await System.IO.File.WriteAllTextAsync(textFilePath, sb.ToString(), Encoding.UTF8);
            var logMessage = $"Converted CSV to text for SaldosContables: {textFilePath}";
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

    private async Task D2_BulkInsertSaldosContablesData(string textFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE Stage_Saldos_Contables;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    var logMessage = "Truncated table Stage_Saldos_Contables.";
                    logBuilder.AppendLine(logMessage);
                    _logger.LogInformation(logMessage);

                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{textFilePath.Replace("\\", "\\\\")}' " +
                                          "INTO TABLE Stage_Saldos_Contables " +
                                          "FIELDS TERMINATED BY '|' " +
                                          "ENCLOSED BY '\"' " +
                                          "LINES TERMINATED BY '\\n' " +
                                          "IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logMessage = "Bulk inserted data into Stage_Saldos_Contables.";
                    logBuilder.AppendLine(logMessage);
                    _logger.LogInformation(logMessage);

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error during bulk insert for SaldosContables: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    throw;
                }
            }
        }
    }

    private async Task D2_ExecuteSaldosContablesInsert(StringBuilder logBuilder, string logPath)
    {
        // Directly defining the SQL query for the insert operation
        var sqlInsertCommand = @"
        INSERT INTO Saldos_Contables (
            Id_Credito, Referencia, Nombre, Id_Sucursal, Sucursal, Id_Convenio, Convenio, Financiamiento,
            Estatus_Inicial, Estatus_Final, Fecha_Apertura, Fecha_Terminacion, Importe, Dias_Atraso,
            Cuotas_Atrasadas, Periodos_Atraso, Pagos_Sostenidos, Pago, Frecuencia, Fecha_Ultimo_Pago,
            Importe_Ultimo_Pago, Saldo_Inicial_Capital, Otorgado, Pagos, Ajuste_Cargo_Capital, Ajuste_Abono_Capital,
            Saldo_Final_Capital, Calculo, Diferencia, Capital_Vigente, Capital_Vencido, Saldo_Inicial_Interes,
            Devengamiento, Pagos_Interes, Ajuste_Cargo_Interes, Ajuste_Abono_Interes, Interes_No_Devengado,
            Saldo_Final_Interes, Calculo_Interes, Diferencia_Interes, Interes_Devengado, IVA_Interes_Devengado,
            Interes_No_DevengadoB, Fecha_Cartera_Vencida, Saldo_Contable, Saldo_Insoluto, Porc_Provision,
            Reserva, nCAT, vOpTable, Status, FechaGenerado
        )
        SELECT
            Id_Credito, Referencia, Nombre, Id_Sucursal, Sucursal, Id_Convenio, Convenio, Financiamiento,
            Estatus_Inicial, Estatus_Final,
            CASE 
                WHEN Fecha_Apertura = '0000-00-00' OR Fecha_Apertura = '' OR Fecha_Apertura = '0.00' THEN NULL 
                ELSE STR_TO_DATE(Fecha_Apertura, '%Y-%m-%d') 
            END AS Fecha_Apertura,
            CASE 
                WHEN Fecha_Terminacion = '0000-00-00' OR Fecha_Terminacion = '' OR Fecha_Terminacion = '0.00' THEN NULL 
                ELSE STR_TO_DATE(Fecha_Terminacion, '%Y-%m-%d') 
            END AS Fecha_Terminacion,
            Importe, Dias_Atraso, Cuotas_Atrasadas, Periodos_Atraso, Pagos_Sostenidos, Pago, Frecuencia,
            CASE 
                WHEN Fecha_Ultimo_Pago = '0000-00-00' OR Fecha_Ultimo_Pago = '' OR Fecha_Ultimo_Pago = '0.00' THEN NULL 
                ELSE STR_TO_DATE(Fecha_Ultimo_Pago, '%Y-%m-%d') 
            END AS Fecha_Ultimo_Pago,
            Importe_Ultimo_Pago, Saldo_Inicial_Capital, Otorgado, Pagos, Ajuste_Cargo_Capital, Ajuste_Abono_Capital,
            Saldo_Final_Capital, Calculo, Diferencia, Capital_Vigente, Capital_Vencido, Saldo_Inicial_Interes,
            Devengamiento, Pagos_Interes, Ajuste_Cargo_Interes, Ajuste_Abono_Interes, Interes_No_Devengado,
            Saldo_Final_Interes, Calculo_Interes, Diferencia_Interes, Interes_Devengado, IVA_Interes_Devengado,
            Interes_No_DevengadoB,
            CASE 
                WHEN Fecha_Cartera_Vencida = '0000-00-00' OR Fecha_Cartera_Vencida = '' OR Fecha_Cartera_Vencida = '0.00' THEN NULL 
                ELSE STR_TO_DATE(Fecha_Cartera_Vencida, '%Y-%m-%d') 
            END AS Fecha_Cartera_Vencida,
            Saldo_Contable, Saldo_Insoluto, Porc_Provision, Reserva, nCAT, vOpTable, Status,
            CONCAT(CURDATE(), ' ', @CurrentPeriod) AS FechaGenerado
        FROM Stage_Saldos_Contables;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    command.Parameters.AddWithValue("@CurrentPeriod", GetCurrentPeriod());

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
                    await D2_WriteLog(logBuilder.ToString(), logPath);
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

    private void D2_MoveFilesToHistoric(string originalFilePath, string textFilePath, StringBuilder logBuilder)
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

    private async Task D2_WriteLog(string logContent, string logPath)
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
