using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;

public class D3_Aplicaciones_Pagos_Controller : Controller
{
    private readonly ILogger<D3_Aplicaciones_Pagos_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

    public D3_Aplicaciones_Pagos_Controller(ILogger<D3_Aplicaciones_Pagos_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
    }

    // D3_ProcessAplicacionPagos: Process the AplicacionPagos activity
    [HttpPost("D3_ProcessAplicacionPagos")]
    public async Task<IActionResult> D3_ProcessAplicacionPagos()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadAplicacionPagos.log";
        var logBuilder = new StringBuilder();
        var startLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.";
        logBuilder.AppendLine(startLog);
        _logger.LogInformation(startLog);

        var files = Directory.GetFiles(_filePath, "Aplicacion de pagos por fecha de Aplica.csv");
        if (files.Length == 0)
        {
            var errorLog = "File not found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D3_WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }

        var file = files[0];
        logBuilder.AppendLine("File found.");
        try
        {
            var textFilePath = await D3_ConvertAplicacionPagosCsvToText(file, logBuilder);
            await D3_BulkInsertAplicacionPagosData(textFilePath, logBuilder);
            await D3_ExecuteAplicacionPagosInsertFromStagingTable(logBuilder, logPath); // Insert directly from the staging table
            D3_MoveFilesToHistoric(file, textFilePath, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await D3_WriteLog(logBuilder.ToString(), logPath);
            throw;
        }

        var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.";
        logBuilder.AppendLine(endLog);
        _logger.LogInformation(endLog);
        await D3_WriteLog(logBuilder.ToString(), logPath);
        return Ok("Aplicacion Pagos processed successfully.");
    }

    // D3_ConvertAplicacionPagosCsvToText: Convert the CSV to text format for Aplicacion Pagos
    private async Task<string> D3_ConvertAplicacionPagosCsvToText(string csvFilePath, StringBuilder logBuilder)
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

                    // Replace line breaks and adjust date formats
                    var processedLine = line.Normalize(NormalizationForm.FormC);
                    processedLine = Regex.Replace(processedLine, ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", "\n");
                    processedLine = Regex.Replace(processedLine, @"(\d{2})/(\d{2})/(\d{4})", "$3-$2-$1"); // Convert MM/DD/YYYY to YYYY-MM-DD

                    sb.AppendLine(processedLine);
                }
            }

            await System.IO.File.WriteAllTextAsync(textFilePath, sb.ToString(), Encoding.UTF8);
            var logMessage = $"Converted CSV to text for Aplicacion Pagos: {textFilePath}";
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

    // D3_BulkInsertAplicacionPagosData: Perform bulk insert of Aplicacion Pagos data
    private async Task D3_BulkInsertAplicacionPagosData(string textFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE Stage_Aplicacion_Pagos;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    var logMessage = "Truncated table Stage_Aplicacion_Pagos.";
                    logBuilder.AppendLine(logMessage);
                    _logger.LogInformation(logMessage);

                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{textFilePath.Replace("\\", "\\\\")}' " +
                                          "INTO TABLE Stage_Aplicacion_Pagos " +
                                          "FIELDS TERMINATED BY '|' " +
                                          "ENCLOSED BY '\"' " +
                                          "LINES TERMINATED BY '\\n' " +
                                          "IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logMessage = "Bulk inserted data into Stage_Aplicacion_Pagos.";
                    logBuilder.AppendLine(logMessage);
                    _logger.LogInformation(logMessage);

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error during bulk insert for Aplicacion Pagos: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    throw;
                }
            }
        }
    }

    // D3_ExecuteAplicacionPagosInsertFromStagingTable: Directly insert from the staging table into the final table
    private async Task D3_ExecuteAplicacionPagosInsertFromStagingTable(StringBuilder logBuilder, string logPath)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var insertCommandText = @"
                        INSERT INTO D3_Aplicacion_Pagos (
                            Id_Credito, Id_Convenio, Convenio, Referencia, Id_Pago, Nombre_Cliente, Financiamiento, Origen_de_Movimiento,
                            Fecha_Pago, Fecha_Aplicacion, Fecha_Deposito, Status, Pago, Capital, Interes, IVA_Int, Comision_Financiada,
                            IVA_Comision_Financ, Moratorios, IVA_Mora, Pago_Tardio, IVA_PagoTardio, Recuperacion, IVA_Recup,
                            Com_Liquidacion, IVA_Com_Liquidacion, Retencion_X_Admon, IVA_Retencion_X_Admon, Pago_Exceso, Gestor,
                            Forma_de_pago, vMotive
                        )
                        SELECT 
                            Id_Credito, Id_Convenio, Convenio, Referencia, Id_Pago, Nombre_Cliente, Financiamiento, Origen_de_Movimiento,
                            STR_TO_DATE(Fecha_Pago, '%Y-%m-%d'), STR_TO_DATE(Fecha_Aplicacion, '%Y-%m-%d'), STR_TO_DATE(Fecha_Deposito, '%Y-%m-%d'),
                            Status, Pago, Capital, Interes, IVA_Int, Comision_Financiada, IVA_Comision_Financ, Moratorios, IVA_Mora,
                            Pago_Tardio, IVA_PagoTardio, Recuperacion, IVA_Recup, Com_Liquidacion, IVA_Com_Liquidacion, Retencion_X_Admon,
                            IVA_Retencion_X_Admon, Pago_Exceso, Gestor, Forma_de_pago, vMotive
                        FROM Stage_Aplicacion_Pagos;";

                    var insertCommand = new MySqlCommand(insertCommandText, connection, transaction);
                    await insertCommand.ExecuteNonQueryAsync();
                    var logMessage = "Inserted data from Stage_Aplicacion_Pagos into D3_Aplicacion_Pagos.";
                    logBuilder.AppendLine(logMessage);
                    _logger.LogInformation(logMessage);

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error during insert for Aplicacion Pagos from staging to final table: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    await D3_WriteLog(logBuilder.ToString(), logPath);
                    throw;
                }
            }
        }
    }

    // D3_MoveFilesToHistoric: Move files to the historic folder after processing
    private void D3_MoveFilesToHistoric(string originalFilePath, string textFilePath, StringBuilder logBuilder)
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

    // D3_WriteLog: Write logs to a log file after processing
    private async Task D3_WriteLog(string logContent, string logPath)
    {
        var historicLogPath = Path.Combine(_historicFilePath, $"BulkLoad_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.log");
        await System.IO.File.WriteAllTextAsync(historicLogPath, logContent, Encoding.UTF8);
    }
}
