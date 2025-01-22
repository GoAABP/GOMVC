using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

public class D6_Quebrantos_Controller : Controller
{
    private readonly ILogger<D6_Quebrantos_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

    public D6_Quebrantos_Controller(ILogger<D6_Quebrantos_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
    }

    public async Task D6_ProcessQuebrantos()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D6_Quebrantos_Bulk.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.");
        _logger.LogInformation("Process started.");

        var files = Directory.GetFiles(_filePath, "Quebrantos_Datos_Cobranza*.csv");
        if (files.Length == 0)
        {
            var errorLog = "File not found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D6_WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }

        var file = files[0];
        logBuilder.AppendLine($"File found: {file}");

        try
        {
            var textFilePath = await D6_ConvertQuebrantosCsvToText(file, logBuilder);
            await D6_BulkInsertQuebrantos(textFilePath, logBuilder);
            await D6_ExecuteQuebrantosInsert(logBuilder, logPath);
            D6_MoveFilesToHistoric(file, textFilePath, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await D6_WriteLog(logBuilder.ToString(), logPath);
            throw;
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.");
        _logger.LogInformation("Process completed successfully.");
        await D6_WriteLog(logBuilder.ToString(), logPath);
    }

    public async Task D6_ProcessHistoricQuebrantos()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D6_Quebrantos_Historic_Bulk.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process started.");
        _logger.LogInformation("Historic process started.");

        var files = Directory.GetFiles(_filePath, "Quebrantos_Datos_Cobranza_*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No historic files found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D6_WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }

        foreach (var file in files)
        {
            logBuilder.AppendLine($"Processing file: {file}");
            try
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                var match = Regex.Match(fileName, @"Quebrantos_Datos_Cobranza_(\d{2})(\d{2})(\d{4})(Morning|Afternoon|Night)");

                if (!match.Success)
                {
                    var errorLog = $"Invalid file name format: {fileName}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(errorLog);
                    continue;
                }

                var day = int.Parse(match.Groups[1].Value);
                var month = int.Parse(match.Groups[2].Value);
                var year = int.Parse(match.Groups[3].Value);
                var period = match.Groups[4].Value;

                var parsedDate = new DateTime(year, month, day);
                var defaultTime = period switch
                {
                    "Morning" => new TimeSpan(8, 0, 0),
                    "Afternoon" => new TimeSpan(14, 0, 0),
                    "Night" => new TimeSpan(20, 0, 0),
                    _ => throw new InvalidOperationException("Unknown period.")
                };
                var fechaGenerado = parsedDate.Add(defaultTime);

                logBuilder.AppendLine($"Parsed FechaGenerado: {fechaGenerado} for file: {file}");

                var textFilePath = await D6_ConvertQuebrantosCsvToText(file, logBuilder);
                await D6_BulkInsertQuebrantos(textFilePath, logBuilder);
                await D6_InsertHistoricQuebrantos(fechaGenerado, logBuilder, logPath);
                D6_MoveFilesToHistoric(file, textFilePath, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}.");
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process completed.");
        _logger.LogInformation("Historic process completed.");
        await D6_WriteLog(logBuilder.ToString(), logPath);
    }

    private async Task D6_BulkInsertQuebrantos(string textFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE Stage_Quebrantos;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table Stage_Quebrantos.");

                    var loadCommandText = $@"
                        LOAD DATA LOCAL INFILE '{textFilePath.Replace("\\", "\\\\")}' 
                        INTO TABLE Stage_Quebrantos 
                        FIELDS TERMINATED BY '|' 
                        LINES TERMINATED BY '\n' 
                        IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Bulk inserted data into Stage_Quebrantos from {textFilePath}.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during bulk insert: {ex.Message}");
                    throw;
                }
            }
        }
    }

    private async Task D6_ExecuteQuebrantosInsert(StringBuilder logBuilder, string logPath)
    {
        var sqlInsertCommand = @"
            INSERT INTO Quebrantos (
                Operacion, Referencia, Nombre, Convenio, vFinancingTypeId, KVigente, KVencido, IntVencido,
                IVAIntVencido, IntVencidoCO, IVAIntVencidoCO, TotalQuebranto, PagosRealizados, SdoPendiente,
                IntXDevengar, SdoTotalXPagar, FechaQuebranto, UltPagoTeorico, UltimoPago, UltPagoApl,
                Gestor, nCommission, nCommTax, vMotive, FechaGenerado
            )
            SELECT
                Operacion, Referencia, Nombre, Convenio, vFinancingTypeId, KVigente, KVencido, IntVencido,
                IVAIntVencido, IntVencidoCO, IVAIntVencidoCO, TotalQuebranto, PagosRealizados, SdoPendiente,
                IntXDevengar, SdoTotalXPagar,
                CASE
                    WHEN FechaQuebranto = '0000-00-00' OR FechaQuebranto = '' THEN NULL
                    ELSE STR_TO_DATE(FechaQuebranto, '%Y-%m-%d')
                END AS FechaQuebranto,
                UltPagoTeorico, UltimoPago, UltPagoApl, Gestor, nCommission, nCommTax, vMotive,
                NOW() AS FechaGenerado
            FROM Stage_Quebrantos;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Inserted data successfully into Quebrantos.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during insert: {ex.Message}");
                    throw;
                }
            }
        }
    }

    private async Task D6_InsertHistoricQuebrantos(DateTime fechaGenerado, StringBuilder logBuilder, string logPath)
    {
        var sqlInsertCommand = @"
            INSERT INTO Quebrantos (
                Operacion, Referencia, Nombre, Convenio, vFinancingTypeId, KVigente, KVencido, IntVencido,
                IVAIntVencido, IntVencidoCO, IVAIntVencidoCO, TotalQuebranto, PagosRealizados, SdoPendiente,
                IntXDevengar, SdoTotalXPagar, FechaQuebranto, UltPagoTeorico, UltimoPago, UltPagoApl,
                Gestor, nCommission, nCommTax, vMotive, FechaGenerado
            )
            SELECT
                Operacion, Referencia, Nombre, Convenio, vFinancingTypeId, KVigente, KVencido, IntVencido,
                IVAIntVencido, IntVencidoCO, IVAIntVencidoCO, TotalQuebranto, PagosRealizados, SdoPendiente,
                IntXDevengar, SdoTotalXPagar,
                CASE
                    WHEN FechaQuebranto = '0000-00-00' OR FechaQuebranto = '' THEN NULL
                    ELSE STR_TO_DATE(FechaQuebranto, '%Y-%m-%d')
                END AS FechaQuebranto,
                UltPagoTeorico, UltimoPago, UltPagoApl, Gestor, nCommission, nCommTax, vMotive,
                @FechaGenerado
            FROM Stage_Quebrantos;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    command.Parameters.AddWithValue("@FechaGenerado", fechaGenerado);
                    await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Inserted historic data successfully into Quebrantos.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during historic insert: {ex.Message}");
                    throw;
                }
            }
        }
    }

    private async Task<string> D6_ConvertQuebrantosCsvToText(string csvFilePath, StringBuilder logBuilder)
    {
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
                    sb.AppendLine(line.Replace(",", "|"));
                }
            }

            await System.IO.File.WriteAllTextAsync(textFilePath, sb.ToString());
            logBuilder.AppendLine($"Converted CSV to text: {textFilePath}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error during conversion: {ex.Message}");
            throw;
        }

        return textFilePath;
    }

    private void D6_MoveFilesToHistoric(string originalFilePath, string textFilePath, StringBuilder logBuilder)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");

        var originalHistoricPath = Path.Combine(_historicFilePath, $"{Path.GetFileNameWithoutExtension(originalFilePath)}_{timestamp}{Path.GetExtension(originalFilePath)}");
        System.IO.File.Move(originalFilePath, originalHistoricPath);
        logBuilder.AppendLine($"Moved original file to historic: {originalHistoricPath}");

        var textHistoricPath = Path.Combine(_historicFilePath, $"{Path.GetFileNameWithoutExtension(textFilePath)}_{timestamp}{Path.GetExtension(textFilePath)}");
        System.IO.File.Move(textFilePath, textHistoricPath);
        logBuilder.AppendLine($"Moved text file to historic: {textHistoricPath}");
    }

    private async Task D6_WriteLog(string logContent, string logPath)
    {
        var logDirectory = Path.GetDirectoryName(logPath);
        var timestampedLogPath = Path.Combine(_historicFilePath, $"D6_Quebrantos_Log_{DateTime.Now:yyyy-MM-dd_HH-mm}.log");

        if (System.IO.File.Exists(logPath))
        {
            System.IO.File.Move(logPath, timestampedLogPath);
        }

        await System.IO.File.WriteAllTextAsync(logPath, logContent);
    }
}
