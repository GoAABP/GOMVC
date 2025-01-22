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
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
    }

    public async Task D2_ProcessSaldosContables()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D2_Saldos_Contables_Bulk.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.");
        _logger.LogInformation("Process started.");

        var files = Directory.GetFiles(_filePath, "SaldosContables*.csv");
        if (files.Length == 0)
        {
            var errorLog = "File not found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D2_WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }

        var file = files[0];
        logBuilder.AppendLine($"File found: {file}");

        try
        {
            await D2_BulkInsertSaldosContablesData(file, logBuilder);
            await D2_ExecuteSaldosContablesInsert(logBuilder, logPath);
            D2_MoveFilesToHistoric(file, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await D2_WriteLog(logBuilder.ToString(), logPath);
            throw;
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.");
        _logger.LogInformation("Process completed successfully.");
        await D2_WriteLog(logBuilder.ToString(), logPath);
    }

    public async Task D2_ProcessHistoricSaldosContables()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D2_Saldos_Contables_Historic_Bulk.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process started.");
        _logger.LogInformation("Historic process started.");

        var files = Directory.GetFiles(_filePath, "SaldosContables_*.csv");
        if (files.Length == 0)
        {
            var errorLog = "No historic files found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D2_WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }

        foreach (var file in files)
        {
            logBuilder.AppendLine($"Processing file: {file}");
            try
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                var match = Regex.Match(fileName, @"SaldosContables_(\d{2})(\d{2})(\d{4})(Morning|Afternoon|Night)");

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

                await D2_BulkInsertSaldosContablesData(file, logBuilder);
                await D2_InsertHistoricSaldosContables(fechaGenerado, logBuilder, logPath);
                D2_MoveFilesToHistoric(file, logBuilder);
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error processing file {file}: {ex.Message}");
                _logger.LogError(ex, $"Error processing file {file}.");
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Historic process completed.");
        _logger.LogInformation("Historic process completed.");
        await D2_WriteLog(logBuilder.ToString(), logPath);
    }

    private async Task<bool> ValidateInsertPeriod(MySqlConnection connection, DateTime currentTime)
    {
        string period = currentTime.TimeOfDay switch
        {
            var t when t >= TimeSpan.FromHours(0) && t <= TimeSpan.FromHours(10) => "Morning",
            var t when t > TimeSpan.FromHours(10) && t <= TimeSpan.FromHours(17) => "Afternoon",
            _ => "Night"
        };

        string query = @"
            SELECT COUNT(*)
            FROM D2_Saldos_Contables
            WHERE DATE(FechaGenerado) = @Today
              AND CASE
                  WHEN TIME(FechaGenerado) BETWEEN '00:00:00' AND '10:00:00' THEN 'Morning'
                  WHEN TIME(FechaGenerado) BETWEEN '10:01:00' AND '17:00:00' THEN 'Afternoon'
                  WHEN TIME(FechaGenerado) BETWEEN '17:01:00' AND '23:59:59' THEN 'Night'
              END = @Period";

        using (var command = new MySqlCommand(query, connection))
        {
            command.Parameters.AddWithValue("@Today", currentTime.Date);
            command.Parameters.AddWithValue("@Period", period);

            int count = Convert.ToInt32(await command.ExecuteScalarAsync());
            return count == 0;
        }
    }

    private async Task D2_ExecuteSaldosContablesInsert(StringBuilder logBuilder, string logPath)
    {
        var sqlInsertCommand = @"
            INSERT INTO D2_Saldos_Contables (
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
                STR_TO_DATE(NULLIF(Fecha_Apertura, ''), '%d/%m/%Y') AS Fecha_Apertura, 
                STR_TO_DATE(NULLIF(Fecha_Terminacion, ''), '%d/%m/%Y') AS Fecha_Terminacion, 
                Importe, Dias_Atraso, Cuotas_Atrasadas, Periodos_Atraso, Pagos_Sostenidos, Pago, Frecuencia, 
                STR_TO_DATE(NULLIF(Fecha_Ultimo_Pago, ''), '%d/%m/%Y') AS Fecha_Ultimo_Pago, 
                Importe_Ultimo_Pago, Saldo_Inicial_Capital, Otorgado, Pagos, Ajuste_Cargo_Capital, Ajuste_Abono_Capital, 
                Saldo_Final_Capital, Calculo, Diferencia, Capital_Vigente, Capital_Vencido, Saldo_Inicial_Interes, 
                Devengamiento, Pagos_Interes, Ajuste_Cargo_Interes, Ajuste_Abono_Interes, Interes_No_Devengado, 
                Saldo_Final_Interes, Calculo_Interes, Diferencia_Interes, Interes_Devengado, IVA_Interes_Devengado, 
                Interes_No_DevengadoB, 
                STR_TO_DATE(NULLIF(Fecha_Cartera_Vencida, ''), '%d/%m/%Y') AS Fecha_Cartera_Vencida, 
                Saldo_Contable, Saldo_Insoluto, Porc_Provision, Reserva, nCAT, vOpTable, Status, 
                NOW() AS FechaGenerado
            FROM D2_Stage_Saldos_Contables;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();

            // Validate period before proceeding
            if (!await ValidateInsertPeriod(connection, DateTime.Now))
            {
                var errorMessage = "An insert for this period already exists.";
                logBuilder.AppendLine(errorMessage);
                _logger.LogError(errorMessage);
                await D2_WriteLog(logBuilder.ToString(), logPath);
                throw new InvalidOperationException(errorMessage);
            }

            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Inserted data successfully into D2_Saldos_Contables.");
                    _logger.LogInformation("Inserted data successfully into D2_Saldos_Contables.");
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

    private async Task D2_InsertHistoricSaldosContables(DateTime fechaGenerado, StringBuilder logBuilder, string logPath)
    {
        var sqlInsertCommand = @"
            INSERT INTO D2_Saldos_Contables (
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
                STR_TO_DATE(NULLIF(Fecha_Apertura, ''), '%d/%m/%Y') AS Fecha_Apertura, 
                STR_TO_DATE(NULLIF(Fecha_Terminacion, ''), '%d/%m/%Y') AS Fecha_Terminacion, 
                Importe, Dias_Atraso, Cuotas_Atrasadas, Periodos_Atraso, Pagos_Sostenidos, Pago, Frecuencia, 
                STR_TO_DATE(NULLIF(Fecha_Ultimo_Pago, ''), '%d/%m/%Y') AS Fecha_Ultimo_Pago, 
                Importe_Ultimo_Pago, Saldo_Inicial_Capital, Otorgado, Pagos, Ajuste_Cargo_Capital, Ajuste_Abono_Capital, 
                Saldo_Final_Capital, Calculo, Diferencia, Capital_Vigente, Capital_Vencido, Saldo_Inicial_Interes, 
                Devengamiento, Pagos_Interes, Ajuste_Cargo_Interes, Ajuste_Abono_Interes, Interes_No_Devengado, 
                Saldo_Final_Interes, Calculo_Interes, Diferencia_Interes, Interes_Devengado, IVA_Interes_Devengado, 
                Interes_No_DevengadoB, 
                STR_TO_DATE(NULLIF(Fecha_Cartera_Vencida, ''), '%d/%m/%Y') AS Fecha_Cartera_Vencida, 
                Saldo_Contable, Saldo_Insoluto, Porc_Provision, Reserva, nCAT, vOpTable, Status, 
                @FechaGenerado
            FROM D2_Stage_Saldos_Contables;";

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
                    logBuilder.AppendLine("Inserted historic data successfully into D2_Saldos_Contables.");
                    _logger.LogInformation("Inserted historic data successfully into D2_Saldos_Contables.");
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

    private async Task D2_BulkInsertSaldosContablesData(string csvFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D2_Stage_Saldos_Contables;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D2_Stage_Saldos_Contables.");

                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{csvFilePath.Replace("\\", "\\\\")}' " +
                                          "INTO TABLE D2_Stage_Saldos_Contables " +
                                          "FIELDS TERMINATED BY ',' " +
                                          "ENCLOSED BY '\"' " +
                                          "LINES TERMINATED BY '\\n' " +
                                          "IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Bulk inserted data into D2_Stage_Saldos_Contables.");

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

    private void D2_MoveFilesToHistoric(string filePath, StringBuilder logBuilder)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");
        var fileName = Path.GetFileNameWithoutExtension(filePath);
        var fileExtension = Path.GetExtension(filePath);
        var newFileName = $"{fileName}_{timestamp}{fileExtension}";
        var historicFilePath = Path.Combine(_historicFilePath, newFileName);

        try
        {
            System.IO.File.Move(filePath, historicFilePath);
            logBuilder.AppendLine($"Moved file to historic: {historicFilePath}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error moving file to historic: {ex.Message}");
            _logger.LogError(ex, "Error moving file to historic.");
            throw;
        }
    }

    private async Task D2_WriteLog(string logContent, string logPath)
    {
        var logDirectory = Path.GetDirectoryName(logPath);
        var historicDirectory = _historicFilePath;

        var uniqueLogName = $"D2_Saldos_Contables_Bulk_{DateTime.Now:yyyy-MM-dd_HH-mm}.log";
        var fullLogPath = Path.Combine(logDirectory!, uniqueLogName);

        var existingLogs = Directory.GetFiles(logDirectory!, "D2_Saldos_Contables_Bulk*.log");
        foreach (var existingLog in existingLogs)
        {
            var historicLogPath = Path.Combine(historicDirectory, Path.GetFileName(existingLog));
            System.IO.File.Move(existingLog, historicLogPath);
            _logger.LogInformation($"Moved existing log to historic: {historicLogPath}");
        }

        await System.IO.File.WriteAllTextAsync(fullLogPath, logContent);
        _logger.LogInformation($"New log written to: {fullLogPath}");
    }
}
