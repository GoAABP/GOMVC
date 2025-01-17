using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
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
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadSaldosContables.log";
        var logBuilder = new StringBuilder();
        var startLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.";
        logBuilder.AppendLine(startLog);
        _logger.LogInformation(startLog);

        var files = Directory.GetFiles(_filePath, "SDOSCONT*.CSV");
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

        var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.";
        logBuilder.AppendLine(endLog);
        _logger.LogInformation(endLog);
        await D2_WriteLog(logBuilder.ToString(), logPath);
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
                    _logger.LogInformation("Truncated table D2_Stage_Saldos_Contables.");

                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{csvFilePath.Replace("\\", "\\\\")}' " +
                                          "INTO TABLE D2_Stage_Saldos_Contables " +
                                          "FIELDS TERMINATED BY ',' " +
                                          "ENCLOSED BY '\"' " +
                                          "LINES TERMINATED BY '\\n' " +
                                          "IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Bulk inserted data into D2_Stage_Saldos_Contables.");
                    _logger.LogInformation("Bulk inserted data into D2_Stage_Saldos_Contables.");

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during bulk insert for SaldosContables: {ex.Message}");
                    _logger.LogError(ex, "Error during bulk insert for SaldosContables.");
                    throw;
                }
            }
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
            STR_TO_DATE(NULLIF(Fecha_Apertura, ''), '%d/%m/%Y'),
            STR_TO_DATE(NULLIF(Fecha_Terminacion, ''), '%d/%m/%Y'),
            Importe, Dias_Atraso, Cuotas_Atrasadas, Periodos_Atraso, Pagos_Sostenidos, Pago, Frecuencia,
            STR_TO_DATE(NULLIF(Fecha_Ultimo_Pago, ''), '%d/%m/%Y'),
            Importe_Ultimo_Pago, Saldo_Inicial_Capital, Otorgado, Pagos, Ajuste_Cargo_Capital, Ajuste_Abono_Capital,
            Saldo_Final_Capital, Calculo, Diferencia, Capital_Vigente, Capital_Vencido, Saldo_Inicial_Interes,
            Devengamiento, Pagos_Interes, Ajuste_Cargo_Interes, Ajuste_Abono_Interes, Interes_No_Devengado,
            Saldo_Final_Interes, Calculo_Interes, Diferencia_Interes, Interes_Devengado, IVA_Interes_Devengado,
            Interes_No_DevengadoB,
            STR_TO_DATE(NULLIF(Fecha_Cartera_Vencida, ''), '%d/%m/%Y'),
            Saldo_Contable, Saldo_Insoluto, Porc_Provision, Reserva, nCAT, vOpTable, Status,
            NOW() AS FechaGenerado
        FROM D2_Stage_Saldos_Contables;
        ";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Executed SQL insert successfully.");
                    _logger.LogInformation("Executed SQL insert successfully.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error executing SQL insert: {ex.Message}");
                    _logger.LogError(ex, "Error executing SQL insert.");
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
            _logger.LogInformation($"Moved file to historic: {historicFilePath}");
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
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");
        var uniqueLogPath = Path.Combine(
            Path.GetDirectoryName(logPath)!,
            $"{Path.GetFileNameWithoutExtension(logPath)}_{timestamp}{Path.GetExtension(logPath)}"
        );

        try
        {
            await System.IO.File.WriteAllTextAsync(uniqueLogPath, logContent);
            _logger.LogInformation($"Log written to: {uniqueLogPath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error writing log.");
            throw;
        }
    }
}
