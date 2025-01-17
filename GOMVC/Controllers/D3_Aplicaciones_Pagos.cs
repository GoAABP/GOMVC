using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

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
#pragma warning disable CS8601 // Possible null reference assignment.
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
#pragma warning restore CS8601 // Possible null reference assignment.
    }

    public async Task D3_ProcessAplicacionPagos()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadAplicacionPagos.log";
        var logBuilder = new StringBuilder();
        var startLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.";
        logBuilder.AppendLine(startLog);
        _logger.LogInformation(startLog);

        var files = Directory.GetFiles(_filePath, "Aplicacion de pagos por fecha de Aplica*.csv");
        if (files.Length == 0)
        {
            var errorLog = "File not found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }

        var file = files[0];
        logBuilder.AppendLine($"File found: {file}");

        try
        {
            await D3_LoadDataToStageWithCondition(file, logBuilder);
            await D3_ExecuteAplicacionPagosInsertFromStagingTable(logBuilder, logPath);
            MoveFileToHistoric(file, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await WriteLog(logBuilder.ToString(), logPath);
            throw;
        }

        var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.";
        logBuilder.AppendLine(endLog);
        _logger.LogInformation(endLog);
        await WriteLog(logBuilder.ToString(), logPath);
    }

    private void MoveFileToHistoric(string filePath, StringBuilder logBuilder)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");
        var fileName = Path.GetFileNameWithoutExtension(filePath);
        var fileExtension = Path.GetExtension(filePath);
        var newFileName = $"{fileName}_{timestamp}{fileExtension}";
        var historicFilePath = Path.Combine(_historicFilePath, newFileName);

        System.IO.File.Move(filePath, historicFilePath);
        logBuilder.AppendLine($"Moved file to historic: {historicFilePath}");
        _logger.LogInformation($"Moved file to historic: {historicFilePath}");
    }

    private async Task WriteLog(string logContent, string logPath)
    {
        var historicLogPath = Path.Combine(_historicFilePath, $"BulkLoad_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.log");
        
        // Ensure unique log file name
        if (System.IO.File.Exists(logPath))
        {
#pragma warning disable CS8604 // Possible null reference argument.
            var uniqueLogPath = Path.Combine(
                Path.GetDirectoryName(logPath),
                $"{Path.GetFileNameWithoutExtension(logPath)}_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}{Path.GetExtension(logPath)}"
            );
#pragma warning restore CS8604 // Possible null reference argument.
            System.IO.File.Move(logPath, uniqueLogPath);
        }

        await System.IO.File.WriteAllTextAsync(logPath, logContent);
    }

    private async Task D3_LoadDataToStageWithCondition(string csvFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D3_Stage_Aplicacion_Pagos;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D3_Stage_Aplicacion_Pagos.");
                    _logger.LogInformation("Truncated table D3_Stage_Aplicacion_Pagos.");

                    using (var reader = new StreamReader(csvFilePath))
                    {
                        string line;
                        bool stopInserting = false;

                        if (!reader.EndOfStream) await reader.ReadLineAsync();

                        while (!reader.EndOfStream && !stopInserting)
                        {
#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
                            line = await reader.ReadLineAsync();
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
#pragma warning disable CS8602 // Dereference of a possibly null reference.
                            var values = line.Split(',');
#pragma warning restore CS8602 // Dereference of a possibly null reference.

                            if (values.Length > 0 && values[0].Trim() == "0")
                            {
                                logBuilder.AppendLine("Encountered row with '0' in the first column. Stopping further insertion.");
                                _logger.LogInformation("Encountered row with '0' in the first column. Stopping further insertion.");
                                stopInserting = true;
                                continue;
                            }

                            var insertCommandText = $@"
                                INSERT INTO D3_Stage_Aplicacion_Pagos (
                                    Id_Credito, Id_Convenio, Convenio, Referencia, Id_Pago, Nombre_Cliente, Financiamiento, 
                                    Origen_de_Movimiento, Fecha_Pago, Fecha_Aplicacion
                                ) VALUES (
                                    @Value1, @Value2, @Value3, @Value4, @Value5, @Value6, @Value7, @Value8, @Value9, @Value10
                                );";

                            var insertCommand = new MySqlCommand(insertCommandText, connection, transaction);

                            for (int i = 0; i < values.Length; i++)
                            {
                                string cleanValue = values[i].Trim().Trim('"');
                                if (i == 0 || i == 1 || i == 4)
                                {
                                    if (int.TryParse(cleanValue, out int intValue))
                                    {
                                        insertCommand.Parameters.AddWithValue($"@Value{i + 1}", intValue);
                                    }
                                    else
                                    {
                                        insertCommand.Parameters.AddWithValue($"@Value{i + 1}", DBNull.Value);
                                    }
                                }
                                else
                                {
                                    insertCommand.Parameters.AddWithValue($"@Value{i + 1}", cleanValue);
                                }
                            }

                            await insertCommand.ExecuteNonQueryAsync();
                        }
                    }

                    await transaction.CommitAsync();
                    logBuilder.AppendLine("Data loaded into D3_Stage_Aplicacion_Pagos with condition applied.");
                    _logger.LogInformation("Data loaded into D3_Stage_Aplicacion_Pagos with condition applied.");
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error loading data into stage table: {ex.Message}");
                    _logger.LogError(ex, "Error loading data into stage table.");
                    throw;
                }
            }
        }
    }

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
                            STR_TO_DATE(Fecha_Pago, '%d/%m/%Y'), STR_TO_DATE(Fecha_Aplicacion, '%d/%m/%Y'), STR_TO_DATE(Fecha_Deposito, '%d/%m/%Y'),
                            Status, Pago, Capital, Interes, IVA_Int, Comision_Financiada, IVA_Comision_Financ, Moratorios, IVA_Mora,
                            Pago_Tardio, IVA_PagoTardio, Recuperacion, IVA_Recup, Com_Liquidacion, IVA_Com_Liquidacion, Retencion_X_Admon,
                            IVA_Retencion_X_Admon, Pago_Exceso, Gestor, Forma_de_pago, vMotive
                        FROM D3_Stage_Aplicacion_Pagos;";

                    var insertCommand = new MySqlCommand(insertCommandText, connection, transaction);
                    await insertCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Inserted data from D3_Stage_Aplicacion_Pagos into D3_Aplicacion_Pagos.");
                    _logger.LogInformation("Inserted data from D3_Stage_Aplicacion_Pagos into D3_Aplicacion_Pagos.");

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during insert for Aplicacion Pagos from staging to final table: {ex.Message}");
                    _logger.LogError(ex, "Error during insert for Aplicacion Pagos from staging to final table.");
                    await WriteLog(logBuilder.ToString(), logPath);
                    throw;
                }
            }
        }
    }
}
