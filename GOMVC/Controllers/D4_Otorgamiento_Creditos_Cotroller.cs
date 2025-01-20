using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

public class D4_Otorgamiento_Creditos_Controller : Controller
{
    private readonly ILogger<D4_Otorgamiento_Creditos_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\\Users\\Go Credit\\Documents\\DATA\\FLAT FILES";
    private readonly string _historicFilePath = @"C:\\Users\\Go Credit\\Documents\\DATA\\HISTORIC FILES";

    public D4_Otorgamiento_Creditos_Controller(ILogger<D4_Otorgamiento_Creditos_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
    }

    public async Task<IActionResult> D4_ProcessOtorgamientoCreditos()
    {
        var logPath = @"C:\\Users\\Go Credit\\Documents\\DATA\\LOGS\\BulkLoadOtorgamientoCreditos.log";
        var logBuilder = new StringBuilder();
        var startLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.";
        logBuilder.AppendLine(startLog);
        _logger.LogInformation(startLog);

        var files = Directory.GetFiles(_filePath, "BARTURO*.csv");
        if (files.Length == 0)
        {
            var errorLog = "File not found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D4_WriteLog(logBuilder.ToString(), logPath);
            return NotFound("No files found.");
        }

        var file = files[0];
        logBuilder.AppendLine($"File found: {file}");
        try
        {
            await D4_LoadDataToStageWithCondition(file, logBuilder);
            await D4_ExecuteInsert(logBuilder, logPath);
            D4_MoveFilesToHistoric(file, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await D4_WriteLog(logBuilder.ToString(), logPath);
            return StatusCode(500, "Error during processing.");
        }

        var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.";
        logBuilder.AppendLine(endLog);
        _logger.LogInformation(endLog);
        await D4_WriteLog(logBuilder.ToString(), logPath);
        return Ok("Activity processed successfully.");
    }
    private async Task D4_LoadDataToStageWithCondition(string csvFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D4_Stage_Otorgamiento_Creditos;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D4_Stage_Otorgamiento_Creditos.");
                    _logger.LogInformation("Truncated table D4_Stage_Otorgamiento_Creditos.");

                    using (var reader = new StreamReader(csvFilePath))
                    {
                        string line;
                        bool stopInserting = false;

                        // Skip the header line
                        if (!reader.EndOfStream) await reader.ReadLineAsync();

                        while (!reader.EndOfStream && !stopInserting)
                        {
                            line = await reader.ReadLineAsync();
                            var values = line.Split(',');

                            // Stop processing if the first column is "0"
                            if (values.Length > 0 && values[0].Trim() == "0")
                            {
                                logBuilder.AppendLine("Encountered row with '0' in the first column. Stopping further insertion.");
                                _logger.LogInformation("Encountered row with '0' in the first column. Stopping further insertion.");
                                stopInserting = true;
                                continue;
                            }

                            var insertCommandText = @"
                                INSERT INTO D4_Stage_Otorgamiento_Creditos (
                                    Id_Credito, Referencia, Nombre, Fecha_Apertura, F_Cobro, Id_Convenio, Convenio, Id_Sucursal, Sucursal,
                                    Capital, Primer_Pago, Comision, IVA, Cobertura, IVA_Cobertura, Disposicion, Monto_Retenido, Pago_de_Deuda,
                                    Comision_Financiada, IVA_Comision_Financiada, Solicitud, Vendedor, Nombre_Vendedor, TipoVendedor, vSupervisorId,
                                    vSupName, Producto, Descripcion_Tasa, Persona, Plazo, Id_Producto, vCampaign, Tipo_de_Financiamiento,
                                    vFinancingTypeId, vAliado
                                ) VALUES (
                                    @Value1, @Value2, @Value3, @Value4, @Value5, @Value6, @Value7, @Value8, @Value9,
                                    @Value10, @Value11, @Value12, @Value13, @Value14, @Value15, @Value16, @Value17, @Value18,
                                    @Value19, @Value20, @Value21, @Value22, @Value23, @Value24, @Value25, @Value26, @Value27,
                                    @Value28, @Value29, @Value30, @Value31, @Value32, @Value33, @Value34, @Value35
                                );";

                            var insertCommand = new MySqlCommand(insertCommandText, connection, transaction);

                            for (int i = 0; i < values.Length; i++)
                            {
                                insertCommand.Parameters.AddWithValue($"@Value{i + 1}", string.IsNullOrEmpty(values[i].Trim()) ? DBNull.Value : values[i].Trim());
                            }

                            await insertCommand.ExecuteNonQueryAsync();
                        }
                    }

                    await transaction.CommitAsync();
                    logBuilder.AppendLine("Data loaded into D4_Stage_Otorgamiento_Creditos with condition applied.");
                    _logger.LogInformation("Data loaded into D4_Stage_Otorgamiento_Creditos with condition applied.");
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
    private async Task D4_ExecuteInsert(StringBuilder logBuilder, string logPath)
    {
        var sqlInsertCommand = @"
        INSERT INTO D4_Otorgamiento_Creditos (
            Id_Credito, Referencia, Nombre, Fecha_Apertura, F_Cobro, Id_Convenio, Convenio, Id_Sucursal, Sucursal,
            Capital, Primer_Pago, Comision, IVA, Cobertura, IVA_Cobertura, Disposicion, Monto_Retenido, Pago_de_Deuda,
            Comision_Financiada, IVA_Comision_Financiada, Solicitud, Vendedor, Nombre_Vendedor, TipoVendedor, vSupervisorId,
            vSupName, Producto, Descripcion_Tasa, Persona, Plazo, Id_Producto, vCampaign, Tipo_de_Financiamiento,
            vFinancingTypeId, vAliado
        )
        SELECT 
            Id_Credito, Referencia, Nombre, STR_TO_DATE(NULLIF(Fecha_Apertura, ''), '%d/%m/%Y'), STR_TO_DATE(NULLIF(F_Cobro, ''), '%d/%m/%Y'),
            Id_Convenio, Convenio, Id_Sucursal, Sucursal,
            Capital, STR_TO_DATE(NULLIF(Primer_Pago, ''), '%d/%m/%Y'), Comision, IVA, Cobertura, IVA_Cobertura, Disposicion, Monto_Retenido, Pago_de_Deuda,
            Comision_Financiada, IVA_Comision_Financiada, Solicitud, Vendedor, Nombre_Vendedor, TipoVendedor, vSupervisorId,
            vSupName, Producto, Descripcion_Tasa, Persona, Plazo, Id_Producto, vCampaign, Tipo_de_Financiamiento,
            vFinancingTypeId, vAliado
        FROM D4_Stage_Otorgamiento_Creditos;";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Inserted data from D4_Stage_Otorgamiento_Creditos into D4_Otorgamiento_Creditos.");
                    _logger.LogInformation("Inserted data from D4_Stage_Otorgamiento_Creditos into D4_Otorgamiento_Creditos.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during insert for OtorgamientoCreditos: {ex.Message}");
                    _logger.LogError(ex, "Error during insert for OtorgamientoCreditos.");
                    throw;
                }
            }
        }
    }

    private void D4_MoveFilesToHistoric(string filePath, StringBuilder logBuilder)
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

    private async Task D4_WriteLog(string logContent, string logPath)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");
        var uniqueLogPath = Path.Combine(
            Path.GetDirectoryName(logPath)!,
            $"{Path.GetFileNameWithoutExtension(logPath)}_{timestamp}{Path.GetExtension(logPath)}"
        );

        try
        {
            await System.IO.File.WriteAllTextAsync(uniqueLogPath, logContent, Encoding.UTF8);
            _logger.LogInformation($"Log written to: {uniqueLogPath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error writing log.");
            throw;
        }
    }
}