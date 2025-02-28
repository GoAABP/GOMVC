using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Text;
using System.Threading.Tasks;

public class B2_Amortizacion_Controller : Controller
{
    private readonly ILogger<B2_Amortizacion_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;

    public B2_Amortizacion_Controller(ILogger<B2_Amortizacion_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
    }

    [HttpPost]
    public async Task<IActionResult> B2_Process()
    {
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - B2_Process started.");
        _logger.LogInformation("B2_Amortizacion process started.");

        try
        {
            // Paso 1: Truncar la tabla B2_Amortizaciones (tabla final)
            await B2_TruncateFinalTable(logBuilder);

            // Paso 2: Insertar datos de B2_Stage_Amortizaciones en B2_Amortizaciones,
            // convirtiendo los campos de fecha (vDueDate y dtInsert)
            await B2_InsertIntoFinalTable(logBuilder);

            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - B2_Process completed successfully.");
            _logger.LogInformation("B2_Amortizacion process completed successfully.");
            return Ok("Process completed successfully.");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - B2_Process failed: {ex.Message}");
            _logger.LogError(ex, "Error in B2_Amortizacion process.");
            return StatusCode(500, "An error occurred during the process.");
        }
        finally
        {
            // Aquí se podría escribir el log en un archivo si es necesario.
            // Ejemplo: System.IO.File.WriteAllText(logFilePath, logBuilder.ToString());
        }
    }

    private async Task B2_TruncateFinalTable(StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            string sqlTruncate = "TRUNCATE TABLE B2_Amortizaciones;";
            using (var command = new MySqlCommand(sqlTruncate, connection))
            {
                await command.ExecuteNonQueryAsync();
            }
            logBuilder.AppendLine("B2_Truncated table B2_Amortizaciones.");
            _logger.LogInformation("B2_Truncated table B2_Amortizaciones.");
        }
    }

    private async Task B2_InsertIntoFinalTable(StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    string sqlInsert = @"
                        INSERT INTO B2_Amortizaciones (
                            iCreditId, 
                            iPayment, 
                            vDueDate, 
                            nPrincipal, 
                            nInterest, 
                            nTax, 
                            dtInsert
                        )
                        SELECT 
                            iCreditId,
                            iPayment,
                            CASE 
                                WHEN TRIM(vDueDate) = '' THEN NULL
                                WHEN vDueDate REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(vDueDate, '%d/%m/%Y')
                                WHEN vDueDate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(vDueDate, '%Y-%m-%d')
                                ELSE NULL
                            END AS vDueDate,
                            nPrincipal,
                            nInterest,
                            nTax,
                            CASE 
                                WHEN TRIM(dtInsert) = '' THEN NULL
                                WHEN dtInsert REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN STR_TO_DATE(dtInsert, '%d/%m/%Y %H:%i:%s')
                                WHEN dtInsert REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN STR_TO_DATE(dtInsert, '%Y-%m-%d %H:%i:%s')
                                ELSE NULL
                            END AS dtInsert
                        FROM B2_Stage_Amortizaciones;";

                    using (var command = new MySqlCommand(sqlInsert, connection, transaction))
                    {
                        int rowsAffected = await command.ExecuteNonQueryAsync();
                        logBuilder.AppendLine($"B2_Inserted {rowsAffected} rows into B2_Amortizaciones.");
                        _logger.LogInformation($"B2_Inserted {rowsAffected} rows into B2_Amortizaciones.");
                    }

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"B2_Error during insert: {ex.Message}");
                    _logger.LogError(ex, "B2_Error during insert into B2_Amortizaciones.");
                    throw;
                }
            }
        }
    }
}
