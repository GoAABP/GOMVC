using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
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
            // Paso 1: Truncar la tabla final B2_Amortizaciones
            await B2_TruncateFinalTable(logBuilder);

            // Paso 2: Insertar datos desde la tabla de staging a la tabla final,
            // utilizando conversión de fechas con verificación adicional para valores "NULL"
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
                    // Obtener el total de registros en la tabla de staging
                    var countCmd = new MySqlCommand("SELECT COUNT(*) FROM B2_Stage_Amortizaciones", connection, transaction);
                    int totalRecords = Convert.ToInt32(await countCmd.ExecuteScalarAsync());
                    int batchSize = 1000; // Ajustable según necesidades
                    int batches = (totalRecords + batchSize - 1) / batchSize;
                    logBuilder.AppendLine($"Total de registros a procesar: {totalRecords} en {batches} lotes.");

                    for (int i = 0; i < batches; i++)
                    {
                        int offset = i * batchSize;
                        var selectCmd = new MySqlCommand(
                            "SELECT iCreditId, iPayment, vDueDate, nPrincipal, nInterest, nTax, dtInsert " +
                            "FROM B2_Stage_Amortizaciones " +
                            "LIMIT @batchSize OFFSET @offset", connection, transaction);
                        selectCmd.Parameters.AddWithValue("@batchSize", batchSize);
                        selectCmd.Parameters.AddWithValue("@offset", offset);

                        var tuples = new List<string>();
                        using (var reader = await selectCmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                int creditId = reader.GetInt32(0);
                                // Manejo de valores nulos en iPayment
                                string payment = reader.IsDBNull(1) ? "NULL" : reader.GetInt32(1).ToString();
                                string vDueDate = reader.IsDBNull(2) ? "" : reader.GetString(2);
                                string dtInsert = reader.IsDBNull(6) ? "" : reader.GetString(6);
                                string principal = reader.IsDBNull(3) ? "NULL" : reader.GetDecimal(3).ToString();
                                string interest = reader.IsDBNull(4) ? "NULL" : reader.GetDecimal(4).ToString();
                                string tax = reader.IsDBNull(5) ? "NULL" : reader.GetDecimal(5).ToString();

                                // Eliminamos saltos de línea y retornos de carro para detectar correctamente "NULL"
                                string tuple = $"({creditId}, {payment}, " +
                                    $"CASE WHEN TRIM(REPLACE(REPLACE('{vDueDate}', '\n', ''), '\r', '')) = '' " +
                                    $"OR UPPER(TRIM(REPLACE(REPLACE('{vDueDate}', '\n', ''), '\r', ''))) = 'NULL' " +
                                    $"THEN NULL ELSE STR_TO_DATE('{vDueDate}', '%d/%m/%Y') END, " +
                                    $"{principal}, {interest}, {tax}, " +
                                    $"CASE WHEN TRIM(REPLACE(REPLACE('{dtInsert}', '\n', ''), '\r', '')) = '' " +
                                    $"OR UPPER(TRIM(REPLACE(REPLACE('{dtInsert}', '\n', ''), '\r', ''))) = 'NULL' " +
                                    $"THEN NULL ELSE STR_TO_DATE('{dtInsert}', '%d/%m/%Y %H:%i:%s') END)";
                                tuples.Add(tuple);
                            }
                        }

                        if (tuples.Count > 0)
                        {
                            string insertSql = "INSERT INTO B2_Amortizaciones " +
                                "(iCreditId, iPayment, vDueDate, nPrincipal, nInterest, nTax, dtInsert) VALUES " +
                                string.Join(", ", tuples);
                            using (var insertCmd = new MySqlCommand(insertSql, connection, transaction))
                            {
                                int rowsAffected = await insertCmd.ExecuteNonQueryAsync();
                                logBuilder.AppendLine($"Lote {i + 1}/{batches}: Insertados {rowsAffected} registros.");
                            }
                        }
                    }
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error durante la inserción: {ex.Message}");
                    _logger.LogError(ex, "Error al insertar en B2_Amortizaciones.");
                    throw;
                }
            }
        }
    }
}
