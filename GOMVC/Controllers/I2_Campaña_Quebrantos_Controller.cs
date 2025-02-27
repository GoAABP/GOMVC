using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

public class I2_Campaña_Quebrantos_Controller : Controller
{
    private readonly ILogger<I2_Campaña_Quebrantos_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    // Actualizamos la ubicación de exportación.
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\EXPORTS";

    public I2_Campaña_Quebrantos_Controller(ILogger<I2_Campaña_Quebrantos_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
    }

    [HttpPost]
    public async Task<IActionResult> Process()
    {
        try
        {
            // Ejecuta el upsert de registros
            await UpsertCampañaQuebrantosAsync();

            // Procesa eventos de actualización según el trimestre actual (4 trimestres)
            await ProcessCurrentQuarterEventsAsync();

            // Exporta los registros del trimestre actual a CSV
            string exportResult = await I2_ExportCurrentQuarterAsync();
            _logger.LogInformation(exportResult);

            return Ok("Process completed successfully.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing upsert, quarter events or export.");
            return StatusCode(500, "Error processing campaign data.");
        }
    }

    private async Task UpsertCampañaQuebrantosAsync()
    {
        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        string query = @"
INSERT INTO I2_Campaña_Quebrantos 
  (Id_Credito, Id_Persona, Nombre_Cliente, Quebranto_Contable, Saldo_Q_Pagare, Etapa_Procesal, vFinancing_typeid)
SELECT DISTINCT
    r.Operacion AS Id_Credito,
    d1.Id_Persona,
    r.Nombre AS Nombre_Cliente,
    r.Quebranto_Contable,
    r.Saldo_Q_Pagare,
    j.Etapa_Procesal,
    r.vFinancing_typeid
FROM r1_quebrantos_calculado r
INNER JOIN D1_Saldos_Cartera d1 
    ON d1.Id_Credito = r.Operacion
LEFT JOIN (
    SELECT j1.Credito_MC, j1.Etapa_Procesal
    FROM d7_juicios j1
    INNER JOIN (
        SELECT Credito_MC, MAX(Fecha_Actualizacion) AS max_fecha
        FROM d7_juicios
        GROUP BY Credito_MC
    ) latest 
      ON j1.Credito_MC = latest.Credito_MC
         AND j1.Fecha_Actualizacion = latest.max_fecha
) j ON j.Credito_MC = r.Operacion
WHERE r.Fecha_Generado = (SELECT MAX(Fecha_Generado) FROM r1_quebrantos_calculado)
  AND r.Quebranto_Contable > 3000
  AND r.Saldo_Q_Pagare > 5000
  AND j.Etapa_Procesal NOT IN ('Embargo de bienes', 'ejecución sentencia', 'remate', 'bienes embargados', 'amparo')
  AND r.vFinancing_typeid LIKE '%ELEC%'
ON DUPLICATE KEY UPDATE
  Quebranto_Contable = VALUES(Quebranto_Contable),
  Saldo_Q_Pagare = VALUES(Saldo_Q_Pagare),
  Etapa_Procesal = VALUES(Etapa_Procesal);";

        using var command = new MySqlCommand(query, connection)
        {
            CommandTimeout = 120
        };

        int rowsAffected = await command.ExecuteNonQueryAsync();
        _logger.LogInformation($"Upsert completed. Rows affected: {rowsAffected}");
    }

    /// <summary>
    /// Procesa eventos según el trimestre actual, dividiendo el año en 4 periodos:
    ///  - Primer trimestre: meses 1 a 3
    ///  - Segundo trimestre: meses 4 a 6
    ///  - Tercer trimestre: meses 7 a 9
    ///  - Cuarto trimestre: meses 10 a 12
    /// </summary>
    private async Task ProcessCurrentQuarterEventsAsync()
    {
        int currentMonth = DateTime.Now.Month;
        _logger.LogInformation($"Processing events for month: {currentMonth}");

        if (currentMonth >= 1 && currentMonth <= 3)
        {
            await ProcessFirstQuarterEventsAsync();
        }
        else if (currentMonth >= 4 && currentMonth <= 6)
        {
            await ProcessSecondQuarterEventsAsync();
        }
        else if (currentMonth >= 7 && currentMonth <= 9)
        {
            await ProcessThirdQuarterEventsAsync();
        }
        else if (currentMonth >= 10 && currentMonth <= 12)
        {
            await ProcessFourthQuarterEventsAsync();
        }
    }

    private async Task ProcessFirstQuarterEventsAsync()
    {
        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        string currentPeriodo = $"{DateTime.Now.Year}-01-01";

        string checkQuery = "SELECT COUNT(*) FROM I2_Campaña_Quebrantos WHERE Periodo = @currentPeriodo;";
        using (var checkCmd = new MySqlCommand(checkQuery, connection))
        {
            checkCmd.Parameters.AddWithValue("@currentPeriodo", currentPeriodo);
            long existingCount = Convert.ToInt64(await checkCmd.ExecuteScalarAsync());
            if (existingCount > 0)
            {
                _logger.LogInformation($"There are already {existingCount} records with Periodo = {currentPeriodo}. No update performed.");
                return;
            }
        }

        string countQuery = @"
            SELECT COUNT(DISTINCT Id_Persona) 
            FROM I2_Campaña_Quebrantos
            WHERE Periodo IS NULL OR YEAR(Periodo) != YEAR(CURDATE());";
        long totalDistinctPersons;
        using (var countCmd = new MySqlCommand(countQuery, connection))
        {
            totalDistinctPersons = Convert.ToInt64(await countCmd.ExecuteScalarAsync());
        }
        if (totalDistinctPersons == 0)
        {
            _logger.LogWarning("No matching distinct persons found for first quarter events.");
            return;
        }

        long limitValue = (long)Math.Ceiling(totalDistinctPersons / 3.0);
        _logger.LogInformation($"First quarter: {totalDistinctPersons} distinct persons, updating {limitValue} persons.");

        string updateQuery = @"
UPDATE I2_Campaña_Quebrantos
SET 
    Periodo = @currentPeriodo,
    Fecha_Generado = CURDATE()
WHERE Id_Persona IN (
    SELECT Id_Persona FROM (
         SELECT DISTINCT Id_Persona
         FROM I2_Campaña_Quebrantos
         WHERE Periodo IS NULL OR YEAR(Periodo) != YEAR(CURDATE())
         ORDER BY RAND()
         LIMIT @limitValue
    ) AS t
);";

        using var updateCmd = new MySqlCommand(updateQuery, connection)
        {
            CommandTimeout = 60
        };
        updateCmd.Parameters.AddWithValue("@limitValue", limitValue);
        updateCmd.Parameters.AddWithValue("@currentPeriodo", currentPeriodo);
        int updatedRows = await updateCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"First quarter event processed. Rows updated: {updatedRows}");
    }

    private async Task ProcessSecondQuarterEventsAsync()
    {
        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        string currentPeriodo = $"{DateTime.Now.Year}-04-01";

        string checkQuery = "SELECT COUNT(*) FROM I2_Campaña_Quebrantos WHERE Periodo = @currentPeriodo;";
        using (var checkCmd = new MySqlCommand(checkQuery, connection))
        {
            checkCmd.Parameters.AddWithValue("@currentPeriodo", currentPeriodo);
            long existingCount = Convert.ToInt64(await checkCmd.ExecuteScalarAsync());
            if (existingCount > 0)
            {
                _logger.LogInformation($"There are already {existingCount} records with Periodo = {currentPeriodo}. No update performed.");
                return;
            }
        }

        string countQuery = @"
            SELECT COUNT(DISTINCT Id_Persona)
            FROM I2_Campaña_Quebrantos
            WHERE Periodo IS NULL OR YEAR(Periodo) != YEAR(CURDATE());";
        long totalDistinctPersons;
        using (var countCmd = new MySqlCommand(countQuery, connection))
        {
            totalDistinctPersons = Convert.ToInt64(await countCmd.ExecuteScalarAsync());
        }
        if (totalDistinctPersons == 0)
        {
            _logger.LogWarning("No matching distinct persons found for second quarter events.");
            return;
        }

        long limitValue = (long)Math.Ceiling(totalDistinctPersons / 2.0);
        _logger.LogInformation($"Second quarter: {totalDistinctPersons} distinct persons, updating {limitValue} persons.");

        string updateQuery = @"
UPDATE I2_Campaña_Quebrantos
SET 
    Periodo = @currentPeriodo,
    Fecha_Generado = CURDATE()
WHERE Id_Persona IN (
    SELECT Id_Persona FROM (
         SELECT DISTINCT Id_Persona
         FROM I2_Campaña_Quebrantos
         WHERE Periodo IS NULL OR YEAR(Periodo) != YEAR(CURDATE())
         ORDER BY RAND()
         LIMIT @limitValue
    ) AS t
);";

        using var updateCmd = new MySqlCommand(updateQuery, connection)
        {
            CommandTimeout = 60
        };
        updateCmd.Parameters.AddWithValue("@limitValue", limitValue);
        updateCmd.Parameters.AddWithValue("@currentPeriodo", currentPeriodo);
        int updatedRows = await updateCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Second quarter event processed. Rows updated: {updatedRows}");
    }

    private async Task ProcessThirdQuarterEventsAsync()
    {
        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        string currentPeriodo = $"{DateTime.Now.Year}-07-01";

        string checkQuery = "SELECT COUNT(*) FROM I2_Campaña_Quebrantos WHERE Periodo = @currentPeriodo;";
        using (var checkCmd = new MySqlCommand(checkQuery, connection))
        {
            checkCmd.Parameters.AddWithValue("@currentPeriodo", currentPeriodo);
            long existingCount = Convert.ToInt64(await checkCmd.ExecuteScalarAsync());
            if (existingCount > 0)
            {
                _logger.LogInformation($"There are already {existingCount} records with Periodo = {currentPeriodo}. No update performed.");
                return;
            }
        }

        string updateQuery = @"
UPDATE I2_Campaña_Quebrantos
SET 
    Periodo = @currentPeriodo,
    Fecha_Generado = CURDATE()
WHERE Id_Persona IN (
    SELECT Id_Persona FROM (
         SELECT DISTINCT Id_Persona
         FROM I2_Campaña_Quebrantos
         WHERE Periodo IS NULL OR YEAR(Periodo) != YEAR(CURDATE())
    ) AS t
);";

        using var updateCmd = new MySqlCommand(updateQuery, connection)
        {
            CommandTimeout = 60
        };
        updateCmd.Parameters.AddWithValue("@currentPeriodo", currentPeriodo);
        int updatedRows = await updateCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Third quarter event processed. Rows updated: {updatedRows}");
    }

    private async Task ProcessFourthQuarterEventsAsync()
    {
        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        string currentPeriodo = $"{DateTime.Now.Year}-10-01";

        string checkQuery = "SELECT COUNT(*) FROM I2_Campaña_Quebrantos WHERE Periodo = @currentPeriodo;";
        using (var checkCmd = new MySqlCommand(checkQuery, connection))
        {
            checkCmd.Parameters.AddWithValue("@currentPeriodo", currentPeriodo);
            long existingCount = Convert.ToInt64(await checkCmd.ExecuteScalarAsync());
            if (existingCount > 0)
            {
                _logger.LogInformation($"There are already {existingCount} records with Periodo = {currentPeriodo}. No update performed.");
                return;
            }
        }

        string updateQuery = @"
UPDATE I2_Campaña_Quebrantos
SET 
    Periodo = @currentPeriodo,
    Fecha_Generado = CURDATE()
WHERE Id_Persona IN (
    SELECT Id_Persona FROM (
         SELECT DISTINCT Id_Persona
         FROM I2_Campaña_Quebrantos
         WHERE Periodo IS NULL OR YEAR(Periodo) != YEAR(CURDATE())
    ) AS t
);";

        using var updateCmd = new MySqlCommand(updateQuery, connection)
        {
            CommandTimeout = 60
        };
        updateCmd.Parameters.AddWithValue("@currentPeriodo", currentPeriodo);
        int updatedRows = await updateCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Fourth quarter event processed. Rows updated: {updatedRows}");
    }

    private async Task<string> I2_ExportCurrentQuarterAsync()
    {
        // Exporta los registros del trimestre actual a CSV.
        int currentMonth = DateTime.Now.Month;
        string exportPath = System.IO.Path.Combine(_filePath, "I2_Campaña_Quebrantos_Export.csv");

        int startMonth, endMonth;
        if (currentMonth >= 1 && currentMonth <= 3)
        {
            startMonth = 1; endMonth = 3;
        }
        else if (currentMonth >= 4 && currentMonth <= 6)
        {
            startMonth = 4; endMonth = 6;
        }
        else if (currentMonth >= 7 && currentMonth <= 9)
        {
            startMonth = 7; endMonth = 9;
        }
        else // 10 a 12
        {
            startMonth = 10; endMonth = 12;
        }

        string exportQuery = $@"
            SELECT * 
            FROM I2_Campaña_Quebrantos
            WHERE YEAR(Periodo) = YEAR(CURDATE())
              AND MONTH(Periodo) BETWEEN {startMonth} AND {endMonth};";

        return await I2_ExportToCSVAsync(exportQuery, exportPath);
    }

    private async Task<string> I2_ExportToCSVAsync(string query, string filePath)
    {
        try
        {
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand(query, connection))
                using (var reader = await command.ExecuteReaderAsync())
                {
                    var sb = new StringBuilder();
                    var columnNames = Enumerable.Range(0, reader.FieldCount)
                                                .Select(reader.GetName);
                    sb.AppendLine(string.Join(",", columnNames));

                    while (await reader.ReadAsync())
                    {
                        var values = Enumerable.Range(0, reader.FieldCount)
                            .Select(i => reader.IsDBNull(i) ? "" : $"\"{reader.GetValue(i).ToString().Replace("\"", "\"\"")}\"");
                        sb.AppendLine(string.Join(",", values));
                    }

                    await System.IO.File.WriteAllTextAsync(filePath, sb.ToString(), Encoding.UTF8);
                }
            }
            _logger.LogInformation($"Export completed successfully: {filePath}");
            return $"Export completed successfully: {filePath}";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during export.");
            return $"Error exporting data: {ex.Message}";
        }
    }
}
