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
    // Ruta para exportar el archivo CSV.
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\EXPORTS";

    public I2_Campaña_Quebrantos_Controller(ILogger<I2_Campaña_Quebrantos_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
    }

    // Usa la fecha actual del sistema.
    private DateTime GetCurrentDate()
    {
        return DateTime.Now;
    }

    [HttpPost]
    public async Task<IActionResult> Process()
    {
        try
        {
            // Paso 1: Actualizar registros existentes según Id_Credito.
            await UpdateExistingRecordsAsync();

            // Paso 2: Insertar nuevos registros (solo si no existe un registro con ese Id_Credito).
            await InsertNewRecordsAsync();

            // Otros procesos (por ejemplo, asignación de periodos y exportación)
            await ProcessMonthlyQuarterEventsAsync();
            string exportResult = await I2_ExportCurrentQuarterAsync();
            _logger.LogInformation(exportResult);

            return Ok("Process completed successfully.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing upsert, monthly events or export.");
            return StatusCode(500, "Error processing campaign data.");
        }
    }

    private async Task UpdateExistingRecordsAsync()
    {
        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        string updateQuery = @"
UPDATE I2_Campaña_Quebrantos t
JOIN (
    SELECT DISTINCT
         r.Operacion AS Id_Credito,
         r.Quebranto_Contable,
         r.Saldo_Q_Pagare,
         j.Etapa_Procesal,
         r.vFinancing_typeid
    FROM r1_quebrantos_calculado r
    INNER JOIN D1_Saldos_Cartera d1 ON d1.Id_Credito = r.Operacion
    LEFT JOIN (
         SELECT j1.Credito_MC, j1.Etapa_Procesal
         FROM d7_juicios j1
         INNER JOIN (
             SELECT Credito_MC, MAX(Fecha_Actualizacion) AS max_fecha
             FROM d7_juicios
             GROUP BY Credito_MC
         ) latest ON j1.Credito_MC = latest.Credito_MC
                   AND j1.Fecha_Actualizacion = latest.max_fecha
    ) j ON j.Credito_MC = r.Operacion
    WHERE r.Fecha_Generado = (SELECT MAX(Fecha_Generado) FROM r1_quebrantos_calculado)
      AND r.Quebranto_Contable > 3000
      AND r.Saldo_Q_Pagare > 5000
      AND UPPER(TRIM(j.Etapa_Procesal)) NOT IN ('EMBARGO DE BIENES','EJECUCION DE SENTENCIA','REMATE','BIENES EMBARGADOS','AMPARO','DEFUNCION','QUEBRANTO PAGADO')
      AND r.vFinancing_typeid LIKE '%ELEC%'
) src ON t.Id_Credito = src.Id_Credito
SET 
    t.Quebranto_Contable = src.Quebranto_Contable,
    t.Saldo_Q_Pagare = src.Saldo_Q_Pagare,
    t.Etapa_Procesal = src.Etapa_Procesal,
    t.vFinancing_typeid = src.vFinancing_typeid;";

        using var command = new MySqlCommand(updateQuery, connection)
        {
            CommandTimeout = 120
        };

        int updatedRows = await command.ExecuteNonQueryAsync();
        _logger.LogInformation($"UpdateExistingRecordsAsync: {updatedRows} registros actualizados.");
    }

    private async Task InsertNewRecordsAsync()
    {
        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        string insertQuery = @"
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
INNER JOIN D1_Saldos_Cartera d1 ON d1.Id_Credito = r.Operacion
LEFT JOIN (
    SELECT j1.Credito_MC, j1.Etapa_Procesal
    FROM d7_juicios j1
    INNER JOIN (
         SELECT Credito_MC, MAX(Fecha_Actualizacion) AS max_fecha
         FROM d7_juicios
         GROUP BY Credito_MC
    ) latest ON j1.Credito_MC = latest.Credito_MC
             AND j1.Fecha_Actualizacion = latest.max_fecha
) j ON j.Credito_MC = r.Operacion
WHERE r.Fecha_Generado = (SELECT MAX(Fecha_Generado) FROM r1_quebrantos_calculado)
  AND r.Quebranto_Contable > 3000
  AND r.Saldo_Q_Pagare > 5000
  AND UPPER(TRIM(j.Etapa_Procesal)) NOT IN ('EMBARGO DE BIENES','EJECUCION DE SENTENCIA','REMATE','BIENES EMBARGADOS','AMPARO','DEFUNCION','QUEBRANTO PAGADO')
  AND r.vFinancing_typeid LIKE '%ELEC%'
  AND NOT EXISTS (
      SELECT 1 FROM I2_Campaña_Quebrantos t WHERE t.Id_Credito = r.Operacion
  );";

        using var command = new MySqlCommand(insertQuery, connection)
        {
            CommandTimeout = 120
        };

        int insertedRows = await command.ExecuteNonQueryAsync();
        _logger.LogInformation($"InsertNewRecordsAsync: {insertedRows} registros insertados.");
    }

    /// <summary>
    /// Procesa los eventos mensuales para el trimestre actual.
    /// La asignación se realiza en tres fases sobre los Id_Persona que NO tengan registros asignados en el trimestre:
    /// - En el primer mes del trimestre se asigna 1/3 del total de Id_Persona elegibles.
    /// - En el segundo mes se asigna 1/2 de los Id_Persona restantes (ignorando los asignados en el primer mes).
    /// - En el tercer mes se asigna el resto.
    /// Al seleccionar un Id_Persona, se actualizan todas sus filas con el periodo actual.
    /// </summary>
    private async Task ProcessMonthlyQuarterEventsAsync()
    {
        // Usa la fecha actual del sistema.
        DateTime currentDate = GetCurrentDate();
        int currentMonth = currentDate.Month;
        int currentYear = currentDate.Year;

        // Determina el trimestre actual (por ejemplo, Q1: enero, febrero, marzo).
        int quarter = ((currentMonth - 1) / 3) + 1;
        // Obtiene el orden del mes dentro del trimestre: 
        // 1 para el primer mes, 2 para el segundo y 3 para el tercer mes.
        int monthOrder = currentMonth - ((quarter - 1) * 3);

        // Calcula el inicio y fin del trimestre.
        DateTime quarterStart = new DateTime(currentYear, ((quarter - 1) * 3) + 1, 1);
        DateTime quarterEnd = quarterStart.AddMonths(3).AddDays(-1);

        // Usa el primer día del mes actual como identificador del periodo.
        string currentPeriodo = $"{currentYear}-{currentMonth:00}-01";

        using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        // Evita actualizar si ya existen registros con el periodo actual.
        string checkQuery = "SELECT COUNT(*) FROM I2_Campaña_Quebrantos WHERE Periodo = @currentPeriodo;";
        using (var checkCmd = new MySqlCommand(checkQuery, connection))
        {
            checkCmd.Parameters.AddWithValue("@currentPeriodo", currentPeriodo);
            long existingCount = Convert.ToInt64(await checkCmd.ExecuteScalarAsync());
            if (existingCount > 0)
            {
                _logger.LogInformation($"Ya existen {existingCount} registros con Periodo = {currentPeriodo}. No se realiza actualización.");
                return;
            }
        }

        // Selecciona únicamente los Id_Persona que NO tengan ningún registro asignado en el trimestre actual.
        string countQuery = @"
SELECT COUNT(DISTINCT Id_Persona)
FROM I2_Campaña_Quebrantos
WHERE Id_Persona NOT IN (
    SELECT DISTINCT Id_Persona 
    FROM I2_Campaña_Quebrantos 
    WHERE Periodo IS NOT NULL AND Periodo BETWEEN @quarterStart AND @quarterEnd
);";
        long totalEligible;
        using (var countCmd = new MySqlCommand(countQuery, connection))
        {
            countCmd.Parameters.AddWithValue("@quarterStart", quarterStart);
            countCmd.Parameters.AddWithValue("@quarterEnd", quarterEnd);
            totalEligible = Convert.ToInt64(await countCmd.ExecuteScalarAsync());
        }
        if (totalEligible == 0)
        {
            _logger.LogWarning("No se encontraron Id_Persona elegibles para actualizar en el trimestre actual.");
            return;
        }

        long limitValue = 0;
        if (monthOrder == 1)
        {
            // En el primer mes del trimestre se asigna 1/3 del total de Id_Persona elegibles.
            limitValue = (long)Math.Ceiling(totalEligible / 3.0);
        }
        else if (monthOrder == 2)
        {
            // En el segundo mes se asigna 1/2 de los Id_Persona restantes (ignorando los asignados en el primer mes).
            string remainingQuery = @"
SELECT COUNT(DISTINCT Id_Persona)
FROM I2_Campaña_Quebrantos
WHERE Id_Persona NOT IN (
    SELECT DISTINCT Id_Persona 
    FROM I2_Campaña_Quebrantos 
    WHERE Periodo IS NOT NULL AND Periodo BETWEEN @quarterStart AND @quarterEnd
);";
            long remainingCount;
            using (var remainingCmd = new MySqlCommand(remainingQuery, connection))
            {
                remainingCmd.Parameters.AddWithValue("@quarterStart", quarterStart);
                remainingCmd.Parameters.AddWithValue("@quarterEnd", quarterEnd);
                remainingCount = Convert.ToInt64(await remainingCmd.ExecuteScalarAsync());
            }
            limitValue = (long)Math.Ceiling(remainingCount / 2.0);
        }
        else if (monthOrder == 3)
        {
            // En el tercer mes se asigna el resto de los Id_Persona restantes.
            string remainingQuery = @"
SELECT COUNT(DISTINCT Id_Persona)
FROM I2_Campaña_Quebrantos
WHERE Id_Persona NOT IN (
    SELECT DISTINCT Id_Persona 
    FROM I2_Campaña_Quebrantos 
    WHERE Periodo IS NOT NULL AND Periodo BETWEEN @quarterStart AND @quarterEnd
);";
            long remainingCount;
            using (var remainingCmd = new MySqlCommand(remainingQuery, connection))
            {
                remainingCmd.Parameters.AddWithValue("@quarterStart", quarterStart);
                remainingCmd.Parameters.AddWithValue("@quarterEnd", quarterEnd);
                remainingCount = Convert.ToInt64(await remainingCmd.ExecuteScalarAsync());
            }
            limitValue = remainingCount;
        }

        _logger.LogInformation($"Trimestre: {quarter}, Mes: {monthOrder} (Periodo: {currentPeriodo}), Id_Persona elegibles: {totalEligible}, Límite a actualizar: {limitValue}");

        // Actualiza todas las filas de los Id_Persona seleccionados aleatoriamente.
        string updateQuery = @"
UPDATE I2_Campaña_Quebrantos
SET 
    Periodo = @currentPeriodo,
    Fecha_Generado = CURDATE()
WHERE Id_Persona IN (
    SELECT Id_Persona FROM (
         SELECT DISTINCT Id_Persona
         FROM I2_Campaña_Quebrantos
         WHERE Id_Persona NOT IN (
               SELECT DISTINCT Id_Persona 
               FROM I2_Campaña_Quebrantos 
               WHERE Periodo IS NOT NULL AND Periodo BETWEEN @quarterStart AND @quarterEnd
         )
         ORDER BY RAND()
         LIMIT @limitValue
    ) AS t
);";

        using (var updateCmd = new MySqlCommand(updateQuery, connection)
        {
            CommandTimeout = 60
        })
        {
            updateCmd.Parameters.AddWithValue("@limitValue", limitValue);
            updateCmd.Parameters.AddWithValue("@currentPeriodo", currentPeriodo);
            updateCmd.Parameters.AddWithValue("@quarterStart", quarterStart);
            updateCmd.Parameters.AddWithValue("@quarterEnd", quarterEnd);

            int updatedRows = await updateCmd.ExecuteNonQueryAsync();
            _logger.LogInformation($"ProcessMonthlyQuarterEventsAsync: {updatedRows} registros actualizados.");
        }
    }

    private async Task<string> I2_ExportCurrentQuarterAsync()
    {
        // Usa la fecha actual para exportar.
        DateTime currentDate = GetCurrentDate();
        int currentMonth = currentDate.Month;
        int currentYear = currentDate.Year;

        // Se agrega el mes en el nombre del archivo.
        string exportPath = System.IO.Path.Combine(_filePath, $"I2_Campaña_Quebrantos_Export_{currentYear}_{currentMonth:00}.csv");

        // El archivo export solo incluirá registros cuyo Periodo corresponda al mes actual.
        string exportQuery = $@"
SELECT * 
FROM I2_Campaña_Quebrantos
WHERE YEAR(Periodo) = {currentYear}
  AND MONTH(Periodo) = {currentMonth};";

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
