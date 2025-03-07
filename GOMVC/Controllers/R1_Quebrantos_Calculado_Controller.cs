using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace YourNamespace.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class R1_Quebrantos_Calculados_Controller : Controller
    {
        private readonly ILogger<R1_Quebrantos_Calculados_Controller> _logger;
        private readonly IConfiguration _configuration;
        private readonly string _connectionString;
        // Rutas de archivos
        private readonly string _logsFolder = @"C:\Users\Go Credit\Documents\DATA\LOGS";
        private readonly string _exportFolderPath = @"C:\Users\Go Credit\Documents\DATA\EXPORTS";

        public R1_Quebrantos_Calculados_Controller(ILogger<R1_Quebrantos_Calculados_Controller> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        }

        [HttpPost("ProcessAndExport")]
        public async Task<IActionResult> ProcessAndExport()
        {
            var logBuilder = new StringBuilder();
            var logPath = Path.Combine(_logsFolder, "R1_Quebrantos_Calculados.log");
            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - R1 process started: Calculations & Export.");
            _logger.LogInformation("R1 process started: Calculations & Export.");

            try
            {
                // Ejecutar cálculos e inserción en R1_Quebrantos_Calculado
                await CalculateQuebrantos(logBuilder);
                logBuilder.AppendLine("Quebrantos calculations completed successfully.");
                _logger.LogInformation("Quebrantos calculations completed successfully.");

                // Ejecutar exportación a CSV
                logBuilder.AppendLine("Starting export to CSV...");
                _logger.LogInformation("Starting export to CSV...");
                var exportResult = await ExportQuebrantosToCSV();
                logBuilder.AppendLine($"Export result: {exportResult}");
                _logger.LogInformation($"Export result: {exportResult}");

                logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - R1 process completed successfully.");
                _logger.LogInformation("R1 process completed successfully.");

                await WriteLog(logBuilder.ToString(), logPath);

                return Ok("Quebrantos calculations and export completed successfully.");
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error during process execution: {ex.Message}");
                _logger.LogError(ex, "Error in R1 Quebrantos Calculados process.");
                await WriteLog(logBuilder.ToString(), logPath);
                return StatusCode(500, "An error occurred while processing R1 Quebrantos Calculados.");
            }
        }

        private async Task CalculateQuebrantos(StringBuilder logBuilder)
        {
            var sqlCommands = @"
                DROP TEMPORARY TABLE IF EXISTS Temp_Latest_Quebrantos;
                CREATE TEMPORARY TABLE Temp_Latest_Quebrantos (
                    Operacion INT,
                    Referencia INT,
                    Nombre VARCHAR(255),
                    Convenio VARCHAR(255),
                    vFinancingtypeid VARCHAR(50),
                    KVigente DECIMAL(10,2),
                    KVencido DECIMAL(10,2),
                    IntVencido DECIMAL(10,2),
                    IVAIntVencido DECIMAL(10,2),
                    IntVencidoCO DECIMAL(10,2),
                    IVAIntVencidoCO DECIMAL(10,2),
                    TotalQuebranto DECIMAL(10,2),
                    PagosRealizados DECIMAL(10,2),
                    SdoPendiente DECIMAL(10,2),
                    IntXDevengar DECIMAL(10,2),
                    SdoTotalXPagar DECIMAL(10,2),
                    FechaQuebranto VARCHAR(10),
                    UltPagoTeorico VARCHAR(10),
                    UltimoPago VARCHAR(10),
                    UltPagoApl VARCHAR(10),
                    Gestor VARCHAR(255),
                    nCommission DECIMAL(10,2),
                    nCommTax DECIMAL(10,2),
                    vMotive VARCHAR(255),
                    INDEX idx_operacion (Operacion)
                ) ENGINE=InnoDB;

                INSERT INTO Temp_Latest_Quebrantos
                SELECT 
                    q.Operacion,
                    q.Referencia,
                    q.Nombre,
                    q.Convenio,
                    q.vFinancingtypeid,
                    q.KVigente,
                    q.KVencido,
                    q.IntVencido,
                    q.IVAIntVencido,
                    q.IntVencidoCO,
                    q.IVAIntVencidoCO,
                    q.TotalQuebranto,
                    q.PagosRealizados,
                    q.SdoPendiente,
                    q.IntXDevengar,
                    q.SdoTotalXPagar,
                    q.FechaQuebranto,
                    q.UltPagoTeorico,
                    q.UltimoPago,
                    q.UltPagoApl,
                    q.Gestor,
                    q.nCommission,
                    q.nCommTax,
                    q.vMotive
                FROM D6_Quebrantos q
                JOIN (
                    SELECT Operacion, MAX(FechaGenerado) AS MostRecentFechaGenerado
                    FROM D6_Quebrantos
                    GROUP BY Operacion
                ) lfg 
                  ON q.Operacion = lfg.Operacion 
                  AND q.FechaGenerado = lfg.MostRecentFechaGenerado;

                DROP TEMPORARY TABLE IF EXISTS Temp_Total_Estrategias;
                CREATE TEMPORARY TABLE Temp_Total_Estrategias (
                    id_credito INT NOT NULL,
                    total_pagos DECIMAL(18,2) NOT NULL DEFAULT 0
                );
                INSERT INTO Temp_Total_Estrategias (id_credito, total_pagos)
                SELECT 
                    d.id_credito,
                    COALESCE(SUM(d.pago), 0) AS total_pagos
                FROM d3_aplicacion_pagos d
                INNER JOIN ci1_pagos_estrategia_acumulados c ON d.id_pago = c.id_pago
                GROUP BY d.id_credito;

                INSERT INTO R1_Quebrantos_Calculado (
                    Operacion, 
                    Referencia, 
                    Nombre, 
                    Convenio, 
                    vFinancing_typeid, 
                    K_Vigente, 
                    K_Vencido, 
                    Int_Vencido, 
                    IVA_Int_Vencido, 
                    Int_Vencido_CO, 
                    IVA_Int_Vencido_CO, 
                    Total_Quebranto, 
                    Pagos_Realizados, 
                    Sdo_Pendiente, 
                    Int_X_Devengar, 
                    Sdo_Total_X_Pagar, 
                    Fecha_Quebranto, 
                    Ult_PagoTeorico, 
                    Ultimo_Pago, 
                    Ult_Pago_Apl, 
                    Gestor, 
                    nCommission, 
                    nCommTax, 
                    v_Motive, 
                    Total_Estrategia, 
                    Recuperacion, 
                    Quebranto_Pagare, 
                    Saldo_Q_Pagare, 
                    Quebranto_Capital, 
                    Saldo_Q_Capital, 
                    Quebranto_Contable, 
                    Saldo_Q_Contable, 
                    Motivo, 
                    Valid, 
                    Month, 
                    Year, 
                    Producto, 
                    Financiamiento, 
                    Fecha_Generado
                )
                SELECT 
                    t.Operacion,
                    t.Referencia,
                    t.Nombre,
                    t.Convenio,
                    t.vFinancingtypeid AS vFinancing_typeid,
                    t.KVigente AS K_Vigente,
                    t.KVencido AS K_Vencido,
                    t.IntVencido AS Int_Vencido,
                    t.IVAIntVencido AS IVA_Int_Vencido,
                    t.IntVencidoCO AS Int_Vencido_CO,
                    t.IVAIntVencidoCO AS IVA_Int_Vencido_CO,
                    t.TotalQuebranto AS Total_Quebranto,
                    t.PagosRealizados AS PagosRealizados,
                    t.SdoPendiente AS Sdo_Pendiente,
                    t.IntXDevengar AS Int_X_Devengar,
                    t.SdoTotalXPagar AS Sdo_Total_X_Pagar,
                    t.FechaQuebranto AS Fecha_Quebranto,
                    t.UltPagoTeorico AS Ult_PagoTeorico,
                    t.UltimoPago AS Ultimo_Pago,
                    t.UltPagoApl AS Ult_Pago_Apl,
                    t.Gestor,
                    t.nCommission,
                    t.nCommTax,
                    t.vMotive AS v_Motive,
                    COALESCE(te.total_pagos, 0) AS Total_Estrategia,
                    COALESCE(t.PagosRealizados, 0) - COALESCE(te.total_pagos, 0) AS Recuperacion,
                    (COALESCE(t.SdoTotalXPagar, 0) + COALESCE(te.total_pagos, 0))
                      + (COALESCE(t.PagosRealizados, 0) - COALESCE(te.total_pagos, 0)) AS Quebranto_Pagare,
                    COALESCE(t.SdoTotalXPagar, 0) + COALESCE(te.total_pagos, 0) AS Saldo_Q_Pagare,
                    COALESCE(t.KVigente, 0) + COALESCE(t.KVencido, 0) AS Quebranto_Capital,
                    (COALESCE(t.KVigente, 0) + COALESCE(t.KVencido, 0))
                      - (COALESCE(t.PagosRealizados, 0) - COALESCE(te.total_pagos, 0)) AS Saldo_Q_Capital,
                    (COALESCE(t.KVigente, 0) + COALESCE(t.KVencido, 0)
                      + COALESCE(t.IntVencido, 0) + COALESCE(t.IVAIntVencido, 0)) AS Quebranto_Contable,
                    (COALESCE(t.KVigente, 0) + COALESCE(t.KVencido, 0)
                      + COALESCE(t.IntVencido, 0) + COALESCE(t.IVAIntVencido, 0))
                      - (COALESCE(t.PagosRealizados, 0) - COALESCE(te.total_pagos, 0)) AS Saldo_Q_Contable,
                    NULL AS Motivo,
                    COUNT(*) OVER(PARTITION BY t.Operacion) AS Valid,
                    MONTH(STR_TO_DATE(t.FechaQuebranto, '%Y-%m-%d')) AS Month,
                    YEAR(STR_TO_DATE(t.FechaQuebranto, '%Y-%m-%d')) AS Year,
                    COALESCE(cf.producto, 'Desconocido') AS Producto,
                    t.vFinancingtypeid AS Financiamiento,
                    NOW() AS Fecha_Generado
                FROM Temp_Latest_Quebrantos t
                LEFT JOIN Temp_Total_Estrategias te ON t.Operacion = te.id_credito
                LEFT JOIN c2_financiamiento cf ON t.vFinancingtypeid = cf.Tipo_Financiamiento;
            ";

            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = await connection.BeginTransactionAsync())
                {
                    try
                    {
                        var command = new MySqlCommand(sqlCommands, connection, transaction);
                        await command.ExecuteNonQueryAsync();
                        logBuilder.AppendLine("Quebrantos calculations completed successfully inside SQL transaction.");
                        _logger.LogInformation("Quebrantos calculations executed and committed successfully.");
                        await transaction.CommitAsync();
                    }
                    catch (Exception ex)
                    {
                        await transaction.RollbackAsync();
                        logBuilder.AppendLine($"Error during Quebrantos calculations: {ex.Message}");
                        _logger.LogError(ex, "Error during Quebrantos calculations.");
                        throw;
                    }
                }
            }
        }

        private async Task<string> ExportQuebrantosToCSV()
        {
            var mostRecentDateQuery = "SELECT MAX(Fecha_Generado) FROM R1_Quebrantos_Calculado;";
            try
            {
                if (!Directory.Exists(_exportFolderPath))
                {
                    Directory.CreateDirectory(_exportFolderPath);
                }

                using (var connection = new MySqlConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    DateTime mostRecentDate;
                    using (var cmd = new MySqlCommand(mostRecentDateQuery, connection))
                    {
                        var result = await cmd.ExecuteScalarAsync();
                        if (result == null || result == DBNull.Value)
                        {
                            _logger.LogWarning("No data available for export in R1_Quebrantos_Calculado.");
                            return "No data available for export.";
                        }
                        mostRecentDate = Convert.ToDateTime(result);
                    }

                    _logger.LogInformation($"Exporting data for most recent Fecha_Generado: {mostRecentDate:yyyy-MM-dd HH:mm:ss}");

                    var sqlQuery = @"
                        SELECT * 
                        FROM R1_Quebrantos_Calculado
                        WHERE Fecha_Generado = @MostRecentDate;";

                    using (var command = new MySqlCommand(sqlQuery, connection))
                    {
                        command.Parameters.AddWithValue("@MostRecentDate", mostRecentDate);
                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            if (!reader.HasRows)
                            {
                                _logger.LogWarning("No records found for the most recent date for export.");
                                return "No records to export.";
                            }

                            var exportFilePath = Path.Combine(_exportFolderPath, $"Quebrantos_Export_{mostRecentDate:yyyyMMdd_HHmmss}.csv");
                            using (var writer = new StreamWriter(exportFilePath, false, new UTF8Encoding(true)))
                            {
                                // Escribir encabezado
                                var columnNames = new string[reader.FieldCount];
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    columnNames[i] = reader.GetName(i);
                                }
                                await writer.WriteLineAsync(string.Join(",", columnNames));

                                // Escribir filas
                                while (await reader.ReadAsync())
                                {
                                    var row = new string[reader.FieldCount];
                                    for (int i = 0; i < reader.FieldCount; i++)
                                    {
                                        var value = reader[i]?.ToString() ?? "";
                                        value = value.Replace("\"", "\"\"");
                                        row[i] = $"\"{value}\"";
                                    }
                                    await writer.WriteLineAsync(string.Join(",", row));
                                }
                            }
                            _logger.LogInformation($"Export successful: {exportFilePath}");
                            return $"Export successful: {exportFilePath}";
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error exporting R1_Quebrantos_Calculado.");
                return $"Error exporting data: {ex.Message}";
            }
        }

        private async Task WriteLog(string content, string logPath)
        {
            try
            {
                var directory = Path.GetDirectoryName(logPath);
                if (!Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory!);
                }
                await System.IO.File.WriteAllTextAsync(logPath, content);
                _logger.LogInformation($"Log written to: {logPath}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error writing log to {logPath}");
                throw;
            }
        }
    }
}
