using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

public class R1_Quebrantos_Calculado_Controller : Controller
{
    private readonly ILogger<R1_Quebrantos_Calculado_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;

    // Directorios base (ajústelos según convenga)
    private readonly string _exportFolder = @"C:\Users\Go Credit\Documents\DATA\EXPORTS";
    private readonly string _logsFolder = @"C:\Users\Go Credit\Documents\DATA\LOGS";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

    // Fecha de proceso hardcodeada (se puede modificar a conveniencia)
    private DateTime _processDate = new DateTime(2025, 3, 1);

    public R1_Quebrantos_Calculado_Controller(ILogger<R1_Quebrantos_Calculado_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")
                            ?? throw new ArgumentNullException("DefaultConnection");
        // Se registra el proveedor de codificación, tal como en D6
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
    }

    // Proceso normal: utiliza la fecha generada (usada en D6)
    [HttpPost]
    public async Task<IActionResult> R1_ProcessQuebrantosCalculationsAndExport()
    {
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Proceso de cálculo y exportación iniciado (normal).");
        _logger.LogInformation("Proceso de cálculo y exportación iniciado (normal).");

        try
        {
            // Paso 1: Realizar el cálculo de quebrantos (imitando D6_CalculateQuebrantos)
            logBuilder.AppendLine("Ejecutando cálculos de quebrantos...");
            await R1_CalculateQuebrantos(logBuilder);
            logBuilder.AppendLine("Cálculos de quebrantos completados.");

            // Paso 2: Exportar los datos a CSV usando la fecha más reciente en la tabla R1_Quebrantos_Calculado
            logBuilder.AppendLine("Ejecutando exportación a CSV...");
            var exportResult = await R1_ExportQuebrantosToCSV();
            logBuilder.AppendLine($"Resultado de exportación: {exportResult}");

            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Proceso completado exitosamente (normal).");
            _logger.LogInformation("Proceso de cálculo y exportación completado (normal).");
            return Ok(logBuilder.ToString());
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error en el proceso (normal): {ex.Message}");
            _logger.LogError(ex, "Error en el proceso de cálculo y exportación (normal).");
            return StatusCode(500, logBuilder.ToString());
        }
    }

    // Proceso con fecha hardcodeada: utiliza _processDate para filtrar la exportación
    [HttpPost("processHardcoded")]
    public async Task<IActionResult> R1_ProcessQuebrantosCalculationsAndExport_Hardcoded()
    {
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Proceso de cálculo y exportación iniciado (fecha hardcodeada: {_processDate:yyyy-MM-dd}).");
        _logger.LogInformation("Proceso de cálculo y exportación iniciado (con fecha hardcodeada).");

        try
        {
            // Paso 1: Realizar el cálculo de quebrantos
            logBuilder.AppendLine("Ejecutando cálculos de quebrantos...");
            await R1_CalculateQuebrantos(logBuilder);
            logBuilder.AppendLine("Cálculos de quebrantos completados.");

            // Paso 2: Exportar los datos a CSV usando la fecha hardcodeada
            logBuilder.AppendLine("Ejecutando exportación a CSV (hardcodeada)...");
            var exportResult = await R1_ExportQuebrantosToCSV_Hardcoded();
            logBuilder.AppendLine($"Resultado de exportación: {exportResult}");

            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Proceso completado exitosamente (fecha hardcodeada).");
            _logger.LogInformation("Proceso de cálculo y exportación completado (con fecha hardcodeada).");
            return Ok(logBuilder.ToString());
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error en el proceso (hardcodeada): {ex.Message}");
            _logger.LogError(ex, "Error en el proceso de cálculo y exportación (fecha hardcodeada).");
            return StatusCode(500, logBuilder.ToString());
        }
    }

    // Método que imita D6_CalculateQuebrantos
    private async Task R1_CalculateQuebrantos(StringBuilder logBuilder)
    {
        var sqlCommands = @"
            -- Crear tabla temporal para la información más reciente de cada operación
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

            -- Crear tabla temporal para el total de estrategias
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

            -- Insertar en la tabla final (R1_Quebrantos_Calculado) con los cálculos correspondientes
            INSERT INTO R1_Quebrantos_Calculado (
                Operacion, Referencia, Nombre, Convenio, vFinancing_typeid, 
                K_Vigente, K_Vencido, Int_Vencido, IVA_Int_Vencido, 
                Int_Vencido_CO, IVA_Int_Vencido_CO, Total_Quebranto, 
                PagosRealizados, Sdo_Pendiente, Int_X_Devengar, Sdo_Total_X_Pagar, 
                Fecha_Quebranto, Ult_Pago_Teorico, Ultimo_Pago, Ult_Pago_Apl, 
                Gestor, nCommission, nCommTax, v_Motive, Total_Estrategia, 
                Recuperacion, Quebranto_Pagare, Saldo_Q_Pagare, Quebranto_Capital, 
                Saldo_Q_Capital, Quebranto_Contable, Saldo_Q_Contable, 
                Motivo, Valid, Month, Year, Producto, Financiamiento, Fecha_Generado
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
                t.SdoPendiente AS SdoPendiente,
                t.IntXDevengar AS Int_X_Devengar,
                t.SdoTotalXPagar AS Sdo_Total_X_Pagar,
                t.FechaQuebranto AS Fecha_Quebranto,
                t.UltPagoTeorico AS Ult_Pago_Teorico,
                t.UltimoPago AS UltimoPago,
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
                    await transaction.CommitAsync();
                    logBuilder.AppendLine("Cálculos de quebrantos ejecutados con éxito.");
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error durante los cálculos: {ex.Message}");
                    _logger.LogError(ex, "Error en el cálculo de quebrantos.");
                    throw;
                }
            }
        }
    }

    // Exportación normal: se consulta la fecha más reciente de la tabla R1_Quebrantos_Calculado
    private async Task<string> R1_ExportQuebrantosToCSV()
    {
        var mostRecentDateQuery = "SELECT MAX(Fecha_Generado) FROM R1_Quebrantos_Calculado;";
        var logBuilder = new StringBuilder();

        try
        {
            if (!Directory.Exists(_exportFolder))
            {
                Directory.CreateDirectory(_exportFolder);
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
                        logBuilder.AppendLine("No se encontraron datos en R1_Quebrantos_Calculado.");
                        return "No data available for export.";
                    }
                    mostRecentDate = Convert.ToDateTime(result);
                }

                logBuilder.AppendLine($"Exportando datos de Fecha_Generado: {mostRecentDate:yyyy-MM-dd HH:mm:ss}");

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
                            logBuilder.AppendLine("No se encontraron registros para la fecha más reciente.");
                            return "No records to export.";
                        }

                        var exportFilePath = Path.Combine(_exportFolder, $"Quebrantos_Export_{mostRecentDate:yyyyMMdd_HHmmss}.csv");

                        using (var writer = new StreamWriter(exportFilePath, false, new UTF8Encoding(true)))
                        {
                            // Escribir cabecera
                            var columnNames = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName);
                            await writer.WriteLineAsync(string.Join(",", columnNames));

                            // Escribir filas
                            while (await reader.ReadAsync())
                            {
                                var row = new string[reader.FieldCount];
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    var value = reader[i]?.ToString() ?? "";
                                    value = value.Replace("\"", "\"\""); // Escapar comillas
                                    row[i] = $"\"{value}\"";
                                }
                                await writer.WriteLineAsync(string.Join(",", row));
                            }
                        }

                        logBuilder.AppendLine($"Exportación exitosa: {exportFilePath}");
                        return $"Exported successfully to: {exportFilePath}";
                    }
                }
            }
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error exportando datos: {ex.Message}");
            _logger.LogError(ex, "Error exportando R1_Quebrantos_Calculado.");
            throw;
        }
    }

    // Exportación con fecha hardcodeada: utiliza _processDate para filtrar los registros
    private async Task<string> R1_ExportQuebrantosToCSV_Hardcoded()
    {
        var logBuilder = new StringBuilder();

        try
        {
            if (!Directory.Exists(_exportFolder))
            {
                Directory.CreateDirectory(_exportFolder);
            }

            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();

                logBuilder.AppendLine($"Exportando datos para la fecha (hardcodeada): {_processDate:yyyy-MM-dd HH:mm:ss}");

                var sqlQuery = @"
                    SELECT * 
                    FROM R1_Quebrantos_Calculado
                    WHERE Fecha_Generado = @ProcessDate;";

                using (var command = new MySqlCommand(sqlQuery, connection))
                {
                    command.Parameters.AddWithValue("@ProcessDate", _processDate);

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (!reader.HasRows)
                        {
                            logBuilder.AppendLine("No se encontraron registros para la fecha indicada.");
                            return "No records to export.";
                        }

                        var exportFilePath = Path.Combine(_exportFolder, $"Quebrantos_Export_{_processDate:yyyyMMdd_HHmmss}.csv");

                        using (var writer = new StreamWriter(exportFilePath, false, new UTF8Encoding(true)))
                        {
                            // Escribir cabecera
                            var columnNames = Enumerable.Range(0, reader.FieldCount)
                                                        .Select(reader.GetName);
                            await writer.WriteLineAsync(string.Join(",", columnNames));

                            // Escribir filas
                            while (await reader.ReadAsync())
                            {
                                var row = new string[reader.FieldCount];
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    var value = reader[i]?.ToString() ?? "";
                                    value = value.Replace("\"", "\"\""); // Escapar comillas
                                    row[i] = $"\"{value}\"";
                                }
                                await writer.WriteLineAsync(string.Join(",", row));
                            }
                        }

                        logBuilder.AppendLine($"Exportación exitosa: {exportFilePath}");
                        return $"Exported successfully to: {exportFilePath}";
                    }
                }
            }
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error exportando datos (hardcodeada): {ex.Message}");
            _logger.LogError(ex, "Error exportando R1_Quebrantos_Calculado (fecha hardcodeada).");
            throw;
        }
    }
}
