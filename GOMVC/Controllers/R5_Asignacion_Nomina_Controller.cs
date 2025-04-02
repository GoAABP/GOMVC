using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GOMVC.Controllers
{
    public class R5_Asignacion_Nomina_Controller : Controller
    {
        private readonly ILogger<R5_Asignacion_Nomina_Controller> _logger;
        private readonly IConfiguration _configuration;
        private readonly string _connectionString;
        // Rutas configuradas para logs, exportación y archivos históricos
        private readonly string _logFolderPath = @"C:\Users\Go Credit\Documents\DATA\LOGS";
        private readonly string _historicLogsFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC LOGS";
        private readonly string _exportFolderPath = @"C:\Users\Go Credit\Documents\DATA\EXPORTS";

        public R5_Asignacion_Nomina_Controller(ILogger<R5_Asignacion_Nomina_Controller> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
        }

        /// <summary>
        /// Proceso de consolidación de datos para asignación de nómina (R5).
        /// Se crea o reemplaza la vista vw_asignacion_nomina y se insertan los datos en la tabla R5_Asignacion_Nomina.
        /// Además, se exporta a CSV los registros correspondientes a la fecha más reciente.
        /// </summary>
        [HttpPost]
        public async Task<IActionResult> R5_ProcessAsignacionNomina()
        {
            // Generar nombre del log basado en timestamp
            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var controllerName = nameof(R5_Asignacion_Nomina_Controller);
            var logFileName = $"{controllerName}_{timestamp}.log";
            // Mover logs previos a la carpeta histórica
            MoveExistingLogs(controllerName, _logFolderPath, _historicLogsFolder);

            var logBuilder = new StringBuilder();
            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Proceso R5 Asignacion Nomina iniciado.");
            _logger.LogInformation("Proceso R5 Asignacion Nomina iniciado.");

            try
            {
                // Consolidar e insertar datos en R5_Asignacion_Nomina
                int rowsAffected = await R5_InsertAsignacionNominaDataAsync(logBuilder);
                logBuilder.AppendLine($"[OK] - Proceso completado. Filas insertadas: {rowsAffected}");
                _logger.LogInformation($"Proceso completado. Filas insertadas: {rowsAffected}");

                // Llamada al proceso de exportación a CSV
                _logger.LogInformation("Iniciando exportación a CSV...");
                IActionResult exportResult = await R5_ExportInternalAsync(logBuilder);
                string exportMessage = exportResult is OkObjectResult okResult 
                    ? okResult.Value.ToString() 
                    : "[E207] - Exportación no generada.";
                logBuilder.AppendLine(exportMessage);
                _logger.LogInformation(exportMessage);

                await R5_WriteLogAsync(logBuilder.ToString(), logFileName);
                return Ok($"Proceso completado. Filas insertadas: {rowsAffected}. {exportMessage}");
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"[ERROR] - Error en el proceso: {ex.Message}");
                _logger.LogError(ex, "Error en el proceso R5 Asignacion Nomina.");
                await R5_WriteLogAsync(logBuilder.ToString(), logFileName);
                return StatusCode(500, "Error interno en el proceso R5 Asignacion Nomina.");
            }
        }

        /// <summary>
        /// Método que crea o reemplaza la vista vw_asignacion_nomina y consolida los datos insertándolos en R5_Asignacion_Nomina.
        /// </summary>
        private async Task<int> R5_InsertAsignacionNominaDataAsync(StringBuilder logBuilder)
        {
            var sql = @"
                CREATE OR REPLACE VIEW vw_asignacion_nomina AS
                SELECT 
                  D1.Id_Credito, 
                  D1.Id_Persona, 
                  D1.Referencia, 
                  D1.Afiliado, 
                  D1.Nombre, 
                  D1.Monto, 
                  D1.Pagos, 
                  D1.Dependencia, 
                  D1.Pago, 
                  D1.Estatus, 
                  D1.Fecha_Desembolso, 
                  D1.Frecuencia, 
                  D1.Dias_Atraso, 
                  CASE 
                    WHEN D1.Estatus = 'Activo' AND D1.Dias_Atraso = 0 THEN '0'
                    WHEN D1.Estatus = 'Activo' AND D1.Dias_Atraso BETWEEN 1 AND 30 THEN '1 a 30'
                    WHEN D1.Estatus = 'Activo' AND D1.Dias_Atraso BETWEEN 31 AND 60 THEN '31 a 60'
                    WHEN D1.Estatus = 'Activo' AND D1.Dias_Atraso BETWEEN 61 AND 90 THEN '61 a 90'
                    WHEN D1.Estatus = 'Activo' AND D1.Dias_Atraso BETWEEN 91 AND 120 THEN '91 a 120'
                    WHEN D1.Estatus = 'Activo' AND D1.Dias_Atraso BETWEEN 121 AND 150 THEN '121 a 150'
                    WHEN D1.Estatus = 'Activo' AND D1.Dias_Atraso BETWEEN 151 AND 180 THEN '151 a 180'
                    WHEN D1.Estatus = 'Activo' AND D1.Dias_Atraso >= 181 THEN '181+'
                    ELSE D1.Estatus
                  END AS Bucket,
                  D1.Vencido,
                  D1.Saldo_Total,
                  D1.Saldo_Capital,
                  D1.Motivo,
                  D1.vCollectStatus,
                  C2.Producto,
                  R1.Quebranto_Pagare, 
                  R1.Saldo_Q_Pagare, 
                  R1.Quebranto_Capital, 
                  R1.Saldo_Q_Capital, 
                  R1.Quebranto_Contable, 
                  R1.Saldo_Q_Contable,
                  CONCAT(B1.vStreetAd, ' ', B1.vExtNumberAd, ', ', B1.vNeighborhoodAd, ', ', B1.vTownshipAd, ', ', B1.vStateIdAd) AS Direccion_Cliente,
                  B1.vPhoneNumberAd,
                  B1.vMobile,
                  CONCAT(B1.vFirstNameRe1, ' ', B1.vSecondNameRe1, ' ', B1.vFLastNameRe1, ' ', B1.vSLastNameRe1) AS Nombre_Referencia1,
                  B1.vPhoneNumberRe1 AS Telefono_Referencia1,
                  CONCAT(B1.vFirstNameRe2, ' ', B1.vSecondNameRe2, ' ', B1.vFLastNameRe2, ' ', B1.vSLastNameRe2) AS Nombre_Referencia2,
                  B1.vPhoneNumberRe2 AS Telefono_Referencia2,
                  CONCAT(B1.vFirstNameRe3, ' ', B1.vSecondNameRe3, ' ', B1.vFLastNameRe3, ' ', B1.vSLastNameRe3) AS Nombre_Referencia3,
                  B1.vPhoneNumberRe3 AS Telefono_Referencia3,
                  CONCAT(B1.vFirstNameRe4, ' ', B1.vSecondNameRe4, ' ', B1.vFLastNameRe4, ' ', B1.vSLastNameRe4) AS Nombre_Referencia4,
                  B1.vPhoneNumberRe4 AS Telefono_Referencia4,
                  C1.Abreviatura AS Abreviatura_Dependencia,
                  C1.Clasificacion AS Clasificacion_Dependencia,
                  C1.Estatus AS Estatus_Dependencia,
                  CI3.Plaza AS Plaza_CI3,
                  C8.Validador AS Validador_C8,
                  D1.FechaGenerado
                FROM d1_saldos_cartera D1
                LEFT JOIN c2_financiamiento C2 
                  ON D1.tipo_financiamiento = C2.tipo_financiamiento
                LEFT JOIN r1_quebrantos_calculado R1 
                  ON D1.Id_Credito = R1.Operacion
                  AND R1.Fecha_Generado = (
                    SELECT MAX(R1_sub.Fecha_Generado)
                    FROM r1_quebrantos_calculado R1_sub
                    WHERE R1_sub.Operacion = D1.Id_Credito
                  )
                LEFT JOIN b1_demograficos B1
                  ON D1.Id_Persona = B1.Id_Persona
                LEFT JOIN c1_dependencia C1
                  ON D1.Dependencia = C1.Dependencia
                LEFT JOIN ci3_plazas CI3
                  ON D1.Id_Credito = CI3.Id_Credito
                JOIN c8_validador_motivo_clasificacion C8
                  ON D1.Motivo = C8.Motivo 
                  AND C1.Clasificacion = C8.Clasificacion
                  AND C8.Validador = 1
                WHERE 
                  D1.FechaGenerado = (SELECT MAX(FechaGenerado) FROM d1_saldos_cartera)
                  AND C2.Producto = 'DxN'
                  AND D1.Estatus IN ('Activo','Castigado')
                  AND D1.vCollectStatus NOT IN ('Quebranto pagado','Defuncion');

                INSERT INTO R5_Asignacion_Nomina (
                    Id_Credito, 
                    Id_Persona, 
                    Referencia, 
                    Afiliado, 
                    Nombre, 
                    Monto, 
                    Pagos, 
                    Dependencia, 
                    Pago, 
                    Estatus, 
                    Fecha_Desembolso, 
                    Frecuencia, 
                    Dias_Atraso, 
                    Bucket, 
                    Vencido, 
                    Saldo_Total, 
                    Saldo_Capital, 
                    Motivo, 
                    vCollectStatus, 
                    Producto, 
                    Quebranto_Pagare, 
                    Saldo_Q_Pagare, 
                    Quebranto_Capital, 
                    Saldo_Q_Capital, 
                    Quebranto_Contable, 
                    Saldo_Q_Contable, 
                    Direccion_Cliente, 
                    vPhoneNumberAd, 
                    vMobile, 
                    Nombre_Referencia1, 
                    Telefono_Referencia1, 
                    Nombre_Referencia2, 
                    Telefono_Referencia2, 
                    Nombre_Referencia3, 
                    Telefono_Referencia3, 
                    Nombre_Referencia4, 
                    Telefono_Referencia4, 
                    Abreviatura_Dependencia, 
                    Clasificacion_Dependencia, 
                    Estatus_Dependencia, 
                    Plaza_CI3, 
                    Validador_C8,
                    FechaGenerado
                )
                SELECT 
                    Id_Credito, 
                    Id_Persona, 
                    Referencia, 
                    Afiliado, 
                    Nombre, 
                    Monto, 
                    Pagos, 
                    Dependencia, 
                    Pago, 
                    Estatus, 
                    Fecha_Desembolso, 
                    Frecuencia, 
                    Dias_Atraso, 
                    Bucket, 
                    Vencido, 
                    Saldo_Total, 
                    Saldo_Capital, 
                    Motivo, 
                    vCollectStatus, 
                    Producto, 
                    Quebranto_Pagare, 
                    Saldo_Q_Pagare, 
                    Quebranto_Capital, 
                    Saldo_Q_Capital, 
                    Quebranto_Contable, 
                    Saldo_Q_Contable, 
                    Direccion_Cliente, 
                    vPhoneNumberAd, 
                    vMobile, 
                    Nombre_Referencia1, 
                    Telefono_Referencia1, 
                    Nombre_Referencia2, 
                    Telefono_Referencia2, 
                    Nombre_Referencia3, 
                    Telefono_Referencia3, 
                    Nombre_Referencia4, 
                    Telefono_Referencia4, 
                    Abreviatura_Dependencia, 
                    Clasificacion_Dependencia, 
                    Estatus_Dependencia, 
                    Plaza_CI3, 
                    Validador_C8,
                    FechaGenerado
                FROM vw_asignacion_nomina;
            ";

            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = await connection.BeginTransactionAsync())
                {
                    try
                    {
                        var command = new MySqlCommand(sql, connection, transaction)
                        {
                            CommandTimeout = 300
                        };
                        int rowsAffected = await command.ExecuteNonQueryAsync();
                        await transaction.CommitAsync();
                        logBuilder.AppendLine($"[OK] - Inserted {rowsAffected} rows into R5_Asignacion_Nomina.");
                        return rowsAffected;
                    }
                    catch (Exception ex)
                    {
                        await transaction.RollbackAsync();
                        _logger.LogError(ex, "[ERROR] - Error al insertar datos en R5_Asignacion_Nomina.");
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Método que exporta a CSV los registros de R5_Asignacion_Nomina correspondientes a la fecha más reciente.
        /// </summary>
        private async Task<IActionResult> R5_ExportInternalAsync(StringBuilder logBuilder)
        {
            try
            {
                if (!Directory.Exists(_exportFolderPath))
                {
                    Directory.CreateDirectory(_exportFolderPath);
                }

                DateTime mostRecentDate;
                using (var connection = new MySqlConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    var cmdMostRecent = new MySqlCommand("SELECT MAX(FechaGenerado) FROM R5_Asignacion_Nomina", connection);
                    var result = await cmdMostRecent.ExecuteScalarAsync();
                    if (result == null || result == DBNull.Value)
                    {
                        logBuilder.AppendLine("[E207] - No se encontró fecha de generación en R5_Asignacion_Nomina.");
                        return NotFound("No se encontraron registros para exportar.");
                    }
                    mostRecentDate = Convert.ToDateTime(result);
                    _logger.LogInformation($"[OK] [E207] - Fecha más reciente obtenida: {mostRecentDate:yyyy-MM-dd HH:mm:ss}");
                }

                using (var connection = new MySqlConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    var sqlQuery = "SELECT * FROM R5_Asignacion_Nomina WHERE FechaGenerado = @MostRecentDate";
                    var command = new MySqlCommand(sqlQuery, connection);
                    command.Parameters.AddWithValue("@MostRecentDate", mostRecentDate);
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (!reader.HasRows)
                        {
                            logBuilder.AppendLine("[E207] - No se encontraron registros para la fecha más reciente.");
                            return NotFound("No se encontraron registros para exportar.");
                        }
                        var exportFilePath = Path.Combine(_exportFolderPath, $"R5_AsignacionNomina_Export_{mostRecentDate:yyyyMMdd_HHmmss}.csv");
                        using (var writer = new StreamWriter(exportFilePath, false, new UTF8Encoding(true)))
                        {
                            var columnNames = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName);
                            await writer.WriteLineAsync(string.Join(",", columnNames));
                            while (await reader.ReadAsync())
                            {
                                var rowValues = new System.Collections.Generic.List<string>();
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    var value = reader[i]?.ToString() ?? "";
                                    value = value.Replace("\"", "\"\"");
                                    rowValues.Add($"\"{value}\"");
                                }
                                await writer.WriteLineAsync(string.Join(",", rowValues));
                            }
                        }
                        logBuilder.AppendLine($"[OK] [E207] - Exportación exitosa: {exportFilePath}");
                        _logger.LogInformation($"Exportación completada. Archivo generado: {exportFilePath}");
                        return Ok($"Exportado exitosamente a: {exportFilePath}");
                    }
                }
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"[ERROR] [E207] - Error durante la exportación: {ex.Message}");
                _logger.LogError(ex, "[ERROR] [E207] - Error durante la exportación de R5_Asignacion_Nomina.");
                return StatusCode(500, "Error durante la exportación.");
            }
        }

        /// <summary>
        /// Escribe el log en un archivo.
        /// </summary>
        private async Task R5_WriteLogAsync(string logContent, string logFileName)
        {
            try
            {
                var fullLogPath = Path.Combine(_logFolderPath, logFileName);
                if (!Directory.Exists(_logFolderPath))
                {
                    Directory.CreateDirectory(_logFolderPath);
                }
                await System.IO.File.WriteAllTextAsync(fullLogPath, logContent, Encoding.UTF8);
                _logger.LogInformation($"[OK] - Log escrito en: {fullLogPath}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"[ERROR] - Error al escribir log: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Mueve los logs existentes a la carpeta de históricos.
        /// </summary>
        private void MoveExistingLogs(string controllerName, string logFolder, string historicLogFolder)
        {
            try
            {
                var existingLogs = Directory.GetFiles(logFolder, $"{controllerName}_*.log");
                foreach (var logFile in existingLogs)
                {
                    if (!Directory.Exists(historicLogFolder))
                        Directory.CreateDirectory(historicLogFolder);
                    var fileName = Path.GetFileName(logFile);
                    var destinationFilePath = Path.Combine(historicLogFolder, fileName);
                    if (System.IO.File.Exists(destinationFilePath))
                        System.IO.File.Delete(destinationFilePath);
                    System.IO.File.Move(logFile, destinationFilePath);
                    _logger.LogInformation($"[OK] - Moved existing log {logFile} to historic logs: {destinationFilePath}");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error moving existing logs for {controllerName}");
                // No se bloquea el proceso si falla el movimiento de logs.
            }
        }
    }
}
