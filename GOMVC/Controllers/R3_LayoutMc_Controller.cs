using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GOMVC.Controllers
{
    public class R3_LayoutMc_Controller : Controller
    {
        private readonly ILogger<R3_LayoutMc_Controller> _logger;
        private readonly IConfiguration _configuration;
        private readonly string _connectionString;
        // Carpeta de logs y de exportación según lo estipulado
        private readonly string _logFolderPath = @"C:\Users\Go Credit\Documents\DATA\LOGS";
        private readonly string _exportFolderPath = @"C:\Users\Go Credit\Documents\DATA\EXPORTS";
        // Carpeta para logs históricos (punto 7)
        private readonly string _historicLogsFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC LOGS";

        public R3_LayoutMc_Controller(ILogger<R3_LayoutMc_Controller> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
        }

        /// <summary>
        /// Procesa la consolidación y transformación de datos e inserta los resultados en r3_layoutmc.
        /// Al finalizar, se llama al proceso de exportación para generar un CSV con los registros de la fecha más reciente.
        /// </summary>
        [HttpPost]
        public async Task<IActionResult> R3_ProcessLayout()
        {
            // (5) Generar el nombre del log: Controlador + Timestamp
            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var controllerName = nameof(R3_LayoutMc_Controller);
            var logFileName = $"{controllerName}_{timestamp}.log";
            var logPath = System.IO.Path.Combine(_logFolderPath, logFileName);
            // (7) Mover logs existentes con este nombre a la carpeta histórica
            MoveExistingLogs(controllerName, _logFolderPath, _historicLogsFolder);

            var logBuilder = new StringBuilder();
            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Proceso R3 Layout iniciado.");
            _logger.LogInformation("Proceso R3 Layout iniciado.");

            try
            {
                _logger.LogInformation("Validando existencia de datos de origen en D1_Saldos_Cartera...");
                if (!await R3_ValidateSourceDataAsync(logBuilder))
                {
                    string errorMessage = "[E101] - No existen datos de origen para procesar R3 Layout.";
                    logBuilder.AppendLine(errorMessage);
                    _logger.LogError(errorMessage);
                    await R3_WriteLogAsync(logBuilder.ToString(), logFileName);
                    return BadRequest(errorMessage);
                }

                _logger.LogInformation("Validación completada.");
                _logger.LogInformation("Ejecutando transformación e inserción en r3_layoutmc...");
                int rowsAffected = await R3_InsertLayoutDataAsync(logBuilder);
                logBuilder.AppendLine($"[OK] [E104] - Transformación completada. Filas insertadas: {rowsAffected}");
                _logger.LogInformation($"Transformación completada. Filas insertadas: {rowsAffected}");

                // Llamada al proceso de exportación, con código exclusivo [E107] en caso de error
                _logger.LogInformation("Iniciando exportación a CSV...");
                IActionResult exportResult = await R3_ExportInternalAsync(logBuilder);
                string exportMessage = exportResult is OkObjectResult okResult 
                    ? okResult.Value.ToString() 
                    : "[E107] - Exportación no generada.";
                logBuilder.AppendLine(exportMessage);
                _logger.LogInformation(exportMessage);

                await R3_WriteLogAsync(logBuilder.ToString(), logFileName);
                return Ok($"Proceso completado. Filas insertadas: {rowsAffected}. {exportMessage}");
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"[ERROR] - Error en el proceso: {ex.Message}");
                _logger.LogError(ex, "Error en el proceso R3 Layout.");
                await R3_WriteLogAsync(logBuilder.ToString(), logFileName);
                return StatusCode(500, "Error interno en el proceso R3 Layout.");
            }
        }

        /// <summary>
        /// Valida que existan registros en la tabla de origen (ej. D1_Saldos_Cartera).
        /// </summary>
        private async Task<bool> R3_ValidateSourceDataAsync(StringBuilder logBuilder)
        {
            try
            {
                using (var connection = new MySqlConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    var cmd = new MySqlCommand("SELECT COUNT(*) FROM D1_Saldos_Cartera", connection);
                    var result = await cmd.ExecuteScalarAsync();
                    int count = Convert.ToInt32(result);
                    logBuilder.AppendLine($"[OK] [E101] - D1_Saldos_Cartera tiene {count} registros.");
                    return count > 0;
                }
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"[ERROR] [E101] - Error en R3_ValidateSourceDataAsync: {ex.Message}");
                _logger.LogError(ex, "Error en R3_ValidateSourceDataAsync");
                return false;
            }
        }

        /// <summary>
        /// Ejecuta la consulta SQL que transforma y consolida los datos e inserta los resultados en r3_layoutmc.
        /// Se aumenta el CommandTimeout para consultas largas.
        /// </summary>
       private async Task<int> R3_InsertLayoutDataAsync(StringBuilder logBuilder)
        {
            var sql = @"
        CREATE OR REPLACE VIEW vw_R3_D1_Reciente AS
        SELECT *
        FROM D1_Saldos_Cartera
        WHERE FechaGenerado = (SELECT MAX(FechaGenerado) FROM D1_Saldos_Cartera);

        CREATE OR REPLACE VIEW vw_R3_D2_Reciente AS
        SELECT *
        FROM D2_Saldos_Contables
        WHERE FechaGenerado = (SELECT MAX(FechaGenerado) FROM D2_Saldos_Contables);

        CREATE OR REPLACE VIEW vw_R3_R1_Reciente AS
        SELECT *
        FROM R1_Quebrantos_Calculado
        WHERE Fecha_Generado = (SELECT MAX(Fecha_Generado) FROM R1_Quebrantos_Calculado);

        INSERT INTO r3_layoutmc (
            Credito, 
            Cliente_Numero, 
            Nombre, 
            Segundo_Nombre, 
            Primer_Apellido, 
            Segundo_Apellido, 
            Fecha_de_Nacimiento, 
            Estado_de_Nacimiento, 
            Sexo, 
            RFC, 
            CURP, 
            Nombre_de_Conyuge, 
            Segundo_Nombre_de_Conyuge, 
            Primer_Apellido_de_Conyuge, 
            Segundo_Apellido_de_Conyuge, 
            Calle, 
            Numero, 
            Interior, 
            Entre_Calle_1, 
            Entre_Calle_2, 
            Colonia, 
            Municipio, 
            Ciudad, 
            Estado, 
            Codigo_Postal, 
            Correo_Electronico, 
            Tel_Celular, 
            Telefono_de_Casa, 
            Puesto_del_Trabajo, 
            Centro_de_Trabajo_Dependencia, 
            Tel_Empleo, 
            Referencia_1, 
            Parentesco_Ref_1, 
            Telefono_Ref_1, 
            Referencia_2, 
            Parentesco_Ref_2, 
            Telefono_Ref_2, 
            Referencia_3, 
            Parentesco_Ref_3, 
            Telefono_Ref_3, 
            Referencia_4, 
            Parentesco_Ref_4, 
            Telefono_Ref_4, 
            F_Desembolso, 
            Estatus, 
            Tipo_de_Financiamiento, 
            Cargos_Moratorios, 
            Vencido, 
            Cuotas_Vencidas, 
            Importe_de_Deuda_Original, 
            Primer_Pago_Real, 
            Saldo_Total_Actual, 
            Monto_del_Credito, 
            Plazo_del_Credito, 
            Amortizaciones_Pagadas, 
            F_Ultimo_Pago_Aplicado, 
            Monto_Ultimo_Pago_Aplicado, 
            Dependencia_del_Credito, 
            Sucursal_Otorgamiento, 
            Dias_de_Atraso, 
            Referencia_de_Pago, 
            Motivo_de_Cobranza_Nomina, 
            Clabe_Interbancaria, 
            Banco_Domiciliacion, 
            Frecuencia, 
            Importe_de_Pago, 
            Fecha_de_Siguiente_Pago, 
            Saldo_Contable, 
            Bucket, 
            Rango_Pagado, 
            Producto, 
            Tipo_de_Credito, 
            Plaza_Zona, 
            Estatus_Convenio, 
            Dep_Abreviada, 
            Clasificacion_de_Convenio, 
            Quebranto_Contable, 
            Quebranto_Capital, 
            Saldo_Quebranto, 
            Fecha_Quebranto, 
            Estatus_Cartera, 
            Total_Pagado, 
            Saldo_en_Exceso, 
            Porcentaje_Pagado, 
            Estatus_Domiciliacion, 
            Recuperacion
        )
        SELECT
            T.Id_Credito,
            T.Id_Persona,
            B1.vFirstName AS `Nombre`,
            B1.vSecondName AS `Segundo Nombre`,
            B1.vFLastName AS `Primer Apellido`,
            B1.vSLastName AS `Segundo Apellido`,
            STR_TO_DATE(NULLIF(B1.vBirthDate, ''), '%d/%m/%Y') AS `Fecha de Nacimiento`,
            B1.vStateId AS `Estado de Nacimiento`,
            B1.vOpSex AS Sexo,
            B1.vTreasuryId AS RFC,
            B1.vLegalId AS CURP,
            B1.vSpFirstName AS `Nombre de Conyuge`,
            B1.vSpSecondName AS `Segundo_Nombre_de_Conyuge`,
            B1.vSpFLastName AS `Primer Apellido_de_Conyuge`,
            B1.vSpSLastName AS `Segundo Apellido_de_Conyuge`,
            B1.vStreetAd AS Calle,
            B1.vExtNumberAd AS Numero,
            B1.vIntNumberAd AS Interior,
            B1.vCCornerAd AS `Entre Calle 1`,
            B1.vCornerAd AS `Entre Calle 2`,
            B1.vNeighborhoodAd AS Colonia,
            B1.vTownshipAd AS Municipio,
            B1.vCityAd AS Ciudad,
            B1.vStateIdAd AS Estado,
            B1.iZipAd AS `Codigo_Postal`,
            B1.vEMail AS `Correo_Electronico`,
            B1.vMobile AS `Tel_Celular`,
            B1.vPhoneNumberAd AS `Telefono_de_Casa`,
            B1.vPositionJo AS `Puesto_del_Trabajo`,
            B1.vCompanyJo AS `Centro_de_Trabajo_Dependencia`,
            B1.vPhoneNumberJo AS `Tel_Empleo`,
            CONCAT(B1.vFirstNameRe1, ' ', B1.vFLastNameRe1) AS `Referencia_1`,
            B1.vOpRelationType1 AS `Parentesco_Ref_1`,
            B1.vPhoneNumberRe1 AS `Telefono_Ref_1`,
            CONCAT(B1.vFirstNameRe2, ' ', B1.vFLastNameRe2) AS `Referencia_2`,
            B1.vOpRelationType2 AS `Parentesco_Ref_2`,
            B1.vPhoneNumberRe2 AS `Telefono_Ref_2`,
            CONCAT(B1.vFirstNameRe3, ' ', B1.vFLastNameRe3) AS `Referencia_3`,
            B1.vOpRelationType3 AS `Parentesco_Ref_3`,
            B1.vPhoneNumberRe3 AS `Telefono_Ref_3`,
            CONCAT(B1.vFirstNameRe4, ' ', B1.vFLastNameRe4) AS `Referencia_4`,
            B1.vOpRelationType4 AS `Parentesco_Ref_4`,
            B1.vPhoneNumberRe4 AS `Telefono_Ref_4`,
            T.Fecha_Desembolso AS `F_Desembolso`,
            T.Estatus,
            T.Tipo_Financiamiento AS `Tipo_de_Financiamiento`,
            T.Saldo_Moratorios AS `Cargos_Moratorios`,
            T.Vencido,
            T.Cuotas_Atraso AS `Cuotas_Vencidas`,
            T.Monto_Total AS `Importe_de_Deuda_Original`,
            T.Primer_Pago_Real AS `1er_Pago_Real`,
            T.Saldo_Total AS `Saldo_Total_Actual`,
            T.Monto_Total AS `Monto_del_Credito`,
            T.Pagos AS `Plazo_del_Credito`,
            T.Amort_Pagadas AS `Amortizaciones_Pagadas`,
            T.Ultimo_Pago_Aplicado AS `F_Ultimo_Pago_Aplicado`,
            T.Monto_Ultimo_Pago AS `Monto_Ultimo_Pago_Aplicado`,
            T.Dependencia AS `Dependencia_del_Credito`,
            T.Sucursal AS `Sucursal_Otorgamiento`,
            T.Dias_Atraso AS `Dias_de_Atraso`,
            T.Referencia AS `Referencia_de_Pago`,
            T.Motivo AS `Motivo_de_Cobranza_Nomina`,
            LEFT(T.Clabe, 19) AS `Clabe_Interbancaria`,
            T.Banco AS `Banco_Domiciliacion`,
            T.Frecuencia,
            T.Importe_de_Pago AS `Importe_de_Pago`,
            T.Sig_Pago AS `Fecha_de_Siguiente_Pago`,
            D2.Saldo_Contable AS `Saldo_Contable`,
            T.Bucket,
            CASE 
                WHEN T.Porcentaje_Pagado <= 0 THEN 'FPD'
                WHEN T.Porcentaje_Pagado > 0 AND T.Porcentaje_Pagado <= 0.25 THEN '1-25%'
                WHEN T.Porcentaje_Pagado > 0.25 AND T.Porcentaje_Pagado <= 0.50 THEN '26-50%'
                WHEN T.Porcentaje_Pagado > 0.50 AND T.Porcentaje_Pagado <= 0.75 THEN '51-75%'
                WHEN T.Porcentaje_Pagado > 0.75 AND T.Porcentaje_Pagado <= 1 THEN '76-100%'
                ELSE '76-100%'
            END AS `Rango_Pagado`,
            FIN.Producto,
            FIN.Financiamiento AS `Tipo_de_Credito`,
            P.Plaza,
            DEP.Estatus,
            DEP.Abreviatura,
            DEP.Clasificacion,
            QC.Quebranto_Contable,
            QC.Quebranto_Capital,
            QC.Saldo_Q_Pagare,
            QC.Fecha_Quebranto,
            T.Estatus_Cartera,
            T.Total_Pagado,
            T.Saldo_Pago_Exceso,
            T.Porcentaje_Pagado,
            DOMI.Rechazo,
            QC.Recuperacion
        FROM
            (
                SELECT 
                    *,
                    ROUND(
                        COALESCE(
                            CASE 
                                WHEN Estatus = 'Castigado' THEN (Total_Pagado / NULLIF(Importe_de_Pago, 0)) / NULLIF(Pagos, 0)
                                ELSE Amort_Pagadas / NULLIF(Pagos, 0)
                            END, 0
                        ), 2
                    ) AS Porcentaje_Pagado,
                    CASE 
                        WHEN Estatus = 'Activo' AND Dias_Atraso = 0 THEN '0'
                        WHEN Estatus = 'Activo' AND Dias_Atraso BETWEEN 1 AND 30 THEN '1 a 30'
                        WHEN Estatus = 'Activo' AND Dias_Atraso BETWEEN 31 AND 60 THEN '31 a 60'
                        WHEN Estatus = 'Activo' AND Dias_Atraso BETWEEN 61 AND 90 THEN '61 a 90'
                        WHEN Estatus = 'Activo' AND Dias_Atraso BETWEEN 91 AND 120 THEN '91 a 120'
                        WHEN Estatus = 'Activo' AND Dias_Atraso BETWEEN 121 AND 150 THEN '121 a 150'
                        WHEN Estatus = 'Activo' AND Dias_Atraso BETWEEN 151 AND 180 THEN '151 a 180'
                        WHEN Estatus = 'Activo' AND Dias_Atraso >= 181 THEN '181+'
                        ELSE Estatus
                    END AS Bucket
                FROM vw_R3_D1_Reciente
            ) AS T
        LEFT JOIN vw_R3_D2_Reciente D2 ON T.Id_Credito = D2.Id_Credito
        LEFT JOIN B1_Demograficos B1 ON T.Id_Persona = B1.Id_Persona
        LEFT JOIN C2_Financiamiento FIN ON T.Tipo_Financiamiento = FIN.Tipo_Financiamiento
        LEFT JOIN CI3_Plazas P ON T.Id_Credito = P.Id_Credito
        LEFT JOIN C1_Dependencia DEP ON T.Dependencia = DEP.Dependencia
        LEFT JOIN vw_R3_R1_Reciente QC ON T.Id_Credito = QC.Operacion
        LEFT JOIN CI2_Estatus_Domi DOMI ON T.Id_Credito = DOMI.Id_Credito;";

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
                        logBuilder.AppendLine($"[OK] [E104] - Inserted {rowsAffected} rows into r3_layoutmc.");
                        return rowsAffected;
                    }
                    catch (Exception ex)
                    {
                        await transaction.RollbackAsync();
                        _logger.LogError(ex, "[ERROR] [E104] - Error al insertar datos en r3_layoutmc");
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Ejecuta la lógica de exportación y, en caso de error, utiliza el código exclusivo [E107].
        /// </summary>
        private async Task<IActionResult> R3_ExportInternalAsync(StringBuilder logBuilder)
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
                    var cmdMostRecent = new MySqlCommand("SELECT MAX(Fecha_Generado) FROM r3_layoutmc", connection);
                    var result = await cmdMostRecent.ExecuteScalarAsync();
                    if (result == null || result == DBNull.Value)
                    {
                        logBuilder.AppendLine("[E107] - No se encontró fecha de generación en r3_layoutmc.");
                        return NotFound("No se encontraron registros para exportar.");
                    }
                    mostRecentDate = Convert.ToDateTime(result);
                    _logger.LogInformation($"[OK] [E107] - Fecha más reciente obtenida: {mostRecentDate:yyyy-MM-dd HH:mm:ss}");
                }
                using (var connection = new MySqlConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    var sqlQuery = "SELECT * FROM r3_layoutmc WHERE Fecha_Generado = @MostRecentDate";
                    var command = new MySqlCommand(sqlQuery, connection);
                    command.Parameters.AddWithValue("@MostRecentDate", mostRecentDate);
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (!reader.HasRows)
                        {
                            logBuilder.AppendLine("[E107] - No se encontraron registros para la fecha más reciente.");
                            return NotFound("No se encontraron registros para exportar.");
                        }
                        var exportFilePath = Path.Combine(_exportFolderPath, $"R3_LayoutMc_Export_{mostRecentDate:yyyyMMdd_HHmmss}.csv");
                        using (var writer = new StreamWriter(exportFilePath, false, new UTF8Encoding(true)))
                        {
                            var columnNames = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName);
                            await writer.WriteLineAsync(string.Join(",", columnNames));
                            while (await reader.ReadAsync())
                            {
                                var rowValues = new List<string>();
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    var value = reader[i]?.ToString() ?? "";
                                    value = value.Replace("\"", "\"\"");
                                    rowValues.Add($"\"{value}\"");
                                }
                                await writer.WriteLineAsync(string.Join(",", rowValues));
                            }
                        }
                        logBuilder.AppendLine($"[OK] [E107] - Exportación exitosa: {exportFilePath}");
                        _logger.LogInformation($"Exportación completada. Archivo generado: {exportFilePath}");
                        return Ok($"Exportado exitosamente a: {exportFilePath}");
                    }
                }
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"[ERROR] [E107] - Error durante la exportación: {ex.Message}");
                _logger.LogError(ex, "[ERROR] [E107] - Error durante la exportación de r3_layoutmc.");
                return StatusCode(500, "Error durante la exportación.");
            }
        }

        /// <summary>
        /// Escribe el contenido del log en un archivo.
        /// Antes de escribir, si ya existe un log con el mismo nombre se mueve a Historic Logs.
        /// </summary>
        private async Task R3_WriteLogAsync(string logContent, string logFileName)
        {
            try
            {
                // Verificar si ya existe un log con ese nombre y moverlo a Historic Logs
                var fullLogPath = Path.Combine(_logFolderPath, logFileName);
                if (System.IO.File.Exists(fullLogPath))
                {
                    MoveExistingLogs(nameof(R3_LayoutMc_Controller), _logFolderPath, _historicLogsFolder);
                }
                if (!Directory.Exists(_logFolderPath))
                {
                    Directory.CreateDirectory(_logFolderPath);
                }
                await System.IO.File.WriteAllTextAsync(fullLogPath, logContent, Encoding.UTF8);
                _logger.LogInformation($"[OK] [E106] - Log escrito en: {fullLogPath}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"[ERROR] [E106] - Error al escribir el log: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Mueve el archivo log al directorio de Historic Logs.
        /// </summary>
        private void D7_MoveLogToHistoric(string logPath, string historicLogsFolder)
        {
            try
            {
                if (!System.IO.File.Exists(logPath))
                {
                    _logger.LogWarning($"[E105] - Log file does not exist: {logPath}");
                    return;
                }
                if (!Directory.Exists(historicLogsFolder))
                    Directory.CreateDirectory(historicLogsFolder);

                var ts = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                var logFileName = Path.GetFileNameWithoutExtension(logPath) + $"_{ts}" + Path.GetExtension(logPath);
                var destinationFilePath = Path.Combine(historicLogsFolder, logFileName);
                System.IO.File.Move(logPath, destinationFilePath);
                _logger.LogInformation($"[OK] [E105] - Log file moved to historic logs: {destinationFilePath}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"[ERROR] [E105] - Error moving log file to historic logs: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Mueve los logs existentes a la carpeta Historic Logs.
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
                    _logger.LogInformation($"[OK] [E105] - Moved existing log {logFile} to historic logs: {destinationFilePath}");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error moving existing logs for {controllerName}");
                // No bloquear el proceso si falla el movimiento de logs.
            }
        }
    }
}
