using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace GOMVC.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class INT_MDC_CONTROLLER : Controller
    {
        private readonly ILogger<INT_MDC_CONTROLLER> _logger;
        private readonly IConfiguration _configuration;
        private readonly string _connectionString;
        // Directorios configurados
        private readonly string _filesDirectory = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
        private readonly string _archiveFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Archive";
        private readonly string _errorFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Error";
        private readonly string _processedFolder = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES\Processed";
        private readonly string _logsFolder = @"C:\Users\Go Credit\Documents\DATA\LOGS";

        public INT_MDC_CONTROLLER(ILogger<INT_MDC_CONTROLLER> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            _connectionString = _configuration.GetConnectionString("DefaultConnection");
        }

        [HttpGet]
        public IActionResult Index() => Ok("INT_MDC_CONTROLLER funcionando.");

        [HttpPost("ProcessAll")]
        public async Task<IActionResult> ProcessAll()
        {
            string logFileName = "INT_MDC_CONTROLLER.log";
            string logPath = Path.Combine(_logsFolder, logFileName);
            var logBuilder = new StringBuilder();
            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Proceso ProcessAll iniciado.");

            try
            {
                // 0. Truncar las tablas de staging
                await TruncateTableAsync("INT2_STAGE_MDC_SC", logBuilder);
                await TruncateTableAsync("INT1_STAGE_MDC_TR", logBuilder);

                // 1. Buscar archivos planos en _filesDirectory
                var scFiles = Directory.GetFiles(_filesDirectory, "mdc_*_sc.txt");
                var trFiles = Directory.GetFiles(_filesDirectory, "mdc_*_tr.txt");

                if (scFiles.Length == 0 && trFiles.Length == 0)
                {
                    logBuilder.AppendLine("No se encontraron archivos planos (sc o tr).");
                    return NotFound(logBuilder.ToString());
                }

                foreach (var file in scFiles)
                {
                    logBuilder.AppendLine($"Procesando archivo SC: {file}");
                    await BulkInsertFileAsync(file, "INT2_STAGE_MDC_SC", logBuilder);
                    MoveFile(file, _processedFolder, logBuilder);
                }
                foreach (var file in trFiles)
                {
                    logBuilder.AppendLine($"Procesando archivo TR: {file}");
                    await BulkInsertFileAsync(file, "INT1_STAGE_MDC_TR", logBuilder);
                    MoveFile(file, _processedFolder, logBuilder);
                }

                // 2. Insertar registros nuevos en INT1_MDC desde INT1_STAGE_MDC_TR
                await InsertFinalAsync(logBuilder);

                // 3. Actualizar registros en INT1_MDC (varios updates agrupados en secuencia)
                await UpdateFinalAsync(logBuilder);
                await UpdatePropiedadAsync(logBuilder);
                await UpdateRangoMOPAsync(logBuilder);
                await UpdateBCScoreAsync(logBuilder);

                logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Proceso ProcessAll completado exitosamente.");
                return Ok(logBuilder.ToString());
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error en ProcessAll: {ex.Message}");
                _logger.LogError(ex, "Error en ProcessAll en INT_MDC_CONTROLLER.");
                return StatusCode(500, logBuilder.ToString());
            }
        }

        #region Métodos Auxiliares

        private async Task TruncateTableAsync(string tableName, StringBuilder logBuilder)
        {
            string sql = $"TRUNCATE TABLE {tableName};";
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand(sql, connection))
                {
                    await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Tabla {tableName} truncada.");
                }
            }
        }

        private async Task BulkInsertFileAsync(string filePath, string targetTable, StringBuilder logBuilder)
        {
            string sql = $@"
                LOAD DATA LOCAL INFILE '{filePath.Replace(@"\", @"\\")}'
                INTO TABLE {targetTable}
                FIELDS TERMINATED BY '|' 
                OPTIONALLY ENCLOSED BY '""'
                LINES TERMINATED BY '\n'
                IGNORE 1 LINES;";
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand(sql, connection))
                {
                    command.CommandTimeout = 1800; // Timeout aumentado
                    int rowsAffected = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Archivo {Path.GetFileName(filePath)}: insertados {rowsAffected} registros en {targetTable}.");
                }
            }
        }

        private void MoveFile(string sourceFilePath, string destinationFolder, StringBuilder logBuilder)
        {
            try
            {
                if (!Directory.Exists(destinationFolder))
                    Directory.CreateDirectory(destinationFolder);
                string destinationFilePath = Path.Combine(destinationFolder, Path.GetFileName(sourceFilePath));
                if (System.IO.File.Exists(destinationFilePath))
                    System.IO.File.Delete(destinationFilePath);
                System.IO.File.Move(sourceFilePath, destinationFilePath);
                logBuilder.AppendLine($"Movido archivo: {sourceFilePath} -> {destinationFilePath}");
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error moviendo archivo {sourceFilePath}: {ex.Message}");
                _logger.LogError(ex, $"Error moviendo archivo {sourceFilePath}");
            }
        }

        private async Task InsertFinalAsync(StringBuilder logBuilder)
        {
            // Usamos REPLACE para convertir abreviaturas de meses en español a inglés.
            string insertFinalSql = @"
                INSERT INTO INT1_MDC (
                    ID_PERSONA,
                    ID_CREDITO,
                    Fecha_de_apertura,
                    Estatus,
                    Fecha_de_consulta_MDC,
                    Indicador_TL,
                    Fecha_de_integracion,
                    ID_Buro,
                    Clave_de_usuario,
                    Otorgante,
                    Otorgante_Real,
                    Propiedad,
                    Telefono_Otorgante,
                    No_Cuenta,
                    Responsabilidad,
                    Responsabilidad_Real,
                    Tipo_de_Cuenta,
                    Tipo_de_cuenta_Real,
                    Tipo_de_Contrato,
                    Tipo_de_contrato_Real,
                    Moneda,
                    Importe_Avaluo,
                    No_de_pagos,
                    Frecuencia_de_pagos,
                    Frecuencia_Real,
                    Monto_a_pagar,
                    Fecha_de_ultimo_pago,
                    Fecha_de_ultima_compra,
                    Fecha_de_cierre,
                    Fecha_de_actualizacion,
                    Ultima_vez_saldo_0,
                    Fecha_primer_historico,
                    Fecha_ultimo_historico,
                    Fecha_monto_max_morosidad,
                    Fecha_reestructura,
                    Garantia,
                    Credito_maximo,
                    Saldo_actual,
                    Limite_de_credito,
                    Saldo_vencido,
                    No_Pagos_vencidos,
                    Mop_actual,
                    Rango_MOP,
                    Historico_de_pagos,
                    Claves_de_observacion,
                    No_Total_de_pagos_revisados,
                    No_Total_de_pagos_calificados_MOP_02,
                    No_Total_de_pagos_calificados_MOP_03,
                    No_Total_de_pagos_calificados_MOP_04,
                    No_Total_de_pagos_calificados_MOP_05,
                    Monto_max_morosidad,
                    Max_morosidad,
                    BC_Score
                )
                SELECT
                    NULL,
                    No_Referencia AS ID_CREDITO,
                    Fecha_de_apertura,
                    NULL AS Estatus,
                    STR_TO_DATE(
                        REPLACE(REPLACE(Fecha_de_consulta_MDC, 'ene', 'jan'), 'Ene', 'Jan'),
                        '%d-%b-%Y'
                    ) AS Fecha_de_consulta_MDC,
                    Indicador_TL,
                    Fecha_de_integracion,
                    ID_Buro,
                    Clave_de_usuario,
                    Otorgante,
                    NULL,
                    NULL,
                    Telefono_Otorgante,
                    No_Cuenta,
                    Responsabilidad,
                    NULL,
                    Tipo_de_Cuenta,
                    NULL,
                    Tipo_de_Contrato,
                    NULL,
                    Moneda,
                    Importe_Avaluo,
                    No_de_pagos,
                    Frecuencia_de_pagos,
                    NULL,
                    Monto_a_pagar,
                    Fecha_de_ultimo_pago,
                    Fecha_de_ultima_compra,
                    Fecha_de_cierre,
                    Fecha_de_actualizacion,
                    Ultima_vez_saldo_0,
                    Fecha_primer_historico,
                    Fecha_ultimo_historico,
                    Fecha_monto_max_morosidad,
                    Fecha_reestructura,
                    Garantia,
                    Credito_maximo,
                    Saldo_actual,
                    Limite_de_credito,
                    Saldo_vencido,
                    No_Pagos_vencidos,
                    Mop_actual,
                    NULL,
                    Historico_de_pagos,
                    Claves_de_observacion,
                    No_Total_de_pagos_revisados,
                    No_Total_de_pagos_calificados_MOP_02,
                    No_Total_de_pagos_calificados_MOP_03,
                    No_Total_de_pagos_calificados_MOP_04,
                    No_Total_de_pagos_calificados_MOP_05,
                    Monto_max_morosidad,
                    Max_morosidad,
                    NULL AS BC_Score
                FROM INT1_STAGE_MDC_TR
                WHERE STR_TO_DATE(
                        REPLACE(REPLACE(Fecha_de_consulta_MDC, 'ene', 'jan'), 'Ene', 'Jan'),
                        '%d-%b-%Y'
                ) NOT IN (
                    SELECT Fecha_de_consulta_MDC FROM INT1_MDC
                );";

            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand(insertFinalSql, connection))
                {
                    command.CommandTimeout = 1800;
                    int rowsInserted = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Final Insert: se insertaron {rowsInserted} registros en INT1_MDC.");
                }
            }
        }

        private async Task UpdateFinalAsync(StringBuilder logBuilder)
        {
            // Actualiza INT1_MDC: ID_PERSONA, Estatus y Otorgante_Real desde D1_Saldos_Cartera
            string updateSql = @"
                UPDATE INT1_MDC m
                INNER JOIN D1_Saldos_Cartera d ON d.Id_Credito = m.ID_CREDITO
                SET 
                    m.ID_PERSONA = d.Id_Persona,
                    m.Estatus = CASE 
                                   WHEN m.Fecha_de_cierre = '1900-01-01' THEN 'abierto'
                                   ELSE 'cerrado'
                                END,
                    m.Otorgante_Real = CASE 
                                           WHEN m.Otorgante IN ('Financiera', 'MicroFinanciera', 'Monto Facil') THEN 'Financiera'
                                           WHEN m.Otorgante = 'Banco' THEN 'Banco'
                                           ELSE 'Otro'
                                       END
                WHERE d.Id_Persona IS NOT NULL;";
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand(updateSql, connection))
                {
                    command.CommandTimeout = 1800;
                    int rowsUpdated = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Update: se actualizaron {rowsUpdated} registros en INT1_MDC (ID_PERSONA, Estatus, Otorgante_Real).");
                }
            }

            // Actualizar Responsabilidad_Real desde INTCAT1_Responsabilidad
            string updateRespSql = @"
                UPDATE INT1_MDC m
                INNER JOIN INTCAT1_Responsabilidad cat ON cat.Codigo = m.Responsabilidad
                SET m.Responsabilidad_Real = cat.Descripcion;";
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand(updateRespSql, connection))
                {
                    command.CommandTimeout = 1800;
                    int rowsUpdated = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Update Responsabilidad: se actualizaron {rowsUpdated} registros (Responsabilidad_Real).");
                }
            }

            // Actualizar Tipo_de_cuenta_Real desde INTCAT3_TipoCuenta
            string updateTipoCuentaSql = @"
                UPDATE INT1_MDC m
                INNER JOIN INTCAT3_TipoCuenta cat ON cat.Codigo = m.Tipo_de_Cuenta
                SET m.Tipo_de_cuenta_Real = cat.Descripcion;";
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand(updateTipoCuentaSql, connection))
                {
                    command.CommandTimeout = 1800;
                    int rowsUpdated = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Update Tipo de Cuenta: se actualizaron {rowsUpdated} registros (Tipo_de_cuenta_Real).");
                }
            }

            // Actualizar Tipo_de_contrato_Real desde INTCAT2_TipoContrato
            string updateTipoContratoSql = @"
                UPDATE INT1_MDC m
                INNER JOIN INTCAT2_TipoContrato cat ON cat.Codigo = m.Tipo_de_Contrato
                SET m.Tipo_de_contrato_Real = cat.Descripcion;";
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand(updateTipoContratoSql, connection))
                {
                    command.CommandTimeout = 1800;
                    int rowsUpdated = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Update Tipo de Contrato: se actualizaron {rowsUpdated} registros (Tipo_de_contrato_Real).");
                }
            }

            // Actualizar Frecuencia_Real desde INTCAT4_FrecuenciaPago
            string updateFrecuenciaSql = @"
                UPDATE INT1_MDC m
                INNER JOIN INTCAT4_FrecuenciaPago cat ON cat.Codigo = m.Frecuencia_de_pagos
                SET m.Frecuencia_Real = cat.Descripcion;";
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand(updateFrecuenciaSql, connection))
                {
                    command.CommandTimeout = 1800;
                    int rowsUpdated = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Update Frecuencia: se actualizaron {rowsUpdated} registros (Frecuencia_Real).");
                }
            }
        }

        private async Task UpdateRangoMOPAsync(StringBuilder logBuilder)
        {
            string updateRangoMOPSql = @"
                UPDATE INT1_MDC
                SET Rango_MOP = CASE
                    WHEN Mop_actual IN ('UR', '0', '01', '02') THEN '01, 02'
                    WHEN Mop_actual IN ('03', '04', '05', '06', '07') THEN '03 - 07'
                    WHEN Mop_actual IN ('96', '97', '98', '99') THEN '96 - 97'
                    ELSE Rango_MOP
                END;";
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand(updateRangoMOPSql, connection))
                {
                    command.CommandTimeout = 1800;
                    int rowsUpdated = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Update Rango_MOP: se actualizaron {rowsUpdated} registros.");
                }
            }
        }

        private async Task UpdatePropiedadAsync(StringBuilder logBuilder)
        {
            // Actualiza Propiedad en INT1_MDC según condiciones:
            // - Si Tipo_de_contrato_Real IN ('Bienes raices', 'Mejoras a la casa') -> 'Hipoteca'
            // - Si Tipo_de_contrato_Real = 'compra de automovil' -> 'Auto'
            // - Si Otorgante = 'Automotriz' -> 'Auto'
            // - Si No_Cuenta = 'Automovil' -> 'Auto'
            // - Si Otorgante IN ('Bienes raices', 'Hipotecagobierno', 'Hipotecaria') -> 'Hipotecario'
            // - Si No_Cuenta IN ('Hipoteca', 'Bienes raices', 'Mejoras a la casa') -> 'Hipotecario'
            // - Sino, se asigna NULL.
            string updatePropiedadSql = @"
                UPDATE INT1_MDC
                SET Propiedad = CASE
                    WHEN Tipo_de_contrato_Real IN ('Bienes raices', 'Mejoras a la casa') THEN 'Hipoteca'
                    WHEN Tipo_de_contrato_Real = 'compra de automovil' THEN 'Auto'
                    WHEN Otorgante = 'Automotriz' THEN 'Auto'
                    WHEN No_Cuenta = 'Automovil' THEN 'Auto'
                    WHEN Otorgante IN ('Bienes raices', 'Hipotecagobierno', 'Hipotecaria') THEN 'Hipotecario'
                    WHEN No_Cuenta IN ('Hipoteca', 'Bienes raices', 'Mejoras a la casa') THEN 'Hipotecario'
                    ELSE NULL
                END;";
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand(updatePropiedadSql, connection))
                {
                    command.CommandTimeout = 1800;
                    int rowsUpdated = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Update Propiedad: se actualizaron {rowsUpdated} registros en INT1_MDC (Propiedad).");
                }
            }
        }

        private async Task UpdateBCScoreAsync(StringBuilder logBuilder)
        {
            // Actualizar BC_Score: se cruza INT1_MDC con INT2_STAGE_MDC_SC
            // donde ID_CREDITO = CuentaReferencia y ScoreName = 'BC SCORE'
            string updateBCScoreSql = @"
                UPDATE INT1_MDC m
                INNER JOIN INT2_STAGE_MDC_SC s ON m.ID_CREDITO = s.CuentaReferencia
                SET m.BC_Score = s.ScoreValue
                WHERE s.ScoreName = 'BC SCORE';";
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand(updateBCScoreSql, connection))
                {
                    command.CommandTimeout = 1800;
                    int rowsUpdated = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Update BC_Score: se actualizaron {rowsUpdated} registros en INT1_MDC (BC_Score).");
                }
            }
        }

        #endregion
    }
}
