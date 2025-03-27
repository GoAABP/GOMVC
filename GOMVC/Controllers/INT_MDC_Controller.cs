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
                // 0. Truncar la tabla de staging para TR
                await TruncateTableAsync("INT1_STAGE_MDC_TR", logBuilder);
                // (Opcional) Truncar la tabla final si se desea reiniciar
                // await TruncateTableAsync("INT1_MDC_TR", logBuilder);

                // 1. Buscar archivos planos en _filesDirectory
                var scFiles = Directory.GetFiles(_filesDirectory, "mdc_*_sc.txt");
                var trFiles = Directory.GetFiles(_filesDirectory, "mdc_*_tr.txt");

                if (scFiles.Length == 0 && trFiles.Length == 0)
                {
                    logBuilder.AppendLine("No se encontraron archivos planos (sc o tr).");
                    return NotFound(logBuilder.ToString());
                }

                // Archivos SC: se cargan directamente en la tabla final INT2_MDC_SC
                foreach (var file in scFiles)
                {
                    logBuilder.AppendLine($"Procesando archivo SC: {file}");
                    await BulkInsertFileAsync(file, "INT2_MDC_SC", logBuilder);
                    MoveFile(file, _processedFolder, logBuilder);
                }

                // Archivos TR: se cargan en la tabla de staging INT1_STAGE_MDC_TR
                foreach (var file in trFiles)
                {
                    logBuilder.AppendLine($"Procesando archivo TR: {file}");
                    await BulkInsertFileAsync(file, "INT1_STAGE_MDC_TR", logBuilder);
                    MoveFile(file, _processedFolder, logBuilder);
                }

                // 2. Insertar registros de la tabla staging en la tabla final reordenando las columnas
                await InsertFinalFromStageAsync(logBuilder);

                // 3. Ejecutar un UPDATE combinado en la tabla final para actualizar los campos derivados
                await CombinedUpdateFinalAsync(logBuilder);

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

        // Nuevo endpoint para procesar archivos SC mediante staging
        [HttpPost("ProcessSC")]
        public async Task<IActionResult> ProcessSC()
        {
            string logFileName = "INT2_MDC_SC_Process.log";
            string logPath = Path.Combine(_logsFolder, logFileName);
            var logBuilder = new StringBuilder();
            logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Proceso ProcessSC iniciado.");

            try
            {
                // 0. Truncar la tabla de staging para SC
                await TruncateTableAsync("INT2_STAGE_MDC_SC", logBuilder);

                // 1. Buscar archivos planos SC en _filesDirectory (ej. "mdc_*_sc.txt")
                var scFiles = Directory.GetFiles(_filesDirectory, "mdc_*_sc.txt");

                if (scFiles.Length == 0)
                {
                    logBuilder.AppendLine("No se encontraron archivos SC.");
                    return NotFound(logBuilder.ToString());
                }

                // 2. Procesar cada archivo SC: BULK INSERT a la tabla de staging y mover el archivo procesado.
                foreach (var file in scFiles)
                {
                    logBuilder.AppendLine($"Procesando archivo SC: {file}");
                    await BulkInsertFileAsync(file, "INT2_STAGE_MDC_SC", logBuilder);
                    MoveFile(file, _processedFolder, logBuilder);
                }

                // 3. Insertar registros de la tabla staging en la tabla final INT2_MDC_SC
                await InsertFinalFromStageSC(logBuilder);

                logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Proceso ProcessSC completado exitosamente.");
                return Ok(logBuilder.ToString());
            }
            catch (Exception ex)
            {
                logBuilder.AppendLine($"Error en ProcessSC: {ex.Message}");
                _logger.LogError(ex, "Error en ProcessSC.");
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
                    command.CommandTimeout = 1800;
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

        // Inserta en la tabla final INT1_MDC_TR reordenando las columnas provenientes de la tabla de staging INT1_STAGE_MDC_TR
        private async Task InsertFinalFromStageAsync(StringBuilder logBuilder)
        {
            string sql = @"
                INSERT INTO INT1_MDC_TR (
                    ID_CREDITO,
                    Fecha_de_apertura,
                    Fecha_de_consulta_MDC,
                    Indicador_TL,
                    Fecha_de_integracion,
                    ID_Buro,
                    Clave_de_usuario,
                    Otorgante,
                    Telefono_Otorgante,
                    No_Cuenta,
                    Responsabilidad,
                    Tipo_de_Cuenta,
                    Tipo_de_Contrato,
                    Moneda,
                    Importe_Avaluo,
                    No_de_pagos,
                    Frecuencia_de_pagos,
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
                    Historico_de_pagos,
                    Claves_de_observacion,
                    No_Total_de_pagos_revisados,
                    No_Total_de_pagos_calificados_MOP_02,
                    No_Total_de_pagos_calificados_MOP_03,
                    No_Total_de_pagos_calificados_MOP_04,
                    No_Total_de_pagos_calificados_MOP_05,
                    Monto_max_morosidad,
                    Max_morosidad
                )
                SELECT 
                    No_Referencia,
                    Fecha_de_apertura,
                    STR_TO_DATE(
                      REPLACE(
                        REPLACE(
                          REPLACE(
                            REPLACE(
                              REPLACE(
                                REPLACE(
                                  REPLACE(
                                    REPLACE(Fecha_de_consulta_MDC, 'dic','dec'),
                                  'Dic','Dec'),
                                'ene','jan'),
                              'Ene','Jan'),
                            'abr','apr'),
                          'Abr','Apr'),
                        'ago','aug'),
                      'Ago','Aug'),
                    '%d-%b-%Y'
                    ) AS Fecha_de_consulta_MDC,
                    Indicador_TL,
                    Fecha_de_integracion,
                    ID_Buro,
                    Clave_de_usuario,
                    Otorgante,
                    Telefono_Otorgante,
                    No_Cuenta,
                    Responsabilidad,
                    Tipo_de_Cuenta,
                    Tipo_de_Contrato,
                    Moneda,
                    Importe_Avaluo,
                    No_de_pagos,
                    Frecuencia_de_pagos,
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
                    Historico_de_pagos,
                    Claves_de_observacion,
                    No_Total_de_pagos_revisados,
                    No_Total_de_pagos_calificados_MOP_02,
                    No_Total_de_pagos_calificados_MOP_03,
                    No_Total_de_pagos_calificados_MOP_04,
                    No_Total_de_pagos_calificados_MOP_05,
                    Monto_max_morosidad,
                    Max_morosidad
                FROM INT1_STAGE_MDC_TR;";
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand(sql, connection))
                {
                    command.CommandTimeout = 1800;
                    int rowsInserted = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Insert final (TR): se insertaron {rowsInserted} registros en INT1_MDC_TR desde staging.");
                }
            }
        }

        // Inserta en la tabla final INT2_MDC_SC desde la tabla de staging INT2_STAGE_MDC_SC
        private async Task InsertFinalFromStageSC(StringBuilder logBuilder)
        {
            string sql = @"
                INSERT INTO INT2_MDC_SC (
                    CuentaReferencia,
                    FechaConsulta,
                    ScoreIndicator,
                    ScoreName,
                    ScoreCode,
                    ScoreValue,
                    ReasonCode1,
                    ReasonCode2,
                    ReasonCode3,
                    ReasonCode4,
                    ErrorCode
                )
                SELECT 
                    CuentaReferencia,
                    FechaConsulta,
                    ScoreIndicator,
                    ScoreName,
                    ScoreCode,
                    ScoreValue,
                    ReasonCode1,
                    ReasonCode2,
                    ReasonCode3,
                    ReasonCode4,
                    ErrorCode
                FROM INT2_STAGE_MDC_SC;";
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand(sql, connection))
                {
                    command.CommandTimeout = 1800;
                    int rowsInserted = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Insert final (SC): se insertaron {rowsInserted} registros en INT2_MDC_SC desde INT2_STAGE_MDC_SC.");
                }
            }
        }

        // Ejecuta un UPDATE combinado en la tabla final para actualizar los campos derivados.
        private async Task CombinedUpdateFinalAsync(StringBuilder logBuilder)
        {
            string sql = @"
                UPDATE INT1_MDC_TR m
                LEFT JOIN D1_Saldos_Cartera d ON d.Id_Credito = m.ID_CREDITO
                LEFT JOIN INTCAT1_Responsabilidad r ON r.Codigo = m.Responsabilidad
                LEFT JOIN INTCAT3_TipoCuenta t ON t.Codigo = m.Tipo_de_Cuenta
                LEFT JOIN INTCAT2_TipoContrato c ON c.Codigo = m.Tipo_de_Contrato
                LEFT JOIN INTCAT4_FrecuenciaPago f ON f.Codigo = m.Frecuencia_de_pagos
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
                                       END,
                    m.Responsabilidad_Real = r.Descripcion,
                    m.Tipo_de_cuenta_Real = t.Descripcion,
                    m.Tipo_de_contrato_Real = c.Descripcion,
                    m.Frecuencia_Real = f.Descripcion,
                    m.Propiedad = CASE
                                    WHEN m.Tipo_de_contrato_Real IN ('Bienes raices', 'Mejoras a la casa') THEN 'Hipoteca'
                                    WHEN m.Tipo_de_contrato_Real = 'compra de automovil' THEN 'Auto'
                                    WHEN m.Otorgante = 'Automotriz' THEN 'Auto'
                                    WHEN m.No_Cuenta = 'Automovil' THEN 'Auto'
                                    WHEN m.Otorgante IN ('Bienes raices', 'Hipotecagobierno', 'Hipotecaria') THEN 'Hipotecario'
                                    WHEN m.No_Cuenta IN ('Hipoteca', 'Bienes raices', 'Mejoras a la casa') THEN 'Hipotecario'
                                    ELSE m.Propiedad
                                  END,
                    m.Rango_MOP = CASE
                                    WHEN m.Mop_actual IN ('UR', '00', '01', '02') THEN '01, 02'
                                    WHEN m.Mop_actual IN ('03', '04', '05', '06', '07') THEN '03 - 07'
                                    WHEN m.Mop_actual IN ('96', '97', '98', '99') THEN '96 - 97'
                                    ELSE m.Rango_MOP
                                  END;";
            using (var connection = new MySqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand(sql, connection))
                {
                    command.CommandTimeout = 1800;
                    int rowsUpdated = await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine($"Combined Update: se actualizaron {rowsUpdated} registros en INT1_MDC_TR.");
                }
            }
        }

        #endregion
    }
}
