using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

public class D8_Sistema_Controller : Controller
{
    private readonly ILogger<D8_Sistema_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    public D8_Sistema_Controller(ILogger<D8_Sistema_Controller> logger, IConfiguration configuration)
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    {
        _logger = logger;
        _configuration = configuration;
#pragma warning disable CS8601 // Possible null reference assignment.
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
#pragma warning restore CS8601 // Possible null reference assignment.
    }

    public async Task D8_ProcessSistema()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadSistema.log";
        var logBuilder = new StringBuilder();
        var startLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.";
        logBuilder.AppendLine(startLog);
        _logger.LogInformation(startLog);

        var files = Directory.GetFiles(_filePath, "Sistema_*.csv");
        if (files.Length == 0)
        {
            var errorLog = "File not found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D8_WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }

        var file = files[0];
        logBuilder.AppendLine("File found.");

        try
        {
            var textFilePath = await D8_ConvertSistemaCsvToText(file, logBuilder);
            await D8_BulkInsertSistema(textFilePath, logBuilder);
            await D8_ExecuteInsertSistema(logBuilder);
            D8_MoveFilesToHistoric(file, textFilePath, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await D8_WriteLog(logBuilder.ToString(), logPath);
            throw;
        }

        var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.";
        logBuilder.AppendLine(endLog);
        _logger.LogInformation(endLog);
        await D8_WriteLog(logBuilder.ToString(), logPath);
    }

    private async Task<string> D8_ConvertSistemaCsvToText(string csvFilePath, StringBuilder logBuilder)
    {
        var textFilePath = Path.ChangeExtension(csvFilePath, ".txt");
        var sb = new StringBuilder();

        try
        {
            using (var reader = new StreamReader(csvFilePath, Encoding.GetEncoding("windows-1252")))
            {
                string line;
#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    var processedLine = line.Normalize(NormalizationForm.FormC);
                    processedLine = processedLine.Replace(",", "|"); // Replace commas with the delimiter
                    sb.AppendLine(processedLine);
                }
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
            }

            await System.IO.File.WriteAllTextAsync(textFilePath, sb.ToString(), Encoding.UTF8);
            logBuilder.AppendLine($"Converted CSV to text: {textFilePath}");
            _logger.LogInformation($"Converted CSV to text: {textFilePath}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error during conversion: {ex.Message}");
            _logger.LogError(ex, $"Error during conversion: {ex.Message}");
            throw;
        }

        return textFilePath;
    }

    private async Task D8_BulkInsertSistema(string textFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D8_Stage_Sistema;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D8_Stage_Sistema.");
                    _logger.LogInformation("Truncated table D8_Stage_Sistema.");

                        var loadCommandText = $"LOAD DATA LOCAL INFILE '{textFilePath.Replace("\\", "\\\\")}' " +
                          "INTO TABLE D8_Stage_Sistema" +
                          "FIELDS TERMINATED BY '|' " +
                          "ENCLOSED BY '\"' " +
                          "LINES TERMINATED BY '\\n' " +
                          "IGNORE 1 LINES;";

                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Bulk inserted data into D8_Stage_Sistema.");
                    _logger.LogInformation("Bulk inserted data into D8_Stage_Sistema.");

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error during bulk insert: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    throw;
                }
            }
        }
    }

    private async Task D8_ExecuteInsertSistema(StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var insertCommandText = @"
                        INSERT INTO D8_Sistema (
                            Agencia_Asignada_MC, Agencia_MC, Bandera_PP_Juicio, Codigo_MC, Credito_MC, Cuenta_Al_Corriente,
                            Dias_en_la_instancia_actual, Dias_Para_Siguiente_Pago, Estatus_MC, Estrategia, Excepciones_MC,
                            Fecha_de_Asignacion_CallCenter, Fecha_de_Asignacion_Visita, Fecha_De_Captura_de_Juicio,
                            Fecha_de_Ultima_Visita, Fecha_Promesa_MC, Fecha_Ult_Gestion_MC,
                            Importe_Pago_X2, Importe_Pago_X3, Importe_Pago_X4, Importe_Pago_X6, Monto_Promesa_MC,
                            No_Gestiones, No_Visitas, Nombre_Agencia_MC, Nombre_Del_Deudor_MC, Nombre_Instancia_MC,
                            Producto_MC, Quita_Exclusiva, Resultado_MC, Resultado_Visita_MC, Saldo_Menor, Semaforo_Gestion,
                            Ult_Causa_No_Domiciliacion, Ult_Causa_No_Pago, Usuario_Asignado, Usuario_Asignado_Extrajudicial
                        )
                        SELECT
                            Agencia_Asignada_MC, Agencia_MC, Bandera_PP_Juicio, Codigo_MC, Credito_MC, Cuenta_Al_Corriente,
                            Dias_en_la_instancia_actual, Dias_Para_Siguiente_Pago, Estatus_MC, Estrategia, Excepciones_MC,
                            CASE
                                WHEN Fecha_de_Asignacion_CallCenter = '0000-00-00' OR Fecha_de_Asignacion_CallCenter = '' THEN NULL
                                ELSE STR_TO_DATE(Fecha_de_Asignacion_CallCenter, '%Y-%m-%d')
                            END AS Fecha_de_Asignacion_CallCenter,
                            CASE
                                WHEN Fecha_de_Asignacion_Visita = '0000-00-00' OR Fecha_de_Asignacion_Visita = '' THEN NULL
                                ELSE STR_TO_DATE(Fecha_de_Asignacion_Visita, '%Y-%m-%d')
                            END AS Fecha_de_Asignacion_Visita,
                            CASE
                                WHEN Fecha_De_Captura_de_Juicio = '0000-00-00' OR Fecha_De_Captura_de_Juicio = '' THEN NULL
                                ELSE STR_TO_DATE(Fecha_De_Captura_de_Juicio, '%Y-%m-%d')
                            END AS Fecha_De_Captura_de_Juicio,
                            CASE
                                WHEN Fecha_de_Ultima_Visita = '0000-00-00' OR Fecha_de_Ultima_Visita = '' THEN NULL
                                ELSE STR_TO_DATE(Fecha_de_Ultima_Visita, '%Y-%m-%d')
                            END AS Fecha_de_Ultima_Visita,
                            CASE
                                WHEN Fecha_Promesa_MC = '0000-00-00' OR Fecha_Promesa_MC = '' THEN NULL
                                ELSE STR_TO_DATE(Fecha_Promesa_MC, '%Y-%m-%d')
                            END AS Fecha_Promesa_MC,
                            CASE
                                WHEN Fecha_Ult_Gestion_MC = '0000-00-00' OR Fecha_Ult_Gestion_MC = '' THEN NULL
                                ELSE STR_TO_DATE(Fecha_Ult_Gestion_MC, '%Y-%m-%d')
                            END AS Fecha_Ult_Gestion_MC,
                            Importe_Pago_X2, Importe_Pago_X3, Importe_Pago_X4, Importe_Pago_X6, Monto_Promesa_MC,
                            No_Gestiones, No_Visitas, Nombre_Agencia_MC, Nombre_Del_Deudor_MC, Nombre_Instancia_MC,
                            Producto_MC, Quita_Exclusiva, Resultado_MC, Resultado_Visita_MC, Saldo_Menor, Semaforo_Gestion,
                            Ult_Causa_No_Domiciliacion, Ult_Causa_No_Pago, Usuario_Asignado, Usuario_Asignado_Extrajudicial
                        FROM D8_Stage_Sistema;";

                    var insertCommand = new MySqlCommand(insertCommandText, connection, transaction);
                    await insertCommand.ExecuteNonQueryAsync();

                    logBuilder.AppendLine("Insert successful.");
                    _logger.LogInformation("Insert successful.");

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    var errorLog = $"Error executing insert: {ex.Message}";
                    logBuilder.AppendLine(errorLog);
                    _logger.LogError(ex, errorLog);
                    throw;
                }
            }
        }
    }

    private void D8_MoveFilesToHistoric(string originalFilePath, string textFilePath, StringBuilder logBuilder)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");

        // Move original file
        var originalFileName = Path.GetFileNameWithoutExtension(originalFilePath);
        var originalExtension = Path.GetExtension(originalFilePath);
        var newOriginalFileName = $"{originalFileName}_{timestamp}{originalExtension}";
        var newOriginalFilePath = Path.Combine(_historicFilePath, newOriginalFileName);
        System.IO.File.Move(originalFilePath, newOriginalFilePath);
        logBuilder.AppendLine($"Moved original file to historic: {newOriginalFilePath}");
        _logger.LogInformation($"Moved original file to historic: {newOriginalFilePath}");

        // Move converted file
        var textFileName = Path.GetFileNameWithoutExtension(textFilePath);
        var textExtension = Path.GetExtension(textFilePath);
        var newTextFileName = $"{textFileName}_{timestamp}{textExtension}";
        var newTextFilePath = Path.Combine(_historicFilePath, newTextFileName);
        System.IO.File.Move(textFilePath, newTextFilePath);
        logBuilder.AppendLine($"Moved converted file to historic: {newTextFilePath}");
        _logger.LogInformation($"Moved converted file to historic: {newTextFilePath}");
    }

    private async Task D8_WriteLog(string logContent, string logPath)
    {
        var historicLogPath = Path.Combine(_historicFilePath, $"BulkLoad_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.log");
        if (System.IO.File.Exists(logPath))
        {
            System.IO.File.Move(logPath, historicLogPath);
        }
        await System.IO.File.WriteAllTextAsync(logPath, logContent);
    }
}


