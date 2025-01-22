using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using ClosedXML.Excel;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

public class D5_Gestiones_Controller : Controller
{
    private readonly ILogger<D5_Gestiones_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";
    private readonly string _logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D5_Gestiones_Bulk.log";

    public D5_Gestiones_Controller(ILogger<D5_Gestiones_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
    }

    public async Task<IActionResult> D5_ProcessFile()
    {
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.");
        _logger.LogInformation("Process started.");

        var files = Directory.GetFiles(_filePath, "Re_GestionesRO_*.xlsx");
        if (files.Length == 0)
        {
            logBuilder.AppendLine("File not found.");
            _logger.LogError("File not found.");
            await D5_WriteLog(logBuilder.ToString());
            return NotFound("No files found.");
        }

        var file = files[0];
        logBuilder.AppendLine($"File found: {file}");

        try
        {
            await D5_LoadDataToStage(file, logBuilder);
            await D5_ExecuteInsertFromStagingTable(logBuilder);
            D5_MoveFileToHistoric(file, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error during processing: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await D5_WriteLog(logBuilder.ToString());
            return StatusCode(500, "Error during processing.");
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.");
        _logger.LogInformation("Process completed successfully.");
        await D5_WriteLog(logBuilder.ToString());
        return Ok("File processed successfully.");
    }

    private async Task D5_LoadDataToStage(string filePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D5_Stage_Gestiones;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D5_Stage_Gestiones.");
                    _logger.LogInformation("Truncated table D5_Stage_Gestiones.");

                    using (var workbook = new XLWorkbook(filePath))
                    {
                        var worksheet = workbook.Worksheet(1);
                        var rows = worksheet.RowsUsed();

                        foreach (var row in rows.Skip(1))
                        {
                            try
                            {
                                var insertCommand = new MySqlCommand(@"
                                    INSERT INTO D5_Stage_Gestiones (
                                        Indice, Agencia_Registro, Causa_No_Pago, Causa_No_Domiciliacion, Codigo_Accion, Codigo_Resultado,
                                        Comentarios, Contacto_Generado, Coordenadas, Credito, Estatus_Promesa, Fecha_Actividad,
                                        Fecha_Promesa, Monto_Promesa, Origen, Producto, Resultado, Telefono, Tipo_Pago, Usuario_Registro
                                    ) VALUES (
                                        @Indice, @Agencia_Registro, @Causa_No_Pago, @Causa_No_Domiciliacion, @Codigo_Accion, @Codigo_Resultado,
                                        @Comentarios, @Contacto_Generado, @Coordenadas, @Credito, @Estatus_Promesa, @Fecha_Actividad,
                                        @Fecha_Promesa, @Monto_Promesa, @Origen, @Producto, @Resultado, @Telefono, @Tipo_Pago, @Usuario_Registro
                                    );", connection, transaction);

                                insertCommand.Parameters.AddWithValue("@Indice", row.Cell(1).GetValue<int>());
                                insertCommand.Parameters.AddWithValue("@Agencia_Registro", row.Cell(2).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Causa_No_Pago", row.Cell(3).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Causa_No_Domiciliacion", row.Cell(4).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Codigo_Accion", row.Cell(5).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Codigo_Resultado", row.Cell(6).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Comentarios", row.Cell(7).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Contacto_Generado", row.Cell(8).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Coordenadas", row.Cell(9).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Credito", row.Cell(10).GetValue<int>());
                                insertCommand.Parameters.AddWithValue("@Estatus_Promesa", row.Cell(11).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Fecha_Actividad", FormatDateTime(row.Cell(12).GetValue<string>(), logBuilder));
                                insertCommand.Parameters.AddWithValue("@Fecha_Promesa", FormatDate(row.Cell(13).GetValue<string>(), logBuilder));
                                insertCommand.Parameters.AddWithValue("@Monto_Promesa", row.Cell(14).GetValue<decimal?>());
                                insertCommand.Parameters.AddWithValue("@Origen", row.Cell(15).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Producto", row.Cell(16).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Resultado", row.Cell(17).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Telefono", row.Cell(18).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Tipo_Pago", row.Cell(19).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Usuario_Registro", row.Cell(20).GetValue<string>());

                                await insertCommand.ExecuteNonQueryAsync();
                            }
                            catch (Exception ex)
                            {
                                logBuilder.AppendLine($"Error processing row {row.RowNumber()}: {ex.Message}");
                                _logger.LogWarning($"Error processing row {row.RowNumber()}: {ex.Message}");
                            }
                        }
                    }

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error loading data into stage table: {ex.Message}");
                    _logger.LogError(ex, "Error loading data into stage table.");
                    throw;
                }
            }
        }
    }

    private async Task D5_ExecuteInsertFromStagingTable(StringBuilder logBuilder)
    {
        var sqlInsertCommand = @"
            INSERT INTO D5_Gestiones (
                Agencia_Registro, Causa_No_Pago, Causa_No_Domiciliacion, Codigo_Accion, Codigo_Resultado,
                Comentarios, Contacto_Generado, Coordenadas, Credito, Estatus_Promesa, Fecha_Actividad,
                Fecha_Promesa, Monto_Promesa, Origen, Producto, Resultado, Telefono, Tipo_Pago, Usuario_Registro
            )
            SELECT 
                s.Agencia_Registro, s.Causa_No_Pago, s.Causa_No_Domiciliacion, s.Codigo_Accion, s.Codigo_Resultado,
                s.Comentarios, s.Contacto_Generado, s.Coordenadas, s.Credito, s.Estatus_Promesa, s.Fecha_Actividad,
                s.Fecha_Promesa, s.Monto_Promesa, s.Origen, s.Producto, s.Resultado, s.Telefono, s.Tipo_Pago, s.Usuario_Registro
            FROM D5_Stage_Gestiones s
            WHERE NOT EXISTS (
                SELECT 1 
                FROM D5_Gestiones d
                WHERE 
                    s.Indice = d.Indice AND
                    s.Agencia_Registro = d.Agencia_Registro AND
                    s.Causa_No_Pago = d.Causa_No_Pago AND
                    s.Causa_No_Domiciliacion = d.Causa_No_Domiciliacion AND
                    s.Codigo_Accion = d.Codigo_Accion AND
                    s.Codigo_Resultado = d.Codigo_Resultado AND
                    s.Fecha_Actividad = d.Fecha_Actividad
            );";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var command = new MySqlCommand(sqlInsertCommand, connection, transaction);
                    await command.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Inserted only new rows from D5_Stage_Gestiones into D5_Gestiones.");
                    _logger.LogInformation("Inserted only new rows from D5_Stage_Gestiones into D5_Gestiones.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during insert: {ex.Message}");
                    _logger.LogError(ex, "Error during insert.");
                    throw;
                }
            }
        }
    }

    private void D5_MoveFileToHistoric(string filePath, StringBuilder logBuilder)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");
        var fileName = Path.GetFileNameWithoutExtension(filePath);
        var fileExtension = Path.GetExtension(filePath);
        var historicFilePath = Path.Combine(_historicFilePath, $"{fileName}_{timestamp}{fileExtension}");

        try
        {
            System.IO.File.Move(filePath, historicFilePath);
            logBuilder.AppendLine($"Moved file to historic: {historicFilePath}");
            _logger.LogInformation($"Moved file to historic: {historicFilePath}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error moving file to historic: {ex.Message}");
            _logger.LogError(ex, "Error moving file to historic.");
            throw;
        }
    }

    private async Task D5_WriteLog(string logContent)
    {
        var historicLogPath = Path.Combine(_historicFilePath, $"D5_Gestiones_Log_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.log");

        if (System.IO.File.Exists(_logPath))
        {
            System.IO.File.Move(_logPath, historicLogPath);
            _logger.LogInformation($"Moved existing log to historic: {historicLogPath}");
        }

        await System.IO.File.WriteAllTextAsync(_logPath, logContent);
        _logger.LogInformation($"New log written to: {_logPath}");
    }

    private string FormatDateTime(string date, StringBuilder logBuilder)
    {
        if (string.IsNullOrWhiteSpace(date)) return null;

        try
        {
            var parsedDateTime = DateTime.ParseExact(date.Trim(), "dd/MM/yyyy HH:mm:ss", null);
            return parsedDateTime.ToString("yyyy-MM-dd HH:mm:ss");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error parsing datetime '{date}': {ex.Message}");
            return null;
        }
    }

    private string FormatDate(string date, StringBuilder logBuilder)
    {
        if (string.IsNullOrWhiteSpace(date)) return null;

        try
        {
            var parsedDate = DateTime.ParseExact(date.Trim(), "dd/MM/yyyy", null);
            return parsedDate.ToString("yyyy-MM-dd");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error parsing date '{date}': {ex.Message}");
            return null;
        }
    }
}
