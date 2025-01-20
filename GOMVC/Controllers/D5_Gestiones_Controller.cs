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

    public D5_Gestiones_Controller(ILogger<D5_Gestiones_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;
    }

    public async Task<IActionResult> D5_ProcessFile()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadGestionesRO.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.");
        _logger.LogInformation("Process started.");

        var files = Directory.GetFiles(_filePath, "Re_GestionesRO_*.xlsx");
        if (files.Length == 0)
        {
            logBuilder.AppendLine("File not found.");
            _logger.LogError("File not found.");
            await D5_WriteLog(logBuilder.ToString(), logPath);
            return NotFound("No files found.");
        }

        var file = files[0];
        logBuilder.AppendLine($"File found: {file}");

        try
        {
            await D5_LoadDataToStage(file, logBuilder);
            await D5_ExecuteInsertFromStagingTable(logBuilder, logPath);
            D5_MoveFileToHistoric(file, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error during processing: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await D5_WriteLog(logBuilder.ToString(), logPath);
            return StatusCode(500, "Error during processing.");
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.");
        _logger.LogInformation("Process completed successfully.");
        await D5_WriteLog(logBuilder.ToString(), logPath);
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
                        var worksheet = workbook.Worksheet(1); // Assuming data is in the first sheet
                        var rows = worksheet.RowsUsed();

                        foreach (var row in rows.Skip(1)) // Skip header row
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
                                insertCommand.Parameters.AddWithValue("@Codigo_Accion", row.Cell(5).GetValue<long>()); // Use bigint
                                insertCommand.Parameters.AddWithValue("@Codigo_Resultado", row.Cell(6).GetValue<long>());
                                insertCommand.Parameters.AddWithValue("@Comentarios", row.Cell(7).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Contacto_Generado", row.Cell(8).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Coordenadas", row.Cell(9).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Credito", row.Cell(10).GetValue<int>()); // Use bigint
                                insertCommand.Parameters.AddWithValue("@Estatus_Promesa", row.Cell(11).GetValue<string>());
                                insertCommand.Parameters.AddWithValue("@Fecha_Actividad", FormatDateTime(row.Cell(12).GetValue<string>(), logBuilder)); // DateTime
                                insertCommand.Parameters.AddWithValue("@Fecha_Promesa", FormatDate(row.Cell(13).GetValue<string>(), logBuilder)); // Date
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

    private async Task D5_ExecuteInsertFromStagingTable(StringBuilder logBuilder, string logPath)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var insertCommandText = @"
                        INSERT INTO D5_Gestiones (
                            Agencia_Registro, Causa_No_Pago, Causa_No_Domiciliacion, Codigo_Accion, Codigo_Resultado,
                            Comentarios, Contacto_Generado, Coordenadas, Id_Credito, Estatus_Promesa, Fecha_Actividad,
                            Fecha_Promesa, Monto_Promesa, Origen, Producto, Resultado, Telefono, Tipo_Pago, Usuario_Registro
                        )
                        SELECT
                            Agencia_Registro, Causa_No_Pago, Causa_No_Domiciliacion, Codigo_Accion, Codigo_Resultado,
                            Comentarios, Contacto_Generado, Coordenadas, Credito, Estatus_Promesa,
                            STR_TO_DATE(Fecha_Actividad, '%Y-%m-%d %H:%i:%s'),
                            STR_TO_DATE(Fecha_Promesa, '%Y-%m-%d'),
                            Monto_Promesa, Origen, Producto, Resultado, Telefono, Tipo_Pago, Usuario_Registro
                        FROM D5_Stage_Gestiones;";

                    var insertCommand = new MySqlCommand(insertCommandText, connection, transaction);
                    await insertCommand.ExecuteNonQueryAsync();

                    logBuilder.AppendLine("Inserted data from D5_Stage_Gestiones into D5_Gestiones.");
                    _logger.LogInformation("Inserted data from D5_Stage_Gestiones into D5_Gestiones.");

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error inserting data into final table: {ex.Message}");
                    _logger.LogError(ex, "Error inserting data into final table.");
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
        var newFileName = $"{fileName}_{timestamp}{fileExtension}";
        var historicFilePath = Path.Combine(_historicFilePath, newFileName);

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

    private string FormatDateTime(string date, StringBuilder logBuilder)
    {
        if (string.IsNullOrWhiteSpace(date))
        {
            logBuilder?.AppendLine($"Invalid date (empty or null): {date}");
            return null;
        }

        try
        {
            var culture = new System.Globalization.CultureInfo("es-ES");
            var cleanedDate = date.Trim();

            if (DateTime.TryParseExact(cleanedDate, "dd/MM/yyyy hh:mm:ss tt", culture, System.Globalization.DateTimeStyles.None, out DateTime parsedDateTime))
            {
                return parsedDateTime.ToString("yyyy-MM-dd HH:mm:ss");
            }

            if (DateTime.TryParse(cleanedDate, culture, System.Globalization.DateTimeStyles.None, out parsedDateTime))
            {
                return parsedDateTime.ToString("yyyy-MM-dd HH:mm:ss");
            }
        }
        catch (Exception ex)
        {
            logBuilder?.AppendLine($"Unexpected error parsing datetime '{date}': {ex.Message}");
        }

        logBuilder?.AppendLine($"Failed to parse datetime: {date}");
        return null;
    }

    private string FormatDate(string date, StringBuilder logBuilder)
    {
        if (string.IsNullOrWhiteSpace(date))
        {
            logBuilder?.AppendLine($"Invalid date (empty or null): {date}");
            return null;
        }

        try
        {
            var culture = new System.Globalization.CultureInfo("es-ES");
            var cleanedDate = date.Trim();

            if (DateTime.TryParseExact(cleanedDate, "dd/MM/yyyy", culture, System.Globalization.DateTimeStyles.None, out DateTime parsedDate))
            {
                return parsedDate.ToString("yyyy-MM-dd");
            }

            if (DateTime.TryParse(cleanedDate, culture, System.Globalization.DateTimeStyles.None, out parsedDate))
            {
                return parsedDate.ToString("yyyy-MM-dd");
            }
        }
        catch (Exception ex)
        {
            logBuilder?.AppendLine($"Unexpected error parsing date '{date}': {ex.Message}");
        }

        logBuilder?.AppendLine($"Failed to parse date: {date}");
        return null;
    }

    private async Task D5_WriteLog(string logContent, string logPath)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");
        var uniqueLogPath = Path.Combine(
            Path.GetDirectoryName(logPath)!,
            $"{Path.GetFileNameWithoutExtension(logPath)}_{timestamp}{Path.GetExtension(logPath)}"
        );

        try
        {
            await System.IO.File.WriteAllTextAsync(uniqueLogPath, logContent);
            _logger.LogInformation($"Log written to: {uniqueLogPath}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error writing log.");
            throw;
        }
    }
}

       
