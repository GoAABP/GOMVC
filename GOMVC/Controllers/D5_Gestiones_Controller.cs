using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using OfficeOpenXml;
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
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
    }

    public async Task D5_ProcessGestiones()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\BulkLoadGestiones.log";
        var logBuilder = new StringBuilder();
        var startLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.";
        logBuilder.AppendLine(startLog);
        _logger.LogInformation(startLog);

        var files = Directory.GetFiles(_filePath, "Re_GestionesRO_*.xlsx");
        if (files.Length == 0)
        {
            var errorLog = "File not found.";
            logBuilder.AppendLine(errorLog);
            _logger.LogError(errorLog);
            await D5_WriteLog(logBuilder.ToString(), logPath);
            throw new FileNotFoundException(errorLog);
        }

        var file = files[0];
        logBuilder.AppendLine("File found.");

        try
        {
            var textFilePath = await D5_ConvertExcelToText(file, logBuilder);
            await D5_BulkInsertGestiones(textFilePath, logBuilder);
            await D5_ExecuteInsertGestiones(logBuilder, logPath);
            D5_MoveFilesToHistoric(file, textFilePath, logBuilder);
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await D5_WriteLog(logBuilder.ToString(), logPath);
            throw;
        }

        var endLog = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.";
        logBuilder.AppendLine(endLog);
        _logger.LogInformation(endLog);
        await D5_WriteLog(logBuilder.ToString(), logPath);
    }

    private async Task<string> D5_ConvertExcelToText(string excelFilePath, StringBuilder logBuilder)
    {
        var textFilePath = Path.ChangeExtension(excelFilePath, ".txt");
        var delimiter = "|";
        var sb = new StringBuilder();

        try
        {
            using (var package = new ExcelPackage(new FileInfo(excelFilePath)))
            {
                var worksheet = package.Workbook.Worksheets.First();
                var rowCount = worksheet.Dimension.Rows;
                var colCount = worksheet.Dimension.Columns;

                // Write the header
                for (int col = 2; col <= colCount; col++)
                {
                    sb.Append(worksheet.Cells[1, col].Text + delimiter);
                }
                sb.AppendLine();

                // Write the data rows
                for (int row = 2; row <= rowCount; row++)
                {
                    for (int col = 2; col <= colCount; col++)
                    {
                        var cellValue = worksheet.Cells[row, col].Text;
                        cellValue = cellValue.Replace("\r\n", " ").Replace("\n", " ").Replace("\r", " ");
                        sb.Append(cellValue + delimiter);
                    }
                    sb.AppendLine();
                }
            }

            await System.IO.File.WriteAllTextAsync(textFilePath, sb.ToString(), Encoding.UTF8);
            logBuilder.AppendLine($"Converted Excel to text: {textFilePath}");
            _logger.LogInformation($"Converted Excel to text: {textFilePath}");
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error during conversion: {ex.Message}");
            _logger.LogError(ex, $"Error during conversion: {ex.Message}");
            throw;
        }

        return textFilePath;
    }

    private async Task D5_BulkInsertGestiones(string textFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE Stage_Gestiones;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table Stage_Gestiones.");
                    _logger.LogInformation("Truncated table Stage_Gestiones.");

                    var loadCommandText = $"LOAD DATA LOCAL INFILE '{textFilePath.Replace("\\", "\\\\")}' " +
                                          "INTO TABLE Stage_Gestiones " +
                                          "FIELDS TERMINATED BY '|' " +
                                          "ENCLOSED BY '\"' " +
                                          "LINES TERMINATED BY '\\n' " +
                                          "IGNORE 1 LINES;";
                    var loadCommand = new MySqlCommand(loadCommandText, connection, transaction);
                    await loadCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Bulk inserted data into Stage_Gestiones.");
                    _logger.LogInformation("Bulk inserted data into Stage_Gestiones.");

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during bulk insert: {ex.Message}");
                    _logger.LogError(ex, $"Error during bulk insert: {ex.Message}");
                    throw;
                }
            }
        }
    }

    private async Task D5_ExecuteInsertGestiones(StringBuilder logBuilder, string logPath)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var insertCommandText = @"
                        INSERT INTO Gestiones (
                            AgenciaRegistro, CausaNoPago, CausaNoDomiciliacion, CodigoAccion, CodigoResultado, Comentarios,
                            ContactoGenerado, Coordenadas, Credito, EstatusPromesa, FechaActividad, FechaPromesa, MontoPromesa,
                            Origen, Producto, Resultado, Telefono, TipoPago, UsuarioRegistro
                        )
                        SELECT 
                            AgenciaRegistro, CausaNoPago, CausaNoDomiciliacion, CodigoAccion, CodigoResultado, Comentarios,
                            ContactoGenerado, Coordenadas, Credito, EstatusPromesa, STR_TO_DATE(FechaActividad, '%Y-%m-%d %H:%i:%s'),
                            STR_TO_DATE(FechaPromesa, '%Y-%m-%d %H:%i:%s'), MontoPromesa, Origen, Producto, Resultado, Telefono,
                            TipoPago, UsuarioRegistro
                        FROM Stage_Gestiones;
                    ";

                    var insertCommand = new MySqlCommand(insertCommandText, connection, transaction);
                    await insertCommand.ExecuteNonQueryAsync();

                    logBuilder.AppendLine("Insert successful.");
                    _logger.LogInformation("Insert successful.");

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error executing insert: {ex.Message}");
                    _logger.LogError(ex, $"Error executing insert: {ex.Message}");
                    throw;
                }
            }
        }
    }

    private void D5_MoveFilesToHistoric(string originalFilePath, string textFilePath, StringBuilder logBuilder)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");

        // Move original file
        var originalFileName = Path.GetFileNameWithoutExtension(originalFilePath);
        var originalExtension = Path.GetExtension(originalFilePath);
        var newOriginalFileName = $"{originalFileName}_{timestamp}{originalExtension}";
        var newOriginalFilePath = Path.Combine(_historicFilePath, newOriginalFileName);

        System.IO.File.Move(originalFilePath, newOriginalFilePath);
    }

    private async Task D5_WriteLog(string logContent, string logPath)
    {
        var historicLogPath = Path.Combine(_historicFilePath, $"BulkLoad_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.log");
        if (System.IO.File.Exists(logPath))
        {
            // Move the existing log file to the historic folder
            System.IO.File.Move(logPath, historicLogPath);
        }
        // Write the new log content
        await System.IO.File.WriteAllTextAsync(logPath, logContent);
    }

}
