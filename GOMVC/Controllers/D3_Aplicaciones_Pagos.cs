using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

public class D3_Aplicaciones_Pagos_Controller : Controller
{
    private readonly ILogger<D3_Aplicaciones_Pagos_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";

    public D3_Aplicaciones_Pagos_Controller(ILogger<D3_Aplicaciones_Pagos_Controller> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection")!;

        // Register encoding provider for special character handling
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
    }

    public async Task<IActionResult> D3_ProcessAplicacionPagos()
    {
        var logPath = @"C:\Users\Go Credit\Documents\DATA\LOGS\D3_Aplicaciones_Pagos.log";
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process started.");
        _logger.LogInformation("Process started.");

        var files = Directory.GetFiles(_filePath, "Aplicacion de pagos por fecha de Aplica*.csv");
        if (files.Length == 0)
        {
            logBuilder.AppendLine("File not found.");
            _logger.LogError("File not found.");
            await WriteLog(logBuilder.ToString(), logPath);
            return NotFound("No files found.");
        }

        var file = files[0];
        logBuilder.AppendLine($"File found: {file}");

        try
        {
            // Convert file encoding to UTF-8 with BOM
            var convertedFilePath = ConvertToUTF8WithBOM(file);
            logBuilder.AppendLine($"Converted file encoding to UTF-8 with BOM: {convertedFilePath}");

            await D3_LoadDataToStageWithCondition(convertedFilePath, logBuilder);
            await D3_ExecuteAplicacionPagosInsertFromStagingTable(logBuilder);

            MoveFileToHistoric(file, logBuilder); // Move original file to historic
            MoveFileToHistoric(convertedFilePath, logBuilder); // Move converted file to historic
        }
        catch (Exception ex)
        {
            logBuilder.AppendLine($"Error: {ex.Message}");
            _logger.LogError(ex, "Error during processing.");
            await WriteLog(logBuilder.ToString(), logPath);
            return StatusCode(500, "Error during processing.");
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Process completed successfully.");
        _logger.LogInformation("Process completed successfully.");
        await WriteLog(logBuilder.ToString(), logPath);
        return Ok("File processed successfully.");
    }

    private async Task D3_LoadDataToStageWithCondition(string csvFilePath, StringBuilder logBuilder)
    {
        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var truncateCommand = new MySqlCommand("TRUNCATE TABLE D3_Stage_Aplicacion_Pagos;", connection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync();
                    logBuilder.AppendLine("Truncated table D3_Stage_Aplicacion_Pagos.");
                    _logger.LogInformation("Truncated table D3_Stage_Aplicacion_Pagos.");

                    using (var reader = new StreamReader(csvFilePath, Encoding.UTF8))
                    {
                        string line;
                        bool isHeader = true;

                        while (!reader.EndOfStream)
                        {
                            line = await reader.ReadLineAsync();

                            // Skip header row
                            if (isHeader)
                            {
                                isHeader = false;
                                continue;
                            }

                            var values = line.Split(',');

                            // Stop processing if the first column contains "0"
                            if (values.Length > 0 && values[0].Trim() == "0")
                            {
                                logBuilder.AppendLine("Encountered row with '0' in the first column. Stopping further insertion.");
                                break;
                            }

                            try
                            {
                                var insertCommand = new MySqlCommand(@"
                                    INSERT INTO D3_Stage_Aplicacion_Pagos (
                                        Id_Credito, Id_Convenio, Convenio, Referencia, Id_Pago, Nombre_Cliente, Financiamiento,
                                        Origen_de_Movimiento, Fecha_Pago, Fecha_Aplicacion, Fecha_Deposito, Status, Pago, Capital,
                                        Interes, IVA_Int, Comision_Financiada, IVA_Comision_Financ, Moratorios, IVA_Mora, Pago_Tardio,
                                        IVA_PagoTardio, Recuperacion, IVA_Recup, Com_Liquidacion, IVA_Com_Liquidacion, Retencion_X_Admon,
                                        IVA_Retencion_X_Admon, Pago_Exceso, Gestor, Forma_de_pago, vMotive
                                    ) VALUES (
                                        @Id_Credito, @Id_Convenio, @Convenio, @Referencia, @Id_Pago, @Nombre_Cliente, @Financiamiento,
                                        @Origen_de_Movimiento, @Fecha_Pago, @Fecha_Aplicacion, @Fecha_Deposito, @Status, @Pago, @Capital,
                                        @Interes, @IVA_Int, @Comision_Financiada, @IVA_Comision_Financ, @Moratorios, @IVA_Mora, @Pago_Tardio,
                                        @IVA_PagoTardio, @Recuperacion, @IVA_Recup, @Com_Liquidacion, @IVA_Com_Liquidacion, @Retencion_X_Admon,
                                        @IVA_Retencion_X_Admon, @Pago_Exceso, @Gestor, @Forma_de_pago, @vMotive
                                    );", connection, transaction);

                                // Map each CSV column to SQL parameters
                                insertCommand.Parameters.AddWithValue("@Id_Credito", ParseInteger(values[0]));
                                insertCommand.Parameters.AddWithValue("@Id_Convenio", ParseInteger(values[1]));
                                insertCommand.Parameters.AddWithValue("@Convenio", ParseString(values[2]));
                                insertCommand.Parameters.AddWithValue("@Referencia", ParseString(values[3]));
                                insertCommand.Parameters.AddWithValue("@Id_Pago", ParseInteger(values[4]));
                                insertCommand.Parameters.AddWithValue("@Nombre_Cliente", ParseString(values[5]));
                                insertCommand.Parameters.AddWithValue("@Financiamiento", ParseString(values[6]));
                                insertCommand.Parameters.AddWithValue("@Origen_de_Movimiento", ParseString(values[7]));
                                insertCommand.Parameters.AddWithValue("@Fecha_Pago", ParseDate(values[8]));
                                insertCommand.Parameters.AddWithValue("@Fecha_Aplicacion", ParseDate(values[9]));
                                insertCommand.Parameters.AddWithValue("@Fecha_Deposito", ParseDate(values[10]));
                                insertCommand.Parameters.AddWithValue("@Status", ParseString(values[11]));
                                insertCommand.Parameters.AddWithValue("@Pago", ParseDecimal(values[12]));
                                insertCommand.Parameters.AddWithValue("@Capital", ParseDecimal(values[13]));
                                insertCommand.Parameters.AddWithValue("@Interes", ParseDecimal(values[14]));
                                insertCommand.Parameters.AddWithValue("@IVA_Int", ParseDecimal(values[15]));
                                insertCommand.Parameters.AddWithValue("@Comision_Financiada", ParseDecimal(values[16]));
                                insertCommand.Parameters.AddWithValue("@IVA_Comision_Financ", ParseDecimal(values[17]));
                                insertCommand.Parameters.AddWithValue("@Moratorios", ParseDecimal(values[18]));
                                insertCommand.Parameters.AddWithValue("@IVA_Mora", ParseDecimal(values[19]));
                                insertCommand.Parameters.AddWithValue("@Pago_Tardio", ParseDecimal(values[20]));
                                insertCommand.Parameters.AddWithValue("@IVA_PagoTardio", ParseDecimal(values[21]));
                                insertCommand.Parameters.AddWithValue("@Recuperacion", ParseDecimal(values[22]));
                                insertCommand.Parameters.AddWithValue("@IVA_Recup", ParseDecimal(values[23]));
                                insertCommand.Parameters.AddWithValue("@Com_Liquidacion", ParseDecimal(values[24]));
                                insertCommand.Parameters.AddWithValue("@IVA_Com_Liquidacion", ParseDecimal(values[25]));
                                insertCommand.Parameters.AddWithValue("@Retencion_X_Admon", ParseDecimal(values[26]));
                                insertCommand.Parameters.AddWithValue("@IVA_Retencion_X_Admon", ParseDecimal(values[27]));
                                insertCommand.Parameters.AddWithValue("@Pago_Exceso", ParseDecimal(values[28]));
                                insertCommand.Parameters.AddWithValue("@Gestor", ParseString(values[29]));
                                insertCommand.Parameters.AddWithValue("@Forma_de_pago", ParseString(values[30]));
                                insertCommand.Parameters.AddWithValue("@vMotive", ParseString(values[31]));

                                await insertCommand.ExecuteNonQueryAsync();
                            }
                            catch (Exception ex)
                            {
                                logBuilder.AppendLine($"Error processing row: {line} - {ex.Message}");
                                _logger.LogWarning($"Error processing row: {line} - {ex.Message}");
                            }
                        }
                    }

                    await transaction.CommitAsync();
                    logBuilder.AppendLine("Data loaded into D3_Stage_Aplicacion_Pagos.");
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error loading data into stage table: {ex.Message}");
                    throw;
                }
            }
        }
    }

    private async Task D3_ExecuteAplicacionPagosInsertFromStagingTable(StringBuilder logBuilder)
    {
        var sqlInsertCommand = @"
            INSERT INTO D3_Aplicacion_Pagos (
                Id_Credito, Id_Convenio, Convenio, Referencia, Id_Pago, Nombre_Cliente, Financiamiento,
                Origen_de_Movimiento, Fecha_Pago, Fecha_Aplicacion, Fecha_Deposito, Status, Pago, Capital,
                Interes, IVA_Int, Comision_Financiada, IVA_Comision_Financ, Moratorios, IVA_Mora, Pago_Tardio,
                IVA_PagoTardio, Recuperacion, IVA_Recup, Com_Liquidacion, IVA_Com_Liquidacion, Retencion_X_Admon,
                IVA_Retencion_X_Admon, Pago_Exceso, Gestor, Forma_de_pago, vMotive
            )
            SELECT 
                Id_Credito, Id_Convenio, Convenio, Referencia, Id_Pago, Nombre_Cliente, Financiamiento,
                Origen_de_Movimiento, Fecha_Pago, Fecha_Aplicacion, Fecha_Deposito, Status, Pago, Capital,
                Interes, IVA_Int, Comision_Financiada, IVA_Comision_Financ, Moratorios, IVA_Mora, Pago_Tardio,
                IVA_PagoTardio, Recuperacion, IVA_Recup, Com_Liquidacion, IVA_Com_Liquidacion, Retencion_X_Admon,
                IVA_Retencion_X_Admon, Pago_Exceso, Gestor, Forma_de_pago, vMotive
            FROM D3_Stage_Aplicacion_Pagos
            WHERE NOT EXISTS (
                SELECT 1 FROM D3_Aplicacion_Pagos WHERE D3_Aplicacion_Pagos.Id_Pago = D3_Stage_Aplicacion_Pagos.Id_Pago
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
                    logBuilder.AppendLine("Data inserted into D3_Aplicacion_Pagos.");
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error inserting data: {ex.Message}");
                    throw;
                }
            }
        }
    }

    private async Task WriteLog(string content, string logPath)
    {
        await System.IO.File.WriteAllTextAsync(logPath, content);
    }

    private void MoveFileToHistoric(string filePath, StringBuilder logBuilder)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");
        var historicFilePath = Path.Combine(_historicFilePath, $"{Path.GetFileNameWithoutExtension(filePath)}_{timestamp}{Path.GetExtension(filePath)}");
        System.IO.File.Move(filePath, historicFilePath);
        logBuilder.AppendLine($"Moved file to historic: {historicFilePath}");
    }

    private string ConvertToUTF8WithBOM(string filePath)
    {
        var newFilePath = Path.Combine(
            Path.GetDirectoryName(filePath)!,
            Path.GetFileNameWithoutExtension(filePath) + "_utf8" + Path.GetExtension(filePath)
        );

        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        var sourceEncoding = Encoding.GetEncoding("Windows-1252");
        using (var reader = new StreamReader(filePath, sourceEncoding))
        using (var writer = new StreamWriter(newFilePath, false, new UTF8Encoding(true))) // Add BOM
        {
            while (!reader.EndOfStream)
            {
                writer.WriteLine(reader.ReadLine()?.Replace("\"", "")); // Remove surrounding quotes
            }
        }

        return newFilePath;
    }

    private int? ParseInteger(string value) => int.TryParse(value, out var result) ? result : (int?)null;
    private decimal? ParseDecimal(string value) => decimal.TryParse(value, out var result) ? result : (decimal?)null;
    private string? ParseString(string value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    private DateTime? ParseDate(string value) => DateTime.TryParse(value, out var result) ? result : (DateTime?)null;
}
