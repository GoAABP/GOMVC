using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MimeKit;
using MailKit.Net.Smtp;
using MailKit.Security;
using MySql.Data.MySqlClient;
using System;
using System.Text;
using System.Threading.Tasks;

public class C6_Resultados_Avances_Controller : Controller
{
    private readonly ILogger<C6_Resultados_Avances_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;

#pragma warning disable CS8618
    public C6_Resultados_Avances_Controller(ILogger<C6_Resultados_Avances_Controller> logger, IConfiguration configuration)
#pragma warning restore CS8618
    {
        _logger = logger;
        _configuration = configuration;
#pragma warning disable CS8601 // Possible null reference assignment.
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
#pragma warning restore CS8601 // Possible null reference assignment.
    }

    // First-time execution
    public async Task C6_First_Time_Resultados_Execution()
    {
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting First Time Resultados Execution.");
        _logger.LogInformation("Starting First Time Resultados Execution.");

        var insertIntoC6 = @"
            INSERT INTO C6_Catalogo_Resultados_Avances (Resultado)
            SELECT DISTINCT Resultado
            FROM D5_Gestiones
            WHERE Resultado IS NOT NULL AND Resultado <> ''
            AND Resultado NOT IN (SELECT Resultado FROM C6_Catalogo_Resultados_Avances);";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var insertCommand = new MySqlCommand(insertIntoC6, connection, transaction);
                    var affectedRows = await insertCommand.ExecuteNonQueryAsync();

                    logBuilder.AppendLine($"Inserted {affectedRows} distinct resultados into C6_Catalogo_Resultados_Avances.");
                    _logger.LogInformation($"Inserted {affectedRows} distinct resultados into C6_Catalogo_Resultados_Avances.");

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during execution: {ex.Message}");
                    _logger.LogError(ex, "Error during execution.");
                    throw;
                }
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - First Time Resultados Execution completed.");
        _logger.LogInformation("First Time Resultados Execution completed.");
    }

    // Browse for new resultados
    public async Task C6_Browse_Resultados()
    {
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting Browse Resultados.");
        _logger.LogInformation("Starting Browse Resultados.");

        var queryNewResultados = @"
            SELECT DISTINCT Resultado
            FROM D5_Gestiones
            WHERE Resultado IS NOT NULL AND Resultado <> ''
            AND Resultado NOT IN (SELECT Resultado FROM C6_Catalogo_Resultados_Avances);";

        var insertNewResultados = @"
            INSERT INTO C6_Catalogo_Resultados_Avances (Resultado)
            SELECT DISTINCT Resultado
            FROM D5_Gestiones
            WHERE Resultado IS NOT NULL AND Resultado <> ''
            AND Resultado NOT IN (SELECT Resultado FROM C6_Catalogo_Resultados_Avances);";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    // Fetch new resultados
                    var selectCommand = new MySqlCommand(queryNewResultados, connection, transaction);
                    using var reader = await selectCommand.ExecuteReaderAsync();

                    var newResultadosList = new StringBuilder();
                    int newResultadosCount = 0;

                    while (await reader.ReadAsync())
                    {
                        newResultadosList.AppendLine(reader.GetString(0));
                        newResultadosCount++;
                    }

                    reader.Close();

                    if (newResultadosCount > 0)
                    {
                        // Insert new resultados into C6_Catalogo_Resultados_Avances
                        var insertCommand = new MySqlCommand(insertNewResultados, connection, transaction);
                        await insertCommand.ExecuteNonQueryAsync();

                        logBuilder.AppendLine($"Inserted {newResultadosCount} new resultados into C6_Catalogo_Resultados_Avances.");
                        _logger.LogInformation($"Inserted {newResultadosCount} new resultados into C6_Catalogo_Resultados_Avances.");

                        // Send email notification
                        var emailMessage = $"New resultados have been added:\n\n{newResultadosCount} resultados added:\n{newResultadosList}";
                        await SendEmailAlert(emailMessage);

                        logBuilder.AppendLine("Email alert sent.");
                        _logger.LogInformation("Email alert sent.");
                    }
                    else
                    {
                        logBuilder.AppendLine("No new resultados found. No email sent.");
                        _logger.LogInformation("No new resultados found. No email sent.");
                    }

                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    logBuilder.AppendLine($"Error during execution: {ex.Message}");
                    _logger.LogError(ex, "Error during execution.");
                    throw;
                }
            }
        }

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Browse Resultados completed.");
        _logger.LogInformation("Browse Resultados completed.");
    }

    private async Task SendEmailAlert(string message)
    {
        var emailMessage = new MimeMessage();
        emailMessage.From.Add(new MailboxAddress("Go Credit Notifications", "gomvc.notice@gmail.com"));
        emailMessage.To.Add(new MailboxAddress("Alfredo Bueno", "alfredo.bueno@gocredit.mx"));
        emailMessage.Subject = "Alert: New Resultados Added";
        emailMessage.Body = new TextPart("plain")
        {
            Text = message
        };

        using (var client = new MailKit.Net.Smtp.SmtpClient())
        {
            await client.ConnectAsync("smtp.gmail.com", 587, SecureSocketOptions.StartTls);
            await client.AuthenticateAsync("gomvc.notice@gmail.com", "rnbn ugwd jwgu znav");
            await client.SendAsync(emailMessage);
            await client.DisconnectAsync(true);
        }
    }
}
