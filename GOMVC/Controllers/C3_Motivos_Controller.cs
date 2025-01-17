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

public class C3_Motivos_Controller : Controller
{
    private readonly ILogger<C3_Motivos_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    public C3_Motivos_Controller(ILogger<C3_Motivos_Controller> logger, IConfiguration configuration)
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    {
        _logger = logger;
        _configuration = configuration;
#pragma warning disable CS8601 // Possible null reference assignment.
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
#pragma warning restore CS8601 // Possible null reference assignment.
    }

    public async Task C3_First_Time_Motivo_Execution()
    {
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting First Time Motivo Execution.");
        _logger.LogInformation("Starting First Time Motivo Execution.");
        var insertIntoC3 = @"
            INSERT INTO C3_Motivo (Motivo) 
            SELECT DISTINCT Motivo 
            FROM D1_Saldos_Cartera 
            WHERE Motivo IS NOT NULL AND Motivo <> ''
            AND Motivo NOT IN (SELECT Motivo FROM C3_Motivo);";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    // Execute the insert query
                    var insertCommand = new MySqlCommand(insertIntoC3, connection, transaction);
                    var affectedRows = await insertCommand.ExecuteNonQueryAsync();

                    logBuilder.AppendLine($"Inserted {affectedRows} distinct motivos into C3_Motivo.");
                    _logger.LogInformation($"Inserted {affectedRows} distinct motivos into C3_Motivo.");

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

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - First Time Motivo Execution completed.");
        _logger.LogInformation("First Time Motivo Execution completed.");
    }

    public async Task C3_Browse_Motivos()
    {
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting Browse Motivos.");
        _logger.LogInformation("Starting Browse Motivos.");

        var queryNewMotivos = @"
            SELECT DISTINCT Motivo 
            FROM D1_Saldos_Cartera 
            WHERE Motivo IS NOT NULL AND Motivo <> ''
            AND Motivo NOT IN (SELECT Motivo FROM C3_Motivo);";

        var insertNewMotivos = @"
            INSERT INTO C3_Motivo (Motivo) 
            SELECT DISTINCT Motivo 
            FROM D1_Saldos_Cartera 
            WHERE Motivo IS NOT NULL AND Motivo <> ''
            AND Motivo NOT IN (SELECT Motivo FROM C3_Motivo);";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    // Fetch new motivos
                    var selectCommand = new MySqlCommand(queryNewMotivos, connection, transaction);
                    using var reader = await selectCommand.ExecuteReaderAsync();

                    var newMotivos = new StringBuilder();
                    int newMotivosCount = 0;

                    while (await reader.ReadAsync())
                    {
                        newMotivos.AppendLine(reader.GetString(0));
                        newMotivosCount++;
                    }

                    reader.Close();

                    if (newMotivosCount > 0)
                    {
                        // Insert new motivos into C3_Motivo
                        var insertCommand = new MySqlCommand(insertNewMotivos, connection, transaction);
                        await insertCommand.ExecuteNonQueryAsync();

                        logBuilder.AppendLine($"Inserted {newMotivosCount} new motivos into C3_Motivo.");
                        _logger.LogInformation($"Inserted {newMotivosCount} new motivos into C3_Motivo.");

                        // Build email message
                        var emailMessage = $"New motivos have been added:\n\n{newMotivosCount} motivos added:\n{newMotivos}";
                        await SendEmailAlert(emailMessage);

                        logBuilder.AppendLine("Email alert sent.");
                        _logger.LogInformation("Email alert sent.");
                    }
                    else
                    {
                        logBuilder.AppendLine("No new motivos found. No email sent.");
                        _logger.LogInformation("No new motivos found. No email sent.");
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

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Browse Motivos completed.");
        _logger.LogInformation("Browse Motivos completed.");
    }

    private async Task SendEmailAlert(string message)
    {
        var emailMessage = new MimeMessage();
        emailMessage.From.Add(new MailboxAddress("Your Name", "gomvc.notice@gmail.com"));
        emailMessage.To.Add(new MailboxAddress("Alfredo Bueno", "alfredo.bueno@gocredit.mx"));
        emailMessage.Subject = "Alert: New Motivos Added";
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
