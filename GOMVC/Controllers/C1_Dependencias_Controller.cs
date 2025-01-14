using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using MimeKit;
using MailKit.Net.Smtp;
using MailKit.Security;
using System;
using System.Text;
using System.Threading.Tasks;

public class C1_Dependencias_Controller : Controller
{
    private readonly ILogger<C1_Dependencias_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    public C1_Dependencias_Controller(ILogger<C1_Dependencias_Controller> logger, IConfiguration configuration)
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    {
        _logger = logger;
        _configuration = configuration;
#pragma warning disable CS8601 // Possible null reference assignment.
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
#pragma warning restore CS8601 // Possible null reference assignment.
    }

    public async Task C1_First_Time_Dependencias_Execution()
    {
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting First Time Dependencias Execution.");
        _logger.LogInformation("Starting First Time Dependencias Execution.");
        var insertIntoC1 = @"
            INSERT INTO C1_Dependencia (Dependencia) 
            SELECT DISTINCT Dependencia 
            FROM D1_Saldos_Cartera 
            WHERE Dependencia IS NOT NULL AND Dependencia <> ''
            AND Dependencia NOT IN (SELECT Dependencia FROM C1_Dependencia);";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    // Execute the insert query
                    var insertCommand = new MySqlCommand(insertIntoC1, connection, transaction);
                    var affectedRows = await insertCommand.ExecuteNonQueryAsync();

                    logBuilder.AppendLine($"Inserted {affectedRows} distinct dependencias into C1_Dependencia.");
                    _logger.LogInformation($"Inserted {affectedRows} distinct dependencias into C1_Dependencia.");

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

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - First Time Dependencias Execution completed.");
        _logger.LogInformation("First Time Dependencias Execution completed.");
    }

    public async Task C1_Browse_Dependencias()
    {
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting Browse Dependencias.");
        _logger.LogInformation("Starting Browse Dependencias.");

        var queryNewDependencias = @"
            SELECT DISTINCT Dependencia 
            FROM D1_Saldos_Cartera 
            WHERE Dependencia IS NOT NULL AND Dependencia <> ''
            AND Dependencia NOT IN (SELECT Dependencia FROM C1_Dependencia);";

        var insertNewDependencias = @"
            INSERT INTO C1_Dependencia (Dependencia) 
            SELECT DISTINCT Dependencia 
            FROM D1_Saldos_Cartera 
            WHERE Dependencia IS NOT NULL AND Dependencia <> ''
            AND Dependencia NOT IN (SELECT Dependencia FROM C1_Dependencia);";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    // Fetch new dependencias
                    var selectCommand = new MySqlCommand(queryNewDependencias, connection, transaction);
                    using var reader = await selectCommand.ExecuteReaderAsync();

                    var newDependencias = new StringBuilder();
                    int newDependenciasCount = 0;

                    while (await reader.ReadAsync())
                    {
                        newDependencias.AppendLine(reader.GetString(0));
                        newDependenciasCount++;
                    }

                    reader.Close();

                    if (newDependenciasCount > 0)
                    {
                        // Insert new dependencias into C1_Dependencia
                        var insertCommand = new MySqlCommand(insertNewDependencias, connection, transaction);
                        await insertCommand.ExecuteNonQueryAsync();

                        logBuilder.AppendLine($"Inserted {newDependenciasCount} new dependencias into C1_Dependencia.");
                        _logger.LogInformation($"Inserted {newDependenciasCount} new dependencias into C1_Dependencia.");

                        // Build email message
                        var emailMessage = $"New dependencias have been added:\n\n{newDependenciasCount} dependencias added:\n{newDependencias}";
                        await SendEmailAlert(emailMessage);

                        logBuilder.AppendLine("Email alert sent.");
                        _logger.LogInformation("Email alert sent.");
                    }
                    else
                    {
                        logBuilder.AppendLine("No new dependencias found. No email sent.");
                        _logger.LogInformation("No new dependencias found. No email sent.");
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

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Browse Dependencias completed.");
        _logger.LogInformation("Browse Dependencias completed.");
    }

    private async Task SendEmailAlert(string message)
    {
        var emailMessage = new MimeMessage();
        emailMessage.From.Add(new MailboxAddress("Your Name", "gomvc.notice@gmail.com"));
        emailMessage.To.Add(new MailboxAddress("Alfredo Bueno", "alfredo.bueno@gocredit.mx"));
        emailMessage.Subject = "Alert: New Dependencies Added";
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
