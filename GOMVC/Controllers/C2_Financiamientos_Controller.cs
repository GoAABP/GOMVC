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

public class C2_Financiamientos_Controller : Controller
{
    private readonly ILogger<C2_Financiamientos_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    public C2_Financiamientos_Controller(ILogger<C2_Financiamientos_Controller> logger, IConfiguration configuration)
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    {
        _logger = logger;
        _configuration = configuration;
#pragma warning disable CS8601 // Possible null reference assignment.
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
#pragma warning restore CS8601 // Possible null reference assignment.
    }

    public async Task C2_First_Time_Financiamiento_Execution()
    {
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting First Time Financiamiento Execution.");
        _logger.LogInformation("Starting First Time Financiamiento Execution.");
        var insertIntoC2 = @"
            INSERT INTO C2_Financiamiento (Financiamiento) 
            SELECT DISTINCT Financiamiento 
            FROM D1_Saldos_Cartera 
            WHERE Financiamiento IS NOT NULL AND Financiamiento <> ''
            AND Financiamiento NOT IN (SELECT Financiamiento FROM C2_Financiamiento);";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    // Execute the insert query
                    var insertCommand = new MySqlCommand(insertIntoC2, connection, transaction);
                    var affectedRows = await insertCommand.ExecuteNonQueryAsync();

                    logBuilder.AppendLine($"Inserted {affectedRows} distinct financiamientos into C2_Financiamiento.");
                    _logger.LogInformation($"Inserted {affectedRows} distinct financiamientos into C2_Financiamiento.");

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

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - First Time Financiamiento Execution completed.");
        _logger.LogInformation("First Time Financiamiento Execution completed.");
    }

    public async Task C2_Browse_Financiamientos()
    {
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting Browse Financiamientos.");
        _logger.LogInformation("Starting Browse Financiamientos.");

        var queryNewFinanciamientos = @"
            SELECT DISTINCT Financiamiento 
            FROM D1_Saldos_Cartera 
            WHERE Financiamiento IS NOT NULL AND Financiamiento <> ''
            AND Financiamiento NOT IN (SELECT Financiamiento FROM C2_Financiamiento);";

        var insertNewFinanciamientos = @"
            INSERT INTO C2_Financiamiento (Financiamiento) 
            SELECT DISTINCT Financiamiento 
            FROM D1_Saldos_Cartera 
            WHERE Financiamiento IS NOT NULL AND Financiamiento <> ''
            AND Financiamiento NOT IN (SELECT Financiamiento FROM C2_Financiamiento);";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    // Fetch new financiamientos
                    var selectCommand = new MySqlCommand(queryNewFinanciamientos, connection, transaction);
                    using var reader = await selectCommand.ExecuteReaderAsync();

                    var newFinanciamientos = new StringBuilder();
                    int newFinanciamientosCount = 0;

                    while (await reader.ReadAsync())
                    {
                        newFinanciamientos.AppendLine(reader.GetString(0));
                        newFinanciamientosCount++;
                    }

                    reader.Close();

                    if (newFinanciamientosCount > 0)
                    {
                        // Insert new financiamientos into C2_Financiamiento
                        var insertCommand = new MySqlCommand(insertNewFinanciamientos, connection, transaction);
                        await insertCommand.ExecuteNonQueryAsync();

                        logBuilder.AppendLine($"Inserted {newFinanciamientosCount} new financiamientos into C2_Financiamiento.");
                        _logger.LogInformation($"Inserted {newFinanciamientosCount} new financiamientos into C2_Financiamiento.");

                        // Build email message
                        var emailMessage = $"New financiamientos have been added:\n\n{newFinanciamientosCount} financiamientos added:\n{newFinanciamientos}";
                        await SendEmailAlert(emailMessage);

                        logBuilder.AppendLine("Email alert sent.");
                        _logger.LogInformation("Email alert sent.");
                    }
                    else
                    {
                        logBuilder.AppendLine("No new financiamientos found. No email sent.");
                        _logger.LogInformation("No new financiamientos found. No email sent.");
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

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Browse Financiamientos completed.");
        _logger.LogInformation("Browse Financiamientos completed.");
    }

    private async Task SendEmailAlert(string message)
    {
        var emailMessage = new MimeMessage();
        emailMessage.From.Add(new MailboxAddress("Your Name", "gomvc.notice@gmail.com"));
        emailMessage.To.Add(new MailboxAddress("Alfredo Bueno", "alfredo.bueno@gocredit.mx"));
        emailMessage.Subject = "Alert: New Financiamientos Added";
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
