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

public class C4_Bancos_Controller : Controller
{
    private readonly ILogger<C4_Bancos_Controller> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;

#pragma warning disable CS8618
    public C4_Bancos_Controller(ILogger<C4_Bancos_Controller> logger, IConfiguration configuration)
#pragma warning restore CS8618
    {
        _logger = logger;
        _configuration = configuration;
#pragma warning disable CS8601 // Possible null reference assignment.
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
#pragma warning restore CS8601 // Possible null reference assignment.
    }

    // First-time execution
    public async Task C4_First_Time_Bancos_Execution()
    {
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting First Time Bancos Execution.");
        _logger.LogInformation("Starting First Time Bancos Execution.");

        var insertIntoC4 = @"
            INSERT INTO C4_Catalogo_Bancos (Clabe)
            SELECT DISTINCT SUBSTRING(clabe, 2, 3)
            FROM D1_Saldos_Cartera
            WHERE clabe IS NOT NULL AND clabe <> ''
            AND SUBSTRING(clabe, 2, 3) NOT IN (SELECT Clabe FROM C4_Catalogo_Bancos);";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    var insertCommand = new MySqlCommand(insertIntoC4, connection, transaction);
                    var affectedRows = await insertCommand.ExecuteNonQueryAsync();

                    logBuilder.AppendLine($"Inserted {affectedRows} distinct clabe values into C4_Catalogo_Bancos.");
                    _logger.LogInformation($"Inserted {affectedRows} distinct clabe values into C4_Catalogo_Bancos.");

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

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - First Time Bancos Execution completed.");
        _logger.LogInformation("First Time Bancos Execution completed.");
    }

    // Browse for new clabe values
    public async Task C4_Browse_Bancos()
    {
        var logBuilder = new StringBuilder();
        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Starting Browse Bancos.");
        _logger.LogInformation("Starting Browse Bancos.");

        var queryNewClabe = @"
            SELECT DISTINCT SUBSTRING(clabe, 2, 3) AS NewClabe
            FROM D1_Saldos_Cartera
            WHERE clabe IS NOT NULL AND clabe <> ''
            AND SUBSTRING(clabe, 2, 3) NOT IN (SELECT Clabe FROM C4_Catalogo_Bancos);";

        var insertNewClabe = @"
            INSERT INTO C4_Catalogo_Bancos (Clabe)
            SELECT DISTINCT SUBSTRING(clabe, 2, 3)
            FROM D1_Saldos_Cartera
            WHERE clabe IS NOT NULL AND clabe <> ''
            AND SUBSTRING(clabe, 2, 3) NOT IN (SELECT Clabe FROM C4_Catalogo_Bancos);";

        using (var connection = new MySqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    // Fetch new clabe values
                    var selectCommand = new MySqlCommand(queryNewClabe, connection, transaction);
                    using var reader = await selectCommand.ExecuteReaderAsync();

                    var newClabeList = new StringBuilder();
                    int newClabeCount = 0;

                    while (await reader.ReadAsync())
                    {
                        newClabeList.AppendLine(reader.GetString(0));
                        newClabeCount++;
                    }

                    reader.Close();

                    if (newClabeCount > 0)
                    {
                        // Insert new clabe values
                        var insertCommand = new MySqlCommand(insertNewClabe, connection, transaction);
                        await insertCommand.ExecuteNonQueryAsync();

                        logBuilder.AppendLine($"Inserted {newClabeCount} new clabe values into C4_Catalogo_Bancos.");
                        _logger.LogInformation($"Inserted {newClabeCount} new clabe values into C4_Catalogo_Bancos.");

                        // Send email notification
                        var emailMessage = $"New clabe values have been added:\n\n{newClabeCount} clabe values added:\n{newClabeList}";
                        await SendEmailAlert(emailMessage);

                        logBuilder.AppendLine("Email alert sent.");
                        _logger.LogInformation("Email alert sent.");
                    }
                    else
                    {
                        logBuilder.AppendLine("No new clabe values found. No email sent.");
                        _logger.LogInformation("No new clabe values found. No email sent.");
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

        logBuilder.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Browse Bancos completed.");
        _logger.LogInformation("Browse Bancos completed.");
    }

    private async Task SendEmailAlert(string message)
    {
        var emailMessage = new MimeMessage();
        emailMessage.From.Add(new MailboxAddress("Go Credit Notifications", "gomvc.notice@gmail.com"));
        emailMessage.To.Add(new MailboxAddress("Alfredo Bueno", "alfredo.bueno@gocredit.mx"));
        emailMessage.Subject = "Alert: New Clabe Records Added";
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
