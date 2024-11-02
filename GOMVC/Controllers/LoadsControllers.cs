using GOMVC.Data;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Microsoft.EntityFrameworkCore;

public class LoadsController : Controller
{
    private readonly AppDbContext _context;
    private readonly ILogger<LoadsController> _logger;

    public LoadsController(AppDbContext context, ILogger<LoadsController> logger)
    {
        _context = context;
        _logger = logger;
    }

    [HttpGet]
    public IActionResult Index()
    {
        return View();
    }

    [HttpPost]
    public async Task<IActionResult> ManualLoad(IFormFile file)
    {
        if (file != null && file.Length > 0)
        {
            var filePath = Path.GetTempFileName();
            try
            {
                // Save the uploaded file to a temporary location
                using (var stream = new FileStream(filePath, FileMode.Create))
                {
                    await file.CopyToAsync(stream);
                }

                // Execute MySQL bulk load query using LOAD DATA LOCAL INFILE
                var connectionString = _context.Database.GetDbConnection().ConnectionString;
                using (var connection = new MySqlConnection(connectionString))
                {
                    await connection.OpenAsync();
                    var query = $"LOAD DATA LOCAL INFILE '{filePath.Replace("\\", "\\\\")}' " +
                                "INTO TABLE Stage_Saldos_Cartera " +
                                "FIELDS TERMINATED BY '|' " +
                                "ENCLOSED BY '\"' " +
                                "LINES TERMINATED BY '\n' " +
                                "IGNORE 1 LINES;"; // Adjust as needed for your CSV format

                    _logger.LogInformation("Executing query: {Query}", query);

                    using (var command = new MySqlCommand(query, connection))
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                }

                _logger.LogInformation("File processed and data inserted successfully.");
                TempData["SuccessMessage"] = "File processed and data inserted successfully.";
                return RedirectToAction("Index");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred while processing the file.");
                ModelState.AddModelError("", "An error occurred while processing the file. Please check the logs for more details.");
            }
            finally
            {
                // Clean up the temporary file
                if (System.IO.File.Exists(filePath))
                {
                    System.IO.File.Delete(filePath);
                }
            }
        }
        else
        {
            ModelState.AddModelError("", "Please select a file to upload.");
        }
        return View("Index");
    }
}
