using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;

namespace GOMVC.Controllers
{
    public class LoadsController : Controller
    {
        public IActionResult ExecuteBatchFile()
        {
            string batFilePath = @"C:\Users\Go Credit\Documents\DATA\BIN\BulkLoadSaldosCartera.bat";

            try
            {
                ProcessStartInfo processInfo = new ProcessStartInfo(batFilePath)
                {
                    CreateNoWindow = true,
                    UseShellExecute = false
                };

#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
                using (Process process = Process.Start(processInfo))
                {
#pragma warning disable CS8602 // Dereference of a possibly null reference.
                    process.WaitForExit();
#pragma warning restore CS8602 // Dereference of a possibly null reference.
                }
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.

                return Ok("Batch file executed successfully.");
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Error executing batch file: {ex.Message}");
            }
        }
    }
}
