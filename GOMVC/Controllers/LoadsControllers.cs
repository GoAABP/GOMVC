using GOMVC.Data;
using Microsoft.AspNetCore.Mvc;

public class LoadsController : Controller
{
    private readonly AppDbContext _context;

    public LoadsController(AppDbContext context)
    {
        _context = context;
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
            using (var stream = new StreamReader(file.OpenReadStream()))
            {
                var csvData = await stream.ReadToEndAsync();
                // Parse CSV data and insert into Stage_Saldos_Cartera table
                // Then move data from Stage_Saldos_Cartera to Saldos_Cartera
            }
        }
        return RedirectToAction("Index");
    }
}