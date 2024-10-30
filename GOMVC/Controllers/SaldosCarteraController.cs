using Microsoft.AspNetCore.Mvc;
using GOMVC.Data;
using GOMVC.Models;
using System.Linq;

namespace GOMVC.Controllers
{
    public class SaldosCarteraController(AppDbContext context) : Controller
    {
        private readonly AppDbContext _context = context;

        public IActionResult Index(int pageNumber = 1, int pageSize = 10)
        {
            var totalItems = _context.Saldos_Cartera.Count();
            var saldosCartera = _context.Saldos_Cartera
                .Skip((pageNumber - 1) * pageSize)
                .Take(pageSize)
                .ToList();

            var viewModel = new SaldosCarteraViewModel
            {
                SaldosCartera = saldosCartera,
                PageNumber = pageNumber,
                PageSize = pageSize,
                TotalItems = totalItems
            };

            return View("~/Views/Saldos_cartera/Index.cshtml", viewModel);
        }

        // Add other actions for Create, Edit, Details, and Delete as needed
    }
}
