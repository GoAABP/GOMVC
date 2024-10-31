using Microsoft.AspNetCore.Mvc;
using GOMVC.Data;
using GOMVC.Models;
using System.Linq;

namespace GOMVC.Controllers
{
    public class SaldosCarteraController : Controller
    {
        private readonly AppDbContext _context;

        public SaldosCarteraController(AppDbContext context)
        {
            _context = context;
        }

        public IActionResult Index(int pageNumber = 1, int pageSize = 100, int? idCredito = null, int? idPersona = null, string? nombre = null)
        {
            var query = _context.Saldos_Cartera.AsQueryable();

            // Apply filters
            if (idCredito.HasValue)
            {
                query = query.Where(s => s.IdCredito == idCredito.Value);
            }

            if (idPersona.HasValue)
            {
                query = query.Where(s => s.IdPersona == idPersona.Value);
            }

            if (!string.IsNullOrEmpty(nombre))
            {
            #pragma warning disable CS8602 // Dereference of a possibly null reference.
                query = query.Where(s => s.Nombre.Contains(nombre));
            #pragma warning restore CS8602 // Dereference of a possibly null reference.
            }

            // Get the most recent date
            var mostRecentDate = query.Max(s => s.FechaGenerado);
            query = query.Where(s => s.FechaGenerado == mostRecentDate);

            var totalItems = query.Count();
            var saldosCartera = query
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

            // Pass filter values to the view
            ViewData["IdCredito"] = idCredito;
            ViewData["IdPersona"] = idPersona;
            ViewData["Nombre"] = nombre;

            return View("~/Views/Saldos_cartera/Index.cshtml", viewModel);
        }

        // Add other actions for Create, Edit, Details, and Delete as needed
    }
}
