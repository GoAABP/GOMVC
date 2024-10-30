using Microsoft.AspNetCore.Mvc;
using GOMVC.Data;
using System.Linq;

namespace GOMVC.Controllers
{
    public class SaldosPruebasController : Controller
    {
        private readonly AppDbContext _context;

        public SaldosPruebasController(AppDbContext context)
        {
            _context = context;
        }

        public IActionResult Index()
        {
            var data = _context.SaldosPruebas.ToList();
            return View(data);
        }
    }
}
