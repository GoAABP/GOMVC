using Microsoft.AspNetCore.Mvc;
using GOMVC.Models;
using System.Linq;
using System.Text;
using Microsoft.AspNetCore.Authorization;
using GOMVC.Data;

namespace GOMVC.Controllers
{
    [Authorize]
    public class GestionesController : Controller
    {
        private readonly AppDbContext _context;

        public GestionesController(AppDbContext context)
        {
            _context = context;
        }

        public IActionResult Index(int pageNumber = 1, int pageSize = 100, int? idCredito = null, string? usuarioRegistro = null, bool clearFilters = false)
        {
            var query = _context.Gestiones.AsQueryable();

            // Apply filters
            if (idCredito.HasValue)
            {
                string idCreditoString = idCredito.Value.ToString();
                query = query.Where(s => s.Credito.ToString().Contains(idCreditoString));
            }
            if (!string.IsNullOrEmpty(usuarioRegistro))
            {
                query = query.Where(s => s.UsuarioRegistro.Contains(usuarioRegistro));
            }

            var totalItems = query.Count();
            var gestiones = query
                .Skip((pageNumber - 1) * pageSize)
                .Take(pageSize)
                .ToList();

            // Check for DBNull values and handle them
            foreach (var gestion in gestiones)
            {
                gestion.CausaNoPago = gestion.CausaNoPago ?? string.Empty;
                gestion.CausaNoDomiciliacion = gestion.CausaNoDomiciliacion ?? string.Empty;
                gestion.Coordenadas = gestion.Coordenadas ?? string.Empty;
            }

            var viewModel = new GestionesViewModel
            {
                Gestiones = gestiones,
                PageNumber = pageNumber,
                PageSize = pageSize,
                TotalItems = totalItems
            };

            // Pass filter values to the view
            ViewData["IdCredito"] = idCredito;
            ViewData["UsuarioRegistro"] = usuarioRegistro;

            return View("~/Views/Gestiones/Index.cshtml", viewModel);
        }

        public IActionResult DownloadAll()
        {
            var data = _context.Gestiones.ToList();
            var csv = new StringBuilder();
            csv.AppendLine("Indice,Credito,FechaActividad,UsuarioRegistro,FechaPromesa,MontoPromesa,Resultado,CausaNoPago,CodigoAccion,Comentarios,Producto,Origen,CausaNoDomiciliacion,ContactoGenerado,Coordenadas");

            foreach (var item in data)
            {
                csv.AppendLine($"{item.Indice},{item.Credito},{item.FechaActividad},{item.UsuarioRegistro},{item.FechaPromesa},{item.MontoPromesa},{item.Resultado},{item.CausaNoPago},{item.CodigoAccion},{item.Comentarios},{item.Producto},{item.Origen},{item.CausaNoDomiciliacion},{item.ContactoGenerado},{item.Coordenadas}");
            }

            var fileName = "Gestiones_AllData.csv";
            return File(Encoding.UTF8.GetBytes(csv.ToString()), "text/csv", fileName);
        }

        [HttpPost]
        public IActionResult DownloadCurrentSelection(int? idCredito = null, string? usuarioRegistro = null)
        {
            var query = _context.Gestiones.AsQueryable();

            // Apply filters
            if (idCredito.HasValue)
            {
                string idCreditoString = idCredito.Value.ToString();
                query = query.Where(s => s.Credito.ToString().Contains(idCreditoString));
            }
            if (!string.IsNullOrEmpty(usuarioRegistro))
            {
                query = query.Where(s => s.UsuarioRegistro.Contains(usuarioRegistro));
            }

            var data = query.ToList();
            var csv = new StringBuilder();
            csv.AppendLine("Indice,Credito,FechaActividad,UsuarioRegistro,FechaPromesa,MontoPromesa,Resultado,CausaNoPago,CodigoAccion,Comentarios,Producto,Origen,CausaNoDomiciliacion,ContactoGenerado,Coordenadas");

            foreach (var item in data)
            {
                csv.AppendLine($"{item.Indice},{item.Credito},{item.FechaActividad},{item.UsuarioRegistro},{item.FechaPromesa},{item.MontoPromesa},{item.Resultado},{item.CausaNoPago},{item.CodigoAccion},{item.Comentarios},{item.Producto},{item.Origen},{item.CausaNoDomiciliacion},{item.ContactoGenerado},{item.Coordenadas}");
            }

            var fileName = $"Gestiones_CurrentSelection_{DateTime.Now:yyyyMMdd}.csv";
            return File(Encoding.UTF8.GetBytes(csv.ToString()), "text/csv", fileName);
        }
    }
}
