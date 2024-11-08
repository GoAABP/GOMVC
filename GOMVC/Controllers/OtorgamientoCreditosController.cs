using Microsoft.AspNetCore.Mvc;
using GOMVC.Models;
using System.Linq;
using System.Text;
using Microsoft.AspNetCore.Authorization;
using GOMVC.Data;

namespace GOMVC.Controllers
{
    [Authorize]
    public class OtorgamientoCreditosController : Controller
    {
        private readonly AppDbContext _context;

        public OtorgamientoCreditosController(AppDbContext context)
        {
            _context = context;
        }

        public IActionResult Index(int pageNumber = 1, int pageSize = 100, int? idCredito = null, string? nombre = null, bool clearFilters = false)
        {
            var query = _context.Otorgamiento_Creditos.AsQueryable();

            // Apply filters
            if (idCredito.HasValue)
            {
                string idCreditoString = idCredito.Value.ToString();
                query = query.Where(s => s.Id_Credito.ToString().Contains(idCreditoString));
            }
            if (!string.IsNullOrEmpty(nombre))
            {
                query = query.Where(s => s.Nombre.Contains(nombre));
            }

            var totalItems = query.Count();
            var otorgamientoCreditos = query
                .Skip((pageNumber - 1) * pageSize)
                .Take(pageSize)
                .ToList();

            var viewModel = new OtorgamientoCreditosViewModel
            {
                Otorgamiento_Creditos = otorgamientoCreditos,
                PageNumber = pageNumber,
                PageSize = pageSize,
                TotalItems = totalItems
            };

            // Pass filter values to the view
            ViewData["IdCredito"] = idCredito;
            ViewData["Nombre"] = nombre;

            return View("~/Views/Otorgamiento_Creditos/Index.cshtml", viewModel);
        }

        public IActionResult DownloadAll()
        {
            var data = _context.Otorgamiento_Creditos.ToList();
            var csv = new StringBuilder();
            csv.AppendLine("Id_Credito,Referencia,Nombre,Fecha_Apertura,F_Cobro,Id_Convenio,Convenio,Id_Sucursal,Sucursal,Capital,Primer_Pago,Comision,IVA,Cobertura,IVA_Cobertura,Disposicion,Monto_Retenido,Pago_de_Deuda,Comision_Financiada,IVA_Comision_Financiada,Solicitud,Vendedor,Nombre_Vendedor,TipoVendedor,vSupervisorId,vSupName,Producto,Descripcion_Tasa,Persona,Plazo,Id_Producto,vCampaign,Tipo_de_Financiamiento,vFinancingTypeId,vAliado");

            foreach (var item in data)
            {
                csv.AppendLine($"{item.Id_Credito},{item.Referencia},{item.Nombre},{item.Fecha_Apertura},{item.F_Cobro},{item.Id_Convenio},{item.Convenio},{item.Id_Sucursal},{item.Sucursal},{item.Capital},{item.Primer_Pago},{item.Comision},{item.IVA},{item.Cobertura},{item.IVA_Cobertura},{item.Disposicion},{item.Monto_Retenido},{item.Pago_de_Deuda},{item.Comision_Financiada},{item.IVA_Comision_Financiada},{item.Solicitud},{item.Vendedor},{item.Nombre_Vendedor},{item.TipoVendedor},{item.vSupervisorId},{item.vSupName},{item.Producto},{item.Descripcion_Tasa},{item.Persona},{item.Plazo},{item.Id_Producto},{item.vCampaign},{item.Tipo_de_Financiamiento},{item.vFinancingTypeId},{item.vAliado}");
            }

            var fileName = "OtorgamientoCreditos_AllData.csv";
            return File(Encoding.UTF8.GetBytes(csv.ToString()), "text/csv", fileName);
        }

        [HttpPost]
        public IActionResult DownloadCurrentSelection(int? idCredito = null, string? nombre = null)
        {
            var query = _context.Otorgamiento_Creditos.AsQueryable();

            // Apply filters
            if (idCredito.HasValue)
            {
                string idCreditoString = idCredito.Value.ToString();
                query = query.Where(s => s.Id_Credito.ToString().Contains(idCreditoString));
            }
            if (!string.IsNullOrEmpty(nombre))
            {
                query = query.Where(s => s.Nombre.Contains(nombre));
            }

            var data = query.ToList();
            var csv = new StringBuilder();
            csv.AppendLine("Id_Credito,Referencia,Nombre,Fecha_Apertura,F_Cobro,Id_Convenio,Convenio,Id_Sucursal,Sucursal,Capital,Primer_Pago,Comision,IVA,Cobertura,IVA_Cobertura,Disposicion,Monto_Retenido,Pago_de_Deuda,Comision_Financiada,IVA_Comision_Financiada,Solicitud,Vendedor,Nombre_Vendedor,TipoVendedor,vSupervisorId,vSupName,Producto,Descripcion_Tasa,Persona,Plazo,Id_Producto,vCampaign,Tipo_de_Financiamiento,vFinancingTypeId,vAliado");

            foreach (var item in data)
            {
                csv.AppendLine($"{item.Id_Credito},{item.Referencia},{item.Nombre},{item.Fecha_Apertura},{item.F_Cobro},{item.Id_Convenio},{item.Convenio},{item.Id_Sucursal},{item.Sucursal},{item.Capital},{item.Primer_Pago},{item.Comision},{item.IVA},{item.Cobertura},{item.IVA_Cobertura},{item.Disposicion},{item.Monto_Retenido},{item.Pago_de_Deuda},{item.Comision_Financiada},{item.IVA_Comision_Financiada},{item.Solicitud},{item.Vendedor},{item.Nombre_Vendedor},{item.TipoVendedor},{item.vSupervisorId},{item.vSupName},{item.Producto},{item.Descripcion_Tasa},{item.Persona},{item.Plazo},{item.Id_Producto},{item.vCampaign},{item.Tipo_de_Financiamiento},{item.vFinancingTypeId},{item.vAliado}");
            }

            var fileName = $"OtorgamientoCreditos_CurrentSelection_{DateTime.Now:yyyyMMdd}.csv";
            return File(Encoding.UTF8.GetBytes(csv.ToString()), "text/csv", fileName);
        }
    }
}
