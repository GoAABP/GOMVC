using Microsoft.AspNetCore.Mvc;
using GOMVC.Data;
using GOMVC.Models;
using System.Linq;
using System.Text;
using Microsoft.AspNetCore.Authorization;

namespace GOMVC.Controllers
{
    [Authorize]
    public class AplicacionPAgosController : Controller
    {
        private readonly AppDbContext _context;

        public AplicacionPAgosController(AppDbContext context)
        {
            _context = context;
        }
        public IActionResult Index(int pageNumber = 1, int pageSize = 100, int? idPago = null, int? idCredito = null, string? nombreCliente = null, bool clearFilters = false)
        {
            var query = _context.Aplicacion_Pagos.AsQueryable();

            // Apply filters
            if (idPago.HasValue)
            {
                string idPagoString = idPago.Value.ToString();
                query = query.Where(s => s.Id_Pago.ToString().Contains(idPagoString));
            }
            if (idCredito.HasValue)
            {
                string idCreditoString = idCredito.Value.ToString();
                query = query.Where(s => s.Id_Credito.ToString().Contains(idCreditoString));
            }
            if (!string.IsNullOrEmpty(nombreCliente))
            {
                query = query.Where(s => s.Nombre_Cliente.Contains(nombreCliente));
            }

            var totalItems = query.Count();
            var aplicacionPagos = query
                .Skip((pageNumber - 1) * pageSize)
                .Take(pageSize)
                .ToList();

            var viewModel = new AplicacionPagosViewModel
            {
                Aplicacion_Pagos = aplicacionPagos,
                PageNumber = pageNumber,
                PageSize = pageSize,
                TotalItems = totalItems
            };

            // Pass filter values to the view
            ViewData["IdPago"] = idPago;
            ViewData["IdCredito"] = idCredito;
            ViewData["NombreCliente"] = nombreCliente;

            return View("~/Views/Aplicacion_Pagos/Index.cshtml", viewModel);
        }

        public IActionResult DownloadAll()
        {
            var data = _context.Aplicacion_Pagos.ToList();
            var csv = new StringBuilder();
            csv.AppendLine("Id_Pago,Id_Credito,Id_Convenio,Convenio,Referencia,Nombre_Cliente,Financiamiento,Origen_de_Movimiento,Fecha_Pago,Fecha_Aplicacion,Fecha_Deposito,Status,Pago,Capital,Interes,IVA_Int,Comision_Financiada,IVA_Comision_Financ,Moratorios,IVA_Mora,Pago_Tardio,IVA_PagoTardio,Recuperacion,IVA_Recup,Com_Liquidacion,IVA_Com_Liquidacion,Retencion_X_Admon,IVA_Retencion_X_Admon,Pago_Exceso,Gestor,Forma_de_pago,vMotive");

            foreach (var item in data)
            {
                csv.AppendLine($"{item.Id_Pago},{item.Id_Credito},{item.Id_Convenio},{item.Convenio},{item.Referencia},{item.Nombre_Cliente},{item.Financiamiento},{item.Origen_de_Movimiento},{item.Fecha_Pago},{item.Fecha_Aplicacion},{item.Fecha_Deposito},{item.Status},{item.Pago},{item.Capital},{item.Interes},{item.IVA_Int},{item.Comision_Financiada},{item.IVA_Comision_Financ},{item.Moratorios},{item.IVA_Mora},{item.Pago_Tardio},{item.IVA_PagoTardio},{item.Recuperacion},{item.IVA_Recup},{item.Com_Liquidacion},{item.IVA_Com_Liquidacion},{item.Retencion_X_Admon},{item.IVA_Retencion_X_Admon},{item.Pago_Exceso},{item.Gestor},{item.Forma_de_pago},{item.vMotive}");
            }

            var fileName = "AplicacionPagos_AllData.csv";
            return File(Encoding.UTF8.GetBytes(csv.ToString()), "text/csv", fileName);
        }


        [HttpPost]
        public IActionResult DownloadCurrentSelection(int? idPago = null, int? idCredito = null, string? nombreCliente = null)
        {
            var query = _context.Aplicacion_Pagos.AsQueryable();

            // Apply filters
            if (idPago.HasValue)
            {
                string idPagoString = idPago.Value.ToString();
                query = query.Where(s => s.Id_Pago.ToString().Contains(idPagoString));
            }
            if (idCredito.HasValue)
            {
                string idCreditoString = idCredito.Value.ToString();
                query = query.Where(s => s.Id_Credito.ToString().Contains(idCreditoString));
            }
            if (!string.IsNullOrEmpty(nombreCliente))
            {
                query = query.Where(s => s.Nombre_Cliente.Contains(nombreCliente));
            }

            var data = query.ToList();
            var csv = new StringBuilder();
            csv.AppendLine("Id_Pago,Id_Credito,Id_Convenio,Convenio,Referencia,Nombre_Cliente,Financiamiento,Origen_de_Movimiento,Fecha_Pago,Fecha_Aplicacion,Fecha_Deposito,Status,Pago,Capital,Interes,IVA_Int,Comision_Financiada,IVA_Comision_Financ,Moratorios,IVA_Mora,Pago_Tardio,IVA_PagoTardio,Recuperacion,IVA_Recup,Com_Liquidacion,IVA_Com_Liquidacion,Retencion_X_Admon,IVA_Retencion_X_Admon,Pago_Exceso,Gestor,Forma_de_pago,vMotive");

            foreach (var item in data)
            {
                csv.AppendLine($"{item.Id_Pago},{item.Id_Credito},{item.Id_Convenio},{item.Convenio},{item.Referencia},{item.Nombre_Cliente},{item.Financiamiento},{item.Origen_de_Movimiento},{item.Fecha_Pago},{item.Fecha_Aplicacion},{item.Fecha_Deposito},{item.Status},{item.Pago},{item.Capital},{item.Interes},{item.IVA_Int},{item.Comision_Financiada},{item.IVA_Comision_Financ},{item.Moratorios},{item.IVA_Mora},{item.Pago_Tardio},{item.IVA_PagoTardio},{item.Recuperacion},{item.IVA_Recup},{item.Com_Liquidacion},{item.IVA_Com_Liquidacion},{item.Retencion_X_Admon},{item.IVA_Retencion_X_Admon},{item.Pago_Exceso},{item.Gestor},{item.Forma_de_pago},{item.vMotive}");
            }

            var fileName = $"AplicacionPagos_CurrentSelection_{DateTime.Now:yyyyMMdd}.csv";
            return File(Encoding.UTF8.GetBytes(csv.ToString()), "text/csv", fileName);
        }
    }
}