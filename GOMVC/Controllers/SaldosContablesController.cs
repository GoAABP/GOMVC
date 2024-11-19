using Microsoft.AspNetCore.Mvc;
using GOMVC.Data;
using GOMVC.Models;
using System.Linq;
using System.Text;
using Microsoft.AspNetCore.Authorization;

namespace GOMVC.Controllers
{
    [Authorize]
    public class SaldosContablesController : Controller
    {
        private readonly AppDbContext _context;

        public SaldosContablesController(AppDbContext context)
        {
            _context = context;
        }

        public IActionResult Index(int pageNumber = 1, int pageSize = 100, int? idCredito = null, int? idSucursal = null, string? nombre = null, DateTime? selectedDate = null, bool clearFilters = false)
{
        var query = _context.Saldos_Contables.AsQueryable();

        // Get the most recent date if no date is selected and the date picker is not cleared
        if (!selectedDate.HasValue && !clearFilters && !Request.Query.ContainsKey("selectedDate"))
        {
            selectedDate = query.Max(s => s.FechaGenerado);
        }

        // Apply filters
        if (idCredito.HasValue)
        {
            query = query.Where(s => s.Id_Credito.ToString().Contains(idCredito.Value.ToString()));
        }
        if (idSucursal.HasValue)
        {
#pragma warning disable CS8602 // Dereference of a possibly null reference.
            query = query.Where(s => s.Id_Sucursal.ToString().Contains(idSucursal.Value.ToString()));
#pragma warning restore CS8602 // Dereference of a possibly null reference.
        }
        if (!string.IsNullOrEmpty(nombre))
        {
#pragma warning disable CS8602 // Dereference of a possibly null reference.
            query = query.Where(s => s.Nombre.Contains(nombre));
#pragma warning restore CS8602 // Dereference of a possibly null reference.
        }

        // Apply date filter if selected
        if (selectedDate.HasValue)
        {
            query = query.Where(s => s.FechaGenerado.HasValue && s.FechaGenerado.Value.Date == selectedDate.Value.Date);
        }

        var totalItems = query.Count();
        var saldosContables = query
            .Skip((pageNumber - 1) * pageSize)
            .Take(pageSize)
            .ToList();

        var viewModel = new SaldosContablesViewModel
        {
            Saldos_Contables = saldosContables,
            PageNumber = pageNumber,
            PageSize = pageSize,
            TotalItems = totalItems
        };

        // Pass filter values to the view
        ViewData["IdCredito"] = idCredito;
        ViewData["IdSucursal"] = idSucursal;
        ViewData["Nombre"] = nombre;
        ViewData["SelectedDate"] = selectedDate;

        return View("~/Views/Saldos_Contables/Index.cshtml", viewModel);
    }

        public IActionResult DownloadMostRecentData()
        {
            var query = _context.Saldos_Contables.AsQueryable();
            var mostRecentDate = query.Max(s => s.FechaGenerado);
            var recentData = query.Where(s => s.FechaGenerado == mostRecentDate).ToList();

            var csv = new StringBuilder();
            csv.AppendLine("DBKEY,Id_Credito,Referencia,Nombre,Id_Sucursal,Sucursal,Id_Convenio,Convenio,Financiamiento,Estatus_Inicial,Estatus_Final,Fecha_Apertura,Fecha_Terminacion,Importe,Dias_Atraso,Cuotas_Atrasadas,Periodos_Atraso,Pagos_Sostenidos,Pago,Frecuencia,Fecha_Ultimo_Pago,Importe_Ultimo_Pago,Saldo_Inicial_Capital,Otorgado,Pagos,Ajuste_Cargo_Capital,Ajuste_Abono_Capital,Saldo_Final_Capital,Calculo,Diferencia,Capital_Vigente,Capital_Vencido,Saldo_Inicial_Interes,Devengamiento,Pagos_Interes,Ajuste_Cargo_Interes,Ajuste_Abono_Interes,Interes_No_Devengado,Saldo_Final_Interes,Calculo_Interes,Diferencia_Interes,Interes_Devengado,IVA_Interes_Devengado,Interes_No_DevengadoB,Fecha_Cartera_Vencida,Saldo_Contable,Saldo_Insoluto,Porc_Provision,Reserva,nCAT,vOpTable,Status,FechaGenerado");

            foreach (var item in recentData)
            {
                csv.AppendLine($"{item.DBKEY},{item.Id_Credito},{item.Referencia},{item.Nombre},{item.Id_Sucursal},{item.Sucursal},{item.Id_Convenio},{item.Convenio},{item.Financiamiento},{item.Estatus_Inicial},{item.Estatus_Final},{item.Fecha_Apertura},{item.Fecha_Terminacion},{item.Importe},{item.Dias_Atraso},{item.Cuotas_Atrasadas},{item.Periodos_Atraso},{item.Pagos_Sostenidos},{item.Pago},{item.Frecuencia},{item.Fecha_Ultimo_Pago},{item.Importe_Ultimo_Pago},{item.Saldo_Inicial_Capital},{item.Otorgado},{item.Pagos},{item.Ajuste_Cargo_Capital},{item.Ajuste_Abono_Capital},{item.Saldo_Final_Capital},{item.Calculo},{item.Diferencia},{item.Capital_Vigente},{item.Capital_Vencido},{item.Saldo_Inicial_Interes},{item.Devengamiento},{item.Pagos_Interes},{item.Ajuste_Cargo_Interes},{item.Ajuste_Abono_Interes},{item.Interes_No_Devengado},{item.Saldo_Final_Interes},{item.Calculo_Interes},{item.Diferencia_Interes},{item.Interes_Devengado},{item.IVA_Interes_Devengado},{item.Interes_No_DevengadoB},{item.Fecha_Cartera_Vencida},{item.Saldo_Contable},{item.Saldo_Insoluto},{item.Porc_Provision},{item.Reserva},{item.nCAT},{item.vOpTable},{item.Status},{item.FechaGenerado}");
            }

            var fileName = $"SaldosContables_{mostRecentDate:yyyyMMdd}.csv";
            return File(Encoding.UTF8.GetBytes(csv.ToString()), "text/csv", fileName);
        }

        [HttpPost]
        public IActionResult DownloadDataByDate(DateTime? selectedDate)
        {
            if (selectedDate == null)
            {
                return BadRequest("Selected date is required.");
            }

            var query = _context.Saldos_Contables.AsQueryable();
            var dataForSelectedDate = query.Where(s => s.FechaGenerado.HasValue && s.FechaGenerado.Value.Date == selectedDate.Value.Date).ToList();

            var csv = new StringBuilder();
            csv.AppendLine("DBKEY,Id_Credito,Referencia,Nombre,Id_Sucursal,Sucursal,Id_Convenio,Convenio,Financiamiento,Estatus_Inicial,Estatus_Final,Fecha_Apertura,Fecha_Terminacion,Importe,Dias_Atraso,Cuotas_Atrasadas,Periodos_Atraso,Pagos_Sostenidos,Pago,Frecuencia,Fecha_Ultimo_Pago,Importe_Ultimo_Pago,Saldo_Inicial_Capital,Otorgado,Pagos,Ajuste_Cargo_Capital,Ajuste_Abono_Capital,Saldo_Final_Capital,Calculo,Diferencia,Capital_Vigente,Capital_Vencido,Saldo_Inicial_Interes,Devengamiento,Pagos_Interes,Ajuste_Cargo_Interes,Ajuste_Abono_Interes,Interes_No_Devengado,Saldo_Final_Interes,Calculo_Interes,Diferencia_Interes,Interes_Devengado,IVA_Interes_Devengado,Interes_No_DevengadoB,Fecha_Cartera_Vencida,Saldo_Contable,Saldo_Insoluto,Porc_Provision,Reserva,nCAT,vOpTable,Status,FechaGenerado");

            foreach (var item in dataForSelectedDate)
            {
                csv.AppendLine($"{item.DBKEY},{item.Id_Credito},{item.Referencia},{item.Nombre},{item.Id_Sucursal},{item.Sucursal},{item.Id_Convenio},{item.Convenio},{item.Financiamiento},{item.Estatus_Inicial},{item.Estatus_Final},{item.Fecha_Apertura},{item.Fecha_Terminacion},{item.Importe},{item.Dias_Atraso},{item.Cuotas_Atrasadas},{item.Periodos_Atraso},{item.Pagos_Sostenidos},{item.Pago},{item.Frecuencia},{item.Fecha_Ultimo_Pago},{item.Importe_Ultimo_Pago},{item.Saldo_Inicial_Capital},{item.Otorgado},{item.Pagos},{item.Ajuste_Cargo_Capital},{item.Ajuste_Abono_Capital},{item.Saldo_Final_Capital},{item.Calculo},{item.Diferencia},{item.Capital_Vigente},{item.Capital_Vencido},{item.Saldo_Inicial_Interes},{item.Devengamiento},{item.Pagos_Interes},{item.Ajuste_Cargo_Interes},{item.Ajuste_Abono_Interes},{item.Interes_No_Devengado},{item.Saldo_Final_Interes},{item.Calculo_Interes},{item.Diferencia_Interes},{item.Interes_Devengado},{item.IVA_Interes_Devengado},{item.Interes_No_DevengadoB},{item.Fecha_Cartera_Vencida},{item.Saldo_Contable},{item.Saldo_Insoluto},{item.Porc_Provision},{item.Reserva},{item.nCAT},{item.vOpTable},{item.Status},{item.FechaGenerado}");
            }

            var fileName = $"SaldosContables_{selectedDate.Value:yyyyMMdd}.csv";
            return File(Encoding.UTF8.GetBytes(csv.ToString()), "text/csv", fileName);
        }

       [HttpPost]
        public IActionResult DownloadCurrentSelection(int? idCredito = null, int? idSucursal = null, string? nombre = null, DateTime? selectedDate = null)
        {
            var query = _context.Saldos_Contables.AsQueryable();

            // Apply filters
            if (idCredito.HasValue)
            {
                query = query.Where(s => s.Id_Credito == idCredito.Value);
            }
            if (idSucursal.HasValue)
            {
                query = query.Where(s => s.Id_Sucursal == idSucursal.Value);
            }
            if (!string.IsNullOrEmpty(nombre))
            {
#pragma warning disable CS8602 // Dereference of a possibly null reference.
                query = query.Where(s => s.Nombre.Contains(nombre));
#pragma warning restore CS8602 // Dereference of a possibly null reference.
            }

            // Apply date filter if selected
            if (selectedDate.HasValue)
            {
                query = query.Where(s => s.FechaGenerado.HasValue && s.FechaGenerado.Value.Date == selectedDate.Value.Date);
            }

            var data = query.ToList();

            var csv = new StringBuilder();
            csv.AppendLine("DBKEY,Id_Credito,Referencia,Nombre,Id_Sucursal,Sucursal,Id_Convenio,Convenio,Financiamiento,Estatus_Inicial,Estatus_Final,Fecha_Apertura,Fecha_Terminacion,Importe,Dias_Atraso,Cuotas_Atrasadas,Periodos_Atraso,Pagos_Sostenidos,Pago,Frecuencia,Fecha_Ultimo_Pago,Importe_Ultimo_Pago,Saldo_Inicial_Capital,Otorgado,Pagos,Ajuste_Cargo_Capital,Ajuste_Abono_Capital,Saldo_Final_Capital,Calculo,Diferencia,Capital_Vigente,Capital_Vencido,Saldo_Inicial_Interes,Devengamiento,Pagos_Interes,Ajuste_Cargo_Interes,Ajuste_Abono_Interes,Interes_No_Devengado,Saldo_Final_Interes,Calculo_Interes,Diferencia_Interes,Interes_Devengado,IVA_Interes_Devengado,Interes_No_DevengadoB,Fecha_Cartera_Vencida,Saldo_Contable,Saldo_Insoluto,Porc_Provision,Reserva,nCAT,vOpTable,Status,FechaGenerado");

            foreach (var item in data)
            {
                csv.AppendLine($"{item.DBKEY},{item.Id_Credito},{item.Referencia},{item.Nombre},{item.Id_Sucursal},{item.Sucursal},{item.Id_Convenio},{item.Convenio},{item.Financiamiento},{item.Estatus_Inicial},{item.Estatus_Final},{item.Fecha_Apertura},{item.Fecha_Terminacion},{item.Importe},{item.Dias_Atraso},{item.Cuotas_Atrasadas},{item.Periodos_Atraso},{item.Pagos_Sostenidos},{item.Pago},{item.Frecuencia},{item.Fecha_Ultimo_Pago},{item.Importe_Ultimo_Pago},{item.Saldo_Inicial_Capital},{item.Otorgado},{item.Pagos},{item.Ajuste_Cargo_Capital},{item.Ajuste_Abono_Capital},{item.Saldo_Final_Capital},{item.Calculo},{item.Diferencia},{item.Capital_Vigente},{item.Capital_Vencido},{item.Saldo_Inicial_Interes},{item.Devengamiento},{item.Pagos_Interes},{item.Ajuste_Cargo_Interes},{item.Ajuste_Abono_Interes},{item.Interes_No_Devengado},{item.Saldo_Final_Interes},{item.Calculo_Interes},{item.Diferencia_Interes},{item.Interes_Devengado},{item.IVA_Interes_Devengado},{item.Interes_No_DevengadoB},{item.Fecha_Cartera_Vencida},{item.Saldo_Contable},{item.Saldo_Insoluto},{item.Porc_Provision},{item.Reserva},{item.nCAT},{item.vOpTable},{item.Status},{item.FechaGenerado}");
            }

            var fileName = $"SaldosContables_{selectedDate?.ToString("yyyyMMdd") ?? "CurrentSelection"}.csv";
            return File(Encoding.UTF8.GetBytes(csv.ToString()), "text/csv", fileName);
        }
    }
}
