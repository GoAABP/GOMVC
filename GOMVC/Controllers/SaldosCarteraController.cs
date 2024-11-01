using Microsoft.AspNetCore.Mvc;
using GOMVC.Data;
using GOMVC.Models;
using System.Linq;
using System.Text;
using Microsoft.AspNetCore.Authorization;

namespace GOMVC.Controllers
{
    [Authorize]
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
                query = query.Where(s => s.Id_Credito == idCredito.Value);
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

        // New action to download most recent data as CSV
        public IActionResult DownloadMostRecentData()
        {
            var query = _context.Saldos_Cartera.AsQueryable();

            // Get the most recent date
            var mostRecentDate = query.Max(s => s.FechaGenerado);
            var recentData = query.Where(s => s.FechaGenerado == mostRecentDate).ToList();

            var csv = new StringBuilder();
            csv.AppendLine(",Id_Solicitud,Id_Credito,IdPersona,Referencia,Afiliado,Nombre,Monto,Comision,Intereses_Totales,Monto_Total,Pagos,Amort_Pagadas,Capital_Pagado,Interes_Pagado,IVA_Int_Pagado,Cargo_PTardio_Pagado,Moratorio_Pagado,Pago_en_Exceso,Comision_Pagada,Total_Pagado,Ajustes_Capital,Saldo_Capital,Saldo_Interes,Saldo_IVA_Int,Saldo_Cargo_PTardio,Saldo_Moratorios,Saldo_Pago_Exceso,Saldo_Comision,Saldo_Total,Importe_de_Pago,Id_Convenio,Dependencia,Primer_Pago_Teorico,Ultimo_Pago,Tipo_Financiamiento,Capital_Vigente,Capital_Vencido,Intereses_Vencidos,Vencido,Sdo_Insoluto,Sdo_Total_c_ListasCobro,Sdo_Vencido_c_ListCobro,Estatus_Cartera,Estatus,Sucursal,Fecha_Desembolso,Frecuencia,Primer_Pago_Real,Ultimo_Pago_c_ListaCobro,Ultimo_Pago_Aplicado,Dias_Ultimo_Pago,Dias_Atraso,Cuotas_Atraso,Periodos_Atraso,Pago,Monto_Ultimo_Pago,Tasa_Int_Anual,Gestor,Motivo,Banco,Estado,Ciudad,Com_Vigente,Com_Vencida,Clabe,Sig_Pago,Monto_Sig_Pago,vFondeador,Valida_Domi,vAfiliateIdO,vAfiliateO,Saldo_Retencion_Adm,RFC,vMotiveExt,iPeriodsExt,vCommentExt,nRetencion,nJoPay,iMaxDays,vMaxDate,nLiquidate,nLiqPrin,nLiqInt,nLiqMor,nLiqCha,nLiqPrinTran,nLiqIntTran,nLiqMorTran,nLiqChaTran,nLiqRetTran,vScoreBuro,vCollectStatus,nCAT,vOpTable");

            foreach (var item in recentData)
            {
            csv.AppendLine($"{item.Id_Solicitud},{item.Id_Credito},{item.IdPersona},{item.Referencia},{item.Afiliado},{item.Nombre},{item.Monto},{item.Comision},{item.Intereses_Totales},{item.Monto_Total},{item.Pagos},{item.Amort_Pagadas},{item.Capital_Pagado},{item.Interes_Pagado},{item.IVA_Int_Pagado},{item.Cargo_PTardio_Pagado},{item.Moratorio_Pagado},{item.Pago_en_Exceso},{item.Comision_Pagada},{item.Total_Pagado},{item.Ajustes_Capital},{item.Saldo_Capital},{item.Saldo_Interes},{item.Saldo_IVA_Int},{item.Saldo_Cargo_PTardio},{item.Saldo_Moratorios},{item.Saldo_Pago_Exceso},{item.Saldo_Comision},{item.Saldo_Total},{item.Importe_de_Pago},{item.Id_Convenio},{item.Dependencia},{item.Primer_Pago_Teorico},{item.Ultimo_Pago},{item.Tipo_Financiamiento},{item.Capital_Vigente},{item.Capital_Vencido},{item.Intereses_Vencidos},{item.Vencido},{item.Sdo_Insoluto},{item.Sdo_Total_c_ListasCobro},{item.Sdo_Vencido_c_ListCobro},{item.Estatus_Cartera},{item.Estatus},{item.Sucursal},{item.Fecha_Desembolso},{item.Frecuencia},{item.Primer_Pago_Real},{item.Ultimo_Pago_c_ListaCobro},{item.Ultimo_Pago_Aplicado},{item.Dias_Ultimo_Pago},{item.Dias_Atraso},{item.Cuotas_Atraso},{item.Periodos_Atraso},{item.Pago},{item.Monto_Ultimo_Pago},{item.Tasa_Int_Anual},{item.Gestor},{item.Motivo},{item.Banco},{item.Estado},{item.Ciudad},{item.Com_Vigente},{item.Com_Vencida},{item.Clabe},{item.Sig_Pago},{item.Monto_Sig_Pago},{item.vFondeador},{item.Valida_Domi},{item.vAfiliateIdO},{item.vAfiliateO},{item.Saldo_Retencion_Adm},{item.RFC},{item.vMotiveExt},{item.iPeriodsExt},{item.vCommentExt},{item.nRetencion},{item.nJoPay},{item.iMaxDays},{item.vMaxDate},{item.nLiquidate},{item.nLiqPrin},{item.nLiqInt},{item.nLiqMor},{item.nLiqCha},{item.nLiqPrinTran},{item.nLiqIntTran},{item.nLiqMorTran},{item.nLiqChaTran},{item.nLiqRetTran},{item.vScoreBuro},{item.vCollectStatus},{item.nCAT},{item.vOpTable}");
            }

        var fileName = $"SaldosCartera_{mostRecentDate:yyyyMMdd}.csv";
        return File(Encoding.UTF8.GetBytes(csv.ToString()), "text/csv", fileName);
        }
    }
}