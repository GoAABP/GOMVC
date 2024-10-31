using Microsoft.AspNetCore.Mvc;
using GOMVC.Data;
using GOMVC.Models;
using System.Linq;
using System.Text;

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

        // New action to download most recent data as CSV
        public IActionResult DownloadMostRecentData()
        {
            var query = _context.Saldos_Cartera.AsQueryable();

            // Get the most recent date
            var mostRecentDate = query.Max(s => s.FechaGenerado);
            var recentData = query.Where(s => s.FechaGenerado == mostRecentDate).ToList();

            var csv = new StringBuilder();
            csv.AppendLine("IdSolicitud,IdCredito,IdPersona,Referencia,Afiliado,Nombre,Monto,Comision,InteresesTotales,MontoTotal,Pagos,AmortPagadas,CapitalPagado,InteresPagado,IVAPagado,CargoPTardioPagado,MoratorioPagado,PagoEnExceso,ComisionPagada,TotalPagado,AjustesCapital,SaldoCapital,SaldoInteres,SaldoIVAInteres,SaldoCargoPTardio,SaldoMoratorios,SaldoPagoExceso,SaldoComision,SaldoTotal,ImportePago,IdConvenio,Dependencia,PrimerPagoTeorico,UltimoPago,TipoFinanciamiento,CapitalVigente,CapitalVencido,InteresesVencidos,Vencido,SaldoInsoluto,SdoTotalCListasCobro,SdoVencidoCListCobro,EstatusCartera,Estatus,Sucursal,FDesembolso,Frecuencia,PrimerPagoReal,UltimoPagoCListCobro,UltimoPagoAplicado,DiasUltimoPago,DiasAtraso,CuotasAtraso,PeriodosAtraso,Pago,MontoUltimoPago,TasaIntAnual,Gestor,Motivo,Banco,Estado,Ciudad,ComVigente,ComVencida,Clabe,SigPago,MontoSigPago,VFondeador,ValidaDomi,VAfiliateIdO,VAfiliateO,SaldoRetencionAdm,RFC,VMotiveExt,IPeriodsExt,VCommentExt,NRetencion,NJoPay,IMaxDays,VMaxDate,NLiquidate,NLiqPrin,NLiqInt,NLiqMor,NLiqCha,NLiqPrinTran,NLiqIntTran,NLiqMorTran,NLiqChaTran,NLiqRetTran,VScoreBuro,VCollectStatus,NCAT,VOpTable");

            foreach (var item in recentData)
            {
                csv.AppendLine($"{item.IdSolicitud},{item.IdCredito},{item.IdPersona},{item.Referencia},{item.Afiliado},{item.Nombre},{item.Monto},{item.Comision},{item.InteresesTotales},{item.MontoTotal},{item.Pagos},{item.AmortPagadas},{item.CapitalPagado},{item.InteresPagado},{item.IVAPagado},{item.CargoPTardioPagado},{item.MoratorioPagado},{item.PagoEnExceso},{item.ComisionPagada},{item.TotalPagado},{item.AjustesCapital},{item.SaldoCapital},{item.SaldoInteres},{item.SaldoIVAInteres},{item.SaldoCargoPTardio},{item.SaldoMoratorios},{item.SaldoPagoExceso},{item.SaldoComision},{item.SaldoTotal},{item.ImportePago},{item.IdConvenio},{item.Dependencia},{item.PrimerPagoTeorico},{item.UltimoPago},{item.TipoFinanciamiento},{item.CapitalVigente},{item.CapitalVencido},{item.InteresesVencidos},{item.Vencido},{item.SaldoInsoluto},{item.SdoTotalCListasCobro},{item.SdoVencidoCListCobro},{item.EstatusCartera},{item.Estatus},{item.Sucursal},{item.FDesembolso},{item.Frecuencia},{item.PrimerPagoReal},{item.UltimoPagoCListCobro},{item.UltimoPagoAplicado},{item.DiasUltimoPago},{item.DiasAtraso},{item.CuotasAtraso},{item.PeriodosAtraso},{item.Pago},{item.MontoUltimoPago},{item.TasaIntAnual},{item.Gestor},{item.Motivo},{item.Banco},{item.Estado},{item.Ciudad},{item.ComVigente},{item.ComVencida},{item.Clabe},{item.SigPago},{item.MontoSigPago},{item.VFondeador},{item.ValidaDomi},{item.VAfiliateIdO},{item.VAfiliateO},{item.SaldoRetencionAdm},{item.RFC},{item.VMotiveExt},{item.IPeriodsExt},{item.VCommentExt},{item.NRetencion},{item.NJoPay},{item.IMaxDays},{item.VMaxDate},{item.NLiquidate},{item.NLiqPrin},{item.NLiqInt},{item.NLiqMor},{item.NLiqCha},{item.NLiqPrinTran},{item.NLiqIntTran},{item.NLiqMorTran},{item.NLiqChaTran},{item.NLiqRetTran},{item.VScoreBuro},{item.VCollectStatus},{item.NCAT},{item.VOpTable}");
            }

        var fileName = $"SaldosCartera_{mostRecentDate:yyyyMMdd}.csv";
        return File(Encoding.UTF8.GetBytes(csv.ToString()), "text/csv", fileName);
        }
    }
}