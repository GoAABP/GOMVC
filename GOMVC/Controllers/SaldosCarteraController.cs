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
        public IActionResult Index(int pageNumber = 1, int pageSize = 100, int? idCredito = null, int? idPersona = null, string? nombre = null, DateTime? selectedDate = null, bool clearFilters = false)
        {
            var query = _context.Saldos_Cartera.AsQueryable();

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
            if (idPersona.HasValue)
            {
                query = query.Where(s => s.IdPersona.ToString().Contains(idPersona.Value.ToString()));
            }
            if (!string.IsNullOrEmpty(nombre))
            {
                query = query.Where(s => s.Nombre.Contains(nombre));
            }

            // Apply date filter if selected
            if (selectedDate.HasValue)
            {
                query = query.Where(s => s.FechaGenerado.HasValue && s.FechaGenerado.Value.Date == selectedDate.Value.Date);
            }

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
            ViewData["SelectedDate"] = selectedDate;

            return View("~/Views/Saldos_cartera/Index.cshtml", viewModel);
        }
        public IActionResult DownloadMostRecentData()
        {
            var query = _context.Saldos_Cartera.AsQueryable();
            var mostRecentDate = query.Max(s => s.FechaGenerado);
            var recentData = query.Where(s => s.FechaGenerado == mostRecentDate).ToList();

            var csv = new StringBuilder();
            csv.AppendLine("Id_Solicitud,Id_Credito,IdPersona,Referencia,Afiliado,Nombre,Monto,Comision,Intereses_Totales,Monto_Total,Pagos,Amort_Pagadas,Capital_Pagado,Interes_Pagado,IVA_Int_Pagado,Cargo_PTardio_Pagado,Moratorio_Pagado,Pago_en_Exceso,Comision_Pagada,Total_Pagado,Ajustes_Capital,Saldo_Capital,Saldo_Interes,Saldo_IVA_Int,Saldo_Cargo_PTardio,Saldo_Moratorios,Saldo_Pago_Exceso,Saldo_Comision,Saldo_Total,Importe_de_Pago,Id_Convenio,Dependencia,Primer_Pago_Teorico,Ultimo_Pago,Tipo_Financiamiento,Capital_Vigente,Capital_Vencido,Intereses_Vencidos,Vencido,Sdo_Insoluto,Sdo_Total_c_ListasCobro,Sdo_Vencido_c_ListCobro,Estatus_Cartera,Estatus,Sucursal,Fecha_Desembolso,Frecuencia,Primer_Pago_Real,Ultimo_Pago_c_ListaCobro,Ultimo_Pago_Aplicado,Dias_Ultimo_Pago,Dias_Atraso,Cuotas_Atraso,Periodos_Atraso,Pago,Monto_Ultimo_Pago,Tasa_Int_Anual,Gestor,Motivo,Banco,Estado,Ciudad,Com_Vigente,Com_Vencida,Clabe,Sig_Pago,Monto_Sig_Pago,vFondeador,Valida_Domi,vAfiliateIdO,vAfiliateO,Saldo_Retencion_Adm,RFC,vMotiveExt,iPeriodsExt,vCommentExt,nRetencion,nJoPay,iMaxDays,vMaxDate,nLiquidate,nLiqPrin,nLiqInt,nLiqMor,nLiqCha,nLiqPrinTran,nLiqIntTran,nLiqMorTran,nLiqChaTran,nLiqRetTran,vScoreBuro,vCollectStatus,nCAT,vOpTable");

            foreach (var item in recentData)
            {
                csv.AppendLine($@"{item.Id_Solicitud},{item.Id_Credito},{item.IdPersona},{item.Referencia},{item.Afiliado},{item.Nombre},{item.Monto},{item.Comision},{item.Intereses_Totales},{item.Monto_Total},{item.Pagos},{item.Amort_Pagadas},{item.Capital_Pagado},{item.Interes_Pagado},{item.IVA_Int_Pagado},{item.Cargo_PTardio_Pagado},{item.Moratorio_Pagado},{item.Pago_en_Exceso},{item.Comision_Pagada},{item.Total_Pagado},{item.Ajustes_Capital},{item.Saldo_Capital},{item.Saldo_Interes},{item.Saldo_IVA_Int},{item.Saldo_Cargo_PTardio},{item.Saldo_Moratorios},{item.Saldo_Pago_Exceso},{item.Saldo_Comision},{item.Saldo_Total},{item.Importe_de_Pago},{item.Id_Convenio},{item.Dependencia},{item.Primer_Pago_Teorico},{item.Ultimo_Pago},{item.Tipo_Financiamiento},{item.Capital_Vigente},{item.Capital_Vencido},{item.Intereses_Vencidos},{item.Vencido},{item.Sdo_Insoluto},{item.Sdo_Total_c_ListasCobro},{item.Sdo_Vencido_c_ListCobro},{item.Estatus_Cartera},{item.Estatus},{item.Sucursal},{item.Fecha_Desembolso},{item.Frecuencia},{item.Primer_Pago_Real},{item.Ultimo_Pago_c_ListaCobro},{item.Ultimo_Pago_Aplicado},{item.Dias_Ultimo_Pago},{item.Dias_Atraso},{item.Cuotas_Atraso},{item.Periodos_Atraso},{item.Pago},{item.Monto_Ultimo_Pago},{item.Tasa_Int_Anual},{item.Gestor},{item.Motivo},{item.Banco},{item.Estado},{item.Ciudad},{item.Com_Vigente},{item.Com_Vencida},{item.Clabe},{item.Sig_Pago},{item.Monto_Sig_Pago},{item.vFondeador},{item.Valida_Domi},{item.vAfiliateIdO},{item.vAfiliateO},{item.Saldo_Retencion_Adm},{item.RFC},{item.vMotiveExt},{item.iPeriodsExt},{item.vCommentExt},{item.nRetencion},{item.nJoPay},{item.iMaxDays},{item.vMaxDate},{item.nLiquidate},{item.nLiqPrin},{item.nLiqInt},{item.nLiqMor},{item.nLiqCha},{item.nLiqPrinTran},{item.nLiqIntTran},{item.nLiqMorTran},{item.nLiqChaTran},{item.nLiqRetTran},{item.vScoreBuro},{item.vCollectStatus},{item.nCAT},{item.vOpTable}");
            }

            var fileName = $"SaldosCartera_{mostRecentDate:yyyyMMdd}.csv";
            return File(Encoding.UTF8.GetBytes(csv.ToString()), "text/csv", fileName);
        }

        [HttpPost]
        public IActionResult DownloadDataByDate(DateTime? selectedDate)
        {
            if (selectedDate == null)
            {
                return BadRequest("Selected date is required.");
            }

            var query = _context.Saldos_Cartera.AsQueryable();
            var dataForSelectedDate = query.Where(s => s.FechaGenerado.HasValue && s.FechaGenerado.Value.Date == selectedDate.Value.Date).ToList();

            var csv = new StringBuilder();
            csv.AppendLine("Id_Solicitud,Id_Credito,IdPersona,Referencia,Afiliado,Nombre,Monto,Comision,Intereses_Totales,Monto_Total,Pagos,Amort_Pagadas,Capital_Pagado,Interes_Pagado,IVA_Int_Pagado,Cargo_PTardio_Pagado,Moratorio_Pagado,Pago_en_Exceso,Comision_Pagada,Total_Pagado,Ajustes_Capital,Saldo_Capital,Saldo_Interes,Saldo_IVA_Int,Saldo_Cargo_PTardio,Saldo_Moratorios,Saldo_Pago_Exceso,Saldo_Comision,Saldo_Total,Importe_de_Pago,Id_Convenio,Dependencia,Primer_Pago_Teorico,Ultimo_Pago,Tipo_Financiamiento,Capital_Vigente,Capital_Vencido,Intereses_Vencidos,Vencido,Sdo_Insoluto,Sdo_Total_c_ListasCobro,Sdo_Vencido_c_ListCobro,Estatus_Cartera,Estatus,Sucursal,Fecha_Desembolso,Frecuencia,Primer_Pago_Real,Ultimo_Pago_c_ListaCobro,Ultimo_Pago_Aplicado,Dias_Ultimo_Pago,Dias_Atraso,Cuotas_Atraso,Periodos_Atraso,Pago,Monto_Ultimo_Pago,Tasa_Int_Anual,Gestor,Motivo,Banco,Estado,Ciudad,Com_Vigente,Com_Vencida,Clabe,Sig_Pago,Monto_Sig_Pago,vFondeador,Valida_Domi,vAfiliateIdO,vAfiliateO,Saldo_Retencion_Adm,RFC,vMotiveExt,iPeriodsExt,vCommentExt,nRetencion,nJoPay,iMaxDays,vMaxDate,nLiquidate,nLiqPrin,nLiqInt,nLiqMor,nLiqCha,nLiqPrinTran,nLiqIntTran,nLiqMorTran,nLiqChaTran,nLiqRetTran,vScoreBuro,vCollectStatus,nCAT,vOpTable");

            foreach (var item in dataForSelectedDate)
            {
                csv.AppendLine($@"{item.Id_Solicitud},{item.Id_Credito},{item.IdPersona},{item.Referencia},{item.Afiliado},{item.Nombre},{item.Monto},{item.Comision},{item.Intereses_Totales},{item.Monto_Total},{item.Pagos},{item.Amort_Pagadas},{item.Capital_Pagado},{item.Interes_Pagado},{item.IVA_Int_Pagado},{item.Cargo_PTardio_Pagado},{item.Moratorio_Pagado},{item.Pago_en_Exceso},{item.Comision_Pagada},{item.Total_Pagado},{item.Ajustes_Capital},{item.Saldo_Capital},{item.Saldo_Interes},{item.Saldo_IVA_Int},{item.Saldo_Cargo_PTardio},{item.Saldo_Moratorios},{item.Saldo_Pago_Exceso},{item.Saldo_Comision},{item.Saldo_Total},{item.Importe_de_Pago},{item.Id_Convenio},{item.Dependencia},{item.Primer_Pago_Teorico},{item.Ultimo_Pago},{item.Tipo_Financiamiento},{item.Capital_Vigente},{item.Capital_Vencido},{item.Intereses_Vencidos},{item.Vencido},{item.Sdo_Insoluto},{item.Sdo_Total_c_ListasCobro},{item.Sdo_Vencido_c_ListCobro},{item.Estatus_Cartera},{item.Estatus},{item.Sucursal},{item.Fecha_Desembolso},{item.Frecuencia},{item.Primer_Pago_Real},{item.Ultimo_Pago_c_ListaCobro},{item.Ultimo_Pago_Aplicado},{item.Dias_Ultimo_Pago},{item.Dias_Atraso},{item.Cuotas_Atraso},{item.Periodos_Atraso},{item.Pago},{item.Monto_Ultimo_Pago},{item.Tasa_Int_Anual},{item.Gestor},{item.Motivo},{item.Banco},{item.Estado},{item.Ciudad},{item.Com_Vigente},{item.Com_Vencida},{item.Clabe},{item.Sig_Pago},{item.Monto_Sig_Pago},{item.vFondeador},{item.Valida_Domi},{item.vAfiliateIdO},{item.vAfiliateO},{item.Saldo_Retencion_Adm},{item.RFC},{item.vMotiveExt},{item.iPeriodsExt},{item.vCommentExt},{item.nRetencion},{item.nJoPay},{item.iMaxDays},{item.vMaxDate},{item.nLiquidate},{item.nLiqPrin},{item.nLiqInt},{item.nLiqMor},{item.nLiqCha},{item.nLiqPrinTran},{item.nLiqIntTran},{item.nLiqMorTran},{item.nLiqChaTran},{item.nLiqRetTran},{item.vScoreBuro},{item.vCollectStatus},{item.nCAT},{item.vOpTable}");
            }

            var fileName = $"SaldosCartera_{selectedDate.Value:yyyyMMdd}.csv";
            return File(Encoding.UTF8.GetBytes(csv.ToString()), "text/csv", fileName);
        }

        [HttpPost]
        public IActionResult DownloadCurrentSelection(int? idCredito = null, int? idPersona = null, string? nombre = null, DateTime? selectedDate = null)
        {
            var query = _context.Saldos_Cartera.AsQueryable();

            // Apply filters
            if (idCredito.HasValue)
            {
                query = query.Where(s => s.Id_Credito.ToString().Contains(idCredito.Value.ToString()));
            }
            if (idPersona.HasValue)
            {
                query = query.Where(s => s.IdPersona.ToString().Contains(idPersona.Value.ToString()));
            }
            if (!string.IsNullOrEmpty(nombre))
            {
                query = query.Where(s => s.Nombre.Contains(nombre));
            }

            // Apply date filter if selected
            if (selectedDate.HasValue)
            {
                query = query.Where(s => s.FechaGenerado.HasValue && s.FechaGenerado.Value.Date == selectedDate.Value.Date);
            }

            var data = query.ToList();

            var csv = new StringBuilder();
            csv.AppendLine("Id_Solicitud,Id_Credito,IdPersona,Referencia,Afiliado,Nombre,Monto,Comision,Intereses_Totales,Monto_Total,Pagos,Amort_Pagadas,Capital_Pagado,Interes_Pagado,IVA_Int_Pagado,Cargo_PTardio_Pagado,Moratorio_Pagado,Pago_en_Exceso,Comision_Pagada,Total_Pagado,Ajustes_Capital,Saldo_Capital,Saldo_Interes,Saldo_IVA_Int,Saldo_Cargo_PTardio,Saldo_Moratorios,Saldo_Pago_Exceso,Saldo_Comision,Saldo_Total,Importe_de_Pago,Id_Convenio,Dependencia,Primer_Pago_Teorico,Ultimo_Pago,Tipo_Financiamiento,Capital_Vigente,Capital_Vencido,Intereses_Vencidos,Vencido,Sdo_Insoluto,Sdo_Total_c_ListasCobro,Sdo_Vencido_c_ListCobro,Estatus_Cartera,Estatus,Sucursal,Fecha_Desembolso,Frecuencia,Primer_Pago_Real,Ultimo_Pago_c_ListaCobro,Ultimo_Pago_Aplicado,Dias_Ultimo_Pago,Dias_Atraso,Cuotas_Atraso,Periodos_Atraso,Pago,Monto_Ultimo_Pago,Tasa_Int_Anual,Gestor,Motivo,Banco,Estado,Ciudad,Com_Vigente,Com_Vencida,Clabe,Sig_Pago,Monto_Sig_Pago,vFondeador,Valida_Domi,vAfiliateIdO,vAfiliateO,Saldo_Retencion_Adm,RFC,vMotiveExt,iPeriodsExt,vCommentExt,nRetencion,nJoPay,iMaxDays,vMaxDate,nLiquidate,nLiqPrin,nLiqInt,nLiqMor,nLiqCha,nLiqPrinTran,nLiqIntTran,nLiqMorTran,nLiqChaTran,nLiqRetTran,vScoreBuro,vCollectStatus,nCAT,vOpTable");

            foreach (var item in data)
            {
                csv.AppendLine($@"{item.Id_Solicitud},{item.Id_Credito},{item.IdPersona},{item.Referencia},{item.Afiliado},{item.Nombre},{item.Monto},{item.Comision},{item.Intereses_Totales},{item.Monto_Total},{item.Pagos},{item.Amort_Pagadas},{item.Capital_Pagado},{item.Interes_Pagado},{item.IVA_Int_Pagado},{item.Cargo_PTardio_Pagado},{item.Moratorio_Pagado},{item.Pago_en_Exceso},{item.Comision_Pagada},{item.Total_Pagado},{item.Ajustes_Capital},{item.Saldo_Capital},{item.Saldo_Interes},{item.Saldo_IVA_Int},{item.Saldo_Cargo_PTardio},{item.Saldo_Moratorios},{item.Saldo_Pago_Exceso},{item.Saldo_Comision},{item.Saldo_Total},{item.Importe_de_Pago},{item.Id_Convenio},{item.Dependencia},{item.Primer_Pago_Teorico},{item.Ultimo_Pago},{item.Tipo_Financiamiento},{item.Capital_Vigente},{item.Capital_Vencido},{item.Intereses_Vencidos},{item.Vencido},{item.Sdo_Insoluto},{item.Sdo_Total_c_ListasCobro},{item.Sdo_Vencido_c_ListCobro},{item.Estatus_Cartera},{item.Estatus},{item.Sucursal},{item.Fecha_Desembolso},{item.Frecuencia},{item.Primer_Pago_Real},{item.Ultimo_Pago_c_ListaCobro},{item.Ultimo_Pago_Aplicado},{item.Dias_Ultimo_Pago},{item.Dias_Atraso},{item.Cuotas_Atraso},{item.Periodos_Atraso},{item.Pago},{item.Monto_Ultimo_Pago},{item.Tasa_Int_Anual},{item.Gestor},{item.Motivo},{item.Banco},{item.Estado},{item.Ciudad},{item.Com_Vigente},{item.Com_Vencida},{item.Clabe},{item.Sig_Pago},{item.Monto_Sig_Pago},{item.vFondeador},{item.Valida_Domi},{item.vAfiliateIdO},{item.vAfiliateO},{item.Saldo_Retencion_Adm},{item.RFC},{item.vMotiveExt},{item.iPeriodsExt},{item.vCommentExt},{item.nRetencion},{item.nJoPay},{item.iMaxDays},{item.vMaxDate},{item.nLiquidate},{item.nLiqPrin},{item.nLiqInt},{item.nLiqMor},{item.nLiqCha},{item.nLiqPrinTran},{item.nLiqIntTran},{item.nLiqMorTran},{item.nLiqChaTran},{item.nLiqRetTran},{item.vScoreBuro},{item.vCollectStatus},{item.nCAT},{item.vOpTable}");
            }

            var fileName = $"SaldosCartera_{selectedDate?.ToString("yyyyMMdd") ?? "CurrentSelection"}.csv";
            return File(Encoding.UTF8.GetBytes(csv.ToString()), "text/csv", fileName);
        }

        private string FormatCsvLine(Saldos_Cartera item)
        {
#pragma warning disable CS8601 // Possible null reference assignment.
            return string.Join(",", new string[]
            {
                item.Id_Solicitud.ToString(),
                item.Id_Credito.ToString(),
                item.IdPersona.ToString(),
                item.Referencia,
                item.Afiliado,
                item.Nombre,
                item.Monto.ToString(),
                item.Comision.ToString(),
                item.Intereses_Totales.ToString(),
                item.Monto_Total.ToString(),
                item.Pagos.ToString(),
                item.Amort_Pagadas.ToString(),
                item.Capital_Pagado.ToString(),
                item.Interes_Pagado.ToString(),
                item.IVA_Int_Pagado.ToString(),
                item.Cargo_PTardio_Pagado.ToString(),
                item.Moratorio_Pagado.ToString(),
                item.Pago_en_Exceso.ToString(),
                item.Comision_Pagada.ToString(),
                item.Total_Pagado.ToString(),   
                item.Ajustes_Capital.ToString(),
                item.Saldo_Capital.ToString(),
                item.Saldo_Interes.ToString(),
                item.Saldo_IVA_Int.ToString(),
                item.Saldo_Cargo_PTardio.ToString(),
                item.Saldo_Moratorios.ToString(),
                item.Saldo_Pago_Exceso.ToString(),
                item.Saldo_Comision.ToString(),
                item.Saldo_Total.ToString(),
                item.Importe_de_Pago.ToString(),
                item.Id_Convenio.ToString(),
                item.Dependencia,
                item.Primer_Pago_Teorico.ToString(),
                item.Ultimo_Pago.ToString(),
                item.Tipo_Financiamiento,
                item.Capital_Vigente.ToString(),
                item.Capital_Vencido.ToString(),
                item.Intereses_Vencidos.ToString(),
                item.Vencido.ToString(),
                item.Sdo_Insoluto.ToString(),
                item.Sdo_Total_c_ListasCobro.ToString(),
                item.Sdo_Vencido_c_ListCobro.ToString(),
                item.Estatus_Cartera,
                item.Estatus,
                item.Sucursal,
                item.Fecha_Desembolso.ToString(),
                item.Frecuencia,
                item.Primer_Pago_Real.ToString(),
                item.Ultimo_Pago_c_ListaCobro.ToString(),
                item.Ultimo_Pago_Aplicado.ToString(),
                item.Dias_Ultimo_Pago.ToString(),
                item.Dias_Atraso.ToString(),
                item.Cuotas_Atraso.ToString(),
                item.Periodos_Atraso.ToString(),
                item.Pago.ToString(),
                item.Monto_Ultimo_Pago.ToString(),
                item.Tasa_Int_Anual.ToString(),
                item.Gestor,
                item.Motivo,
                item.Banco,
                item.Estado,
                item.Ciudad,
                item.Com_Vigente.ToString(),
                item.Com_Vencida.ToString(),
                item.Clabe,
                item.Sig_Pago.ToString(),
                item.Monto_Sig_Pago.ToString(),
                item.vFondeador,
                item.Valida_Domi,
                item.vAfiliateIdO,
                item.vAfiliateO,
                item.Saldo_Retencion_Adm.ToString(),
                item.RFC,
                item.vMotiveExt,
                item.iPeriodsExt.ToString(),
                item.vCommentExt,
                item.nRetencion.ToString(),
                item.nJoPay.ToString(),
                item.iMaxDays.ToString(),
                item.vMaxDate.ToString(),
                item.nLiquidate.ToString(),
                item.nLiqPrin.ToString(),
                item.nLiqInt.ToString(),
                item.nLiqMor.ToString(),
                item.nLiqCha.ToString(),
                item.nLiqPrinTran.ToString(),
                item.nLiqIntTran.ToString(),
                item.nLiqMorTran.ToString(),
                item.nLiqChaTran.ToString(),
                item.nLiqRetTran.ToString(),
                item.vScoreBuro,
                item.vCollectStatus,
                item.nCAT.ToString(),
                item.vOpTable
            }.Select(value => $"\"{value.Replace("\"", "\"\"")}\""));
#pragma warning restore CS8601 // Possible null reference assignment.
        }
    }
}
