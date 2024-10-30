using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace GOMVC.Models
{
    public class Saldos_Cartera
    {
        [Key]
        public int DBKEY { get; set; }
        public int? IdSolicitud { get; set; }
        public int? IdCredito { get; set; }
        public int? IdPersona { get; set; }
        public string? Referencia { get; set; }
        public string? Afiliado { get; set; }
        public string? Nombre { get; set; }
        public decimal? Monto { get; set; }
        public decimal? Comision { get; set; }
        public decimal? InteresesTotales { get; set; }
        public decimal? MontoTotal { get; set; }
        public int? Pagos { get; set; }
        public int? AmortPagadas { get; set; }
        public decimal? CapitalPagado { get; set; }
        public decimal? InteresPagado { get; set; }
        public decimal? IVAPagado { get; set; }
        public decimal? CargoPTardioPagado { get; set; }
        public decimal? MoratorioPagado { get; set; }
        public decimal? PagoEnExceso { get; set; }
        public decimal? ComisionPagada { get; set; }
        public decimal? TotalPagado { get; set; }
        public decimal? AjustesCapital { get; set; }
        public decimal? SaldoCapital { get; set; }
        public decimal? SaldoInteres { get; set; }
        public decimal? SaldoIVAInteres { get; set; }
        public decimal? SaldoCargoPTardio { get; set; }
        public decimal? SaldoMoratorios { get; set; }
        public decimal? SaldoPagoExceso { get; set; }
        public decimal? SaldoComision { get; set; }
        public decimal? SaldoTotal { get; set; }
        public decimal? ImportePago { get; set; }
        public string? IdConvenio { get; set; }
        public string? Dependencia { get; set; }
        public DateTime? PrimerPagoTeorico { get; set; }
        public DateTime? UltimoPago { get; set; }
        public string? TipoFinanciamiento { get; set; }
        public decimal? CapitalVigente { get; set; }
        public decimal? CapitalVencido { get; set; }
        public decimal? InteresesVencidos { get; set; }
        public decimal? Vencido { get; set; }
        public decimal? SaldoInsoluto { get; set; }
        public decimal? SdoTotalCListasCobro { get; set; }
        public decimal? SdoVencidoCListCobro { get; set; }
        public string? EstatusCartera { get; set; }
        public string? Estatus { get; set; }
        public string? Sucursal { get; set; }
        public DateTime? FDesembolso { get; set; }
        public string? Frecuencia { get; set; }
        public DateTime? PrimerPagoReal { get; set; }
        public DateTime? UltimoPagoCListCobro { get; set; }
        public DateTime? UltimoPagoAplicado { get; set; }
        public int? DiasUltimoPago { get; set; }
        public int? DiasAtraso { get; set; }
        public int? CuotasAtraso { get; set; }
        public int? PeriodosAtraso { get; set; }
        public decimal? Pago { get; set; }
        public decimal? MontoUltimoPago { get; set; }
        public decimal? TasaIntAnual { get; set; }
        public string? Gestor { get; set; }
        public string? Motivo { get; set; }
        public string? Banco { get; set; }
        public string? Estado { get; set; }
        public string? Ciudad { get; set; }
        public decimal? ComVigente { get; set; }
        public decimal? ComVencida { get; set; }
        public string? Clabe { get; set; }
        public DateTime? SigPago { get; set; }
        public decimal? MontoSigPago { get; set; }
        public string? VFondeador { get; set; }
        public string? ValidaDomi { get; set; }
        public string? VAfiliateIdO { get; set; }
        public string? VAfiliateO { get; set; }
        public decimal? SaldoRetencionAdm { get; set; }
        public string? RFC { get; set; }
        public string? VMotiveExt { get; set; }
        public int? IPeriodsExt { get; set; }
        public string? VCommentExt { get; set; }
        public decimal? NRetencion { get; set; }
        public decimal? NJoPay { get; set; }
        public int? IMaxDays { get; set; }
        public DateTime? VMaxDate { get; set; }
        public decimal? NLiquidate { get; set; }
        public decimal? NLiqPrin { get; set; }
        public decimal? NLiqInt { get; set; }
        public decimal? NLiqMor { get; set; }
        public decimal? NLiqCha { get; set; }
        public decimal? NLiqPrinTran { get; set; }
        public decimal? NLiqIntTran { get; set; }
        public decimal? NLiqMorTran { get; set; }
        public decimal? NLiqChaTran { get; set; }
        public decimal? NLiqRetTran { get; set; }
        public int? VScoreBuro { get; set; }
        public string? VCollectStatus { get; set; }
        public decimal? NCAT { get; set; }
        public string? VOpTable { get; set; }
        public DateTime? FechaGenerado { get; set; }
    }
}
