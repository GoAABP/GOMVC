using System;
using System.ComponentModel.DataAnnotations;

namespace GOMVC.Models
{
    public class Saldos_Cartera
    {
        [Key]
        public int DBKEY { get; set; }
        public int? Id_Solicitud { get; set; }
        public int? Id_Credito { get; set; }
        public int? IdPersona { get; set; }
        public string? Referencia { get; set; }
        public string? Afiliado { get; set; }
        public string? Nombre { get; set; }
        public decimal? Monto { get; set; }
        public decimal? Comision { get; set; }
        public decimal? Intereses_Totales { get; set; }
        public decimal? Monto_Total { get; set; }
        public int? Pagos { get; set; }
        public int? Amort_Pagadas { get; set; }
        public decimal? Capital_Pagado { get; set; }
        public decimal? Interes_Pagado { get; set; }
        public decimal? IVA_Int_Pagado { get; set; }
        public decimal? Cargo_PTardio_Pagado { get; set; }
        public decimal? Moratorio_Pagado { get; set; }
        public decimal? Pago_en_Exceso { get; set; }
        public decimal? Comision_Pagada { get; set; }
        public decimal? Total_Pagado { get; set; }
        public decimal? Ajustes_Capital { get; set; }
        public decimal? Saldo_Capital { get; set; }
        public decimal? Saldo_Interes { get; set; }
        public decimal? Saldo_IVA_Int { get; set; }
        public decimal? Saldo_Cargo_PTardio { get; set; }
        public decimal? Saldo_Moratorios { get; set; }
        public decimal? Saldo_Pago_Exceso { get; set; }
        public decimal? Saldo_Comision { get; set; }
        public decimal? Saldo_Total { get; set; }
        public decimal? Importe_de_Pago { get; set; }
        public decimal? Id_Convenio { get; set; }
        public string? Dependencia { get; set; }
        public DateTime? Primer_Pago_Teorico { get; set; }
        public DateTime? Ultimo_Pago { get; set; }
        public string? Tipo_Financiamiento { get; set; }
        public decimal? Capital_Vigente { get; set; }
        public decimal? Capital_Vencido { get; set; }
        public decimal? Intereses_Vencidos { get; set; }
        public decimal? Vencido { get; set; }
        public decimal? Sdo_Insoluto { get; set; }
        public decimal? Sdo_Total_c_ListasCobro { get; set; }
        public decimal? Sdo_Vencido_c_ListCobro { get; set; }
        public string? Estatus_Cartera { get; set; }
        public string? Estatus { get; set; }
        public string? Sucursal { get; set; }
        public DateTime? Fecha_Desembolso { get; set; }
        public string? Frecuencia { get; set; }
        public DateTime? Primer_Pago_Real { get; set; }
        public DateTime? Ultimo_Pago_c_ListaCobro { get; set; }
        public DateTime? Ultimo_Pago_Aplicado { get; set; }
        public int? Dias_Ultimo_Pago { get; set; }
        public int? Dias_Atraso { get; set; }
        public int? Cuotas_Atraso { get; set; }
        public int? Periodos_Atraso { get; set; }
        public decimal? Pago { get; set; }
        public decimal? Monto_Ultimo_Pago { get; set; }
        public decimal? Tasa_Int_Anual { get; set; }
        public string? Gestor { get; set; }
        public string? Motivo { get; set; }
        public string? Banco { get; set; }
        public string? Estado { get; set; }
        public string? Ciudad { get; set; }
        public decimal? Com_Vigente { get; set; }
        public decimal? Com_Vencida { get; set; }
        public string? Clabe { get; set; }
        public DateTime? Sig_Pago { get; set; }
        public decimal? Monto_Sig_Pago { get; set; }
        public string? vFondeador { get; set; }
        public string? Valida_Domi { get; set; }
        public string? vAfiliateIdO { get; set; }
        public string? vAfiliateO { get; set; }
        public decimal? Saldo_Retencion_Adm { get; set; }
        public string? RFC { get; set; }
        public string? vMotiveExt { get; set; }
        public int? iPeriodsExt { get; set; }
        public string? vCommentExt { get; set; }
        public decimal? nRetencion { get; set; }
        public decimal? nJoPay { get; set; }
        public int? iMaxDays { get; set; }
        public DateTime? vMaxDate { get; set; }
        public decimal? nLiquidate { get; set; }
        public decimal? nLiqPrin { get; set; }
        public decimal? nLiqInt { get; set; }
        public decimal? nLiqMor { get; set; }
        public decimal? nLiqCha { get; set; }
        public decimal? nLiqPrinTran { get; set; }
        public decimal? nLiqIntTran { get; set; }
        public decimal? nLiqMorTran { get; set; }
        public decimal? nLiqChaTran { get; set; }
        public decimal? nLiqRetTran { get; set; }
        public string? vScoreBuro { get; set; }
        public string? vCollectStatus { get; set; }
        public decimal? nCAT { get; set; }
        public string? vOpTable { get; set; }
        public DateTime? FechaGenerado { get; set; }
    }
}
