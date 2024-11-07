using System;
using System.ComponentModel.DataAnnotations;

namespace GOMVC.Models
{
    public class Aplicacion_Pagos
    {
        [Key]
        public int Id_Pago { get; set; }
        public int? Id_Credito { get; set; }
        public int? Id_Convenio { get; set; }
        public string? Convenio { get; set; }
        public string? Referencia { get; set; }
        public string? Nombre_Cliente { get; set; }
        public string? Financiamiento { get; set; }
        public string? Origen_de_Movimiento { get; set; }
        public DateTime? Fecha_Pago { get; set; }
        public DateTime? Fecha_Aplicacion { get; set; }
        public DateTime? Fecha_Deposito { get; set; }
        public string? Status { get; set; }
        public decimal? Pago { get; set; }
        public decimal? Capital { get; set; }
        public decimal? Interes { get; set; }
        public decimal? IVA_Int { get; set; }
        public decimal? Comision_Financiada { get; set; }
        public decimal? IVA_Comision_Financ { get; set; }
        public decimal? Moratorios { get; set; }
        public decimal? IVA_Mora { get; set; }
        public decimal? Pago_Tardio { get; set; }
        public decimal? IVA_PagoTardio { get; set; }
        public decimal? Recuperacion { get; set; }
        public decimal? IVA_Recup { get; set; }
        public decimal? Com_Liquidacion { get; set; }
        public decimal? IVA_Com_Liquidacion { get; set; }
        public decimal? Retencion_X_Admon { get; set; }
        public decimal? IVA_Retencion_X_Admon { get; set; }
        public decimal? Pago_Exceso { get; set; }
        public string? Gestor { get; set; }
        public string? Forma_de_pago { get; set; }
        public string? vMotive { get; set; }
    }
}
