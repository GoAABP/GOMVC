using System;
using System.ComponentModel.DataAnnotations;

namespace GOMVC.Models
{
    public class Otorgamiento_Creditos
    {
        [Key]
        public int Id_Credito { get; set; }

        [StringLength(50)]
        public string Referencia { get; set; }

        [StringLength(100)]
        public string Nombre { get; set; }

        [DataType(DataType.Date)]
        public DateTime? Fecha_Apertura { get; set; }

        [DataType(DataType.Date)]
        public DateTime? F_Cobro { get; set; }

        public int? Id_Convenio { get; set; }

        [StringLength(100)]
        public string Convenio { get; set; }

        public int? Id_Sucursal { get; set; }

        [StringLength(100)]
        public string Sucursal { get; set; }

        [DataType(DataType.Currency)]
        public decimal? Capital { get; set; }

        [DataType(DataType.Date)]
        public DateTime? Primer_Pago { get; set; }

        [DataType(DataType.Currency)]
        public decimal? Comision { get; set; }

        [DataType(DataType.Currency)]
        public decimal? IVA { get; set; }

        [DataType(DataType.Currency)]
        public decimal? Cobertura { get; set; }

        [DataType(DataType.Currency)]
        public decimal? IVA_Cobertura { get; set; }

        [DataType(DataType.Currency)]
        public decimal? Disposicion { get; set; }

        [DataType(DataType.Currency)]
        public decimal? Monto_Retenido { get; set; }

        [DataType(DataType.Currency)]
        public decimal? Pago_de_Deuda { get; set; }

        [DataType(DataType.Currency)]
        public decimal? Comision_Financiada { get; set; }

        [DataType(DataType.Currency)]
        public decimal? IVA_Comision_Financiada { get; set; }

        public int? Solicitud { get; set; }

        public int? Vendedor { get; set; }

        [StringLength(100)]
        public string Nombre_Vendedor { get; set; }

        [StringLength(50)]
        public string TipoVendedor { get; set; }

        public int? vSupervisorId { get; set; }

        [StringLength(100)]
        public string vSupName { get; set; }

        [StringLength(100)]
        public string Producto { get; set; }

        [StringLength(100)]
        public string Descripcion_Tasa { get; set; }

        [DataType(DataType.Currency)]
        public decimal? Persona { get; set; }

        public int? Plazo { get; set; }

        public int? Id_Producto { get; set; }

        [StringLength(100)]
        public string vCampaign { get; set; }

        [StringLength(100)]
        public string Tipo_de_Financiamiento { get; set; }

        public int? vFinancingTypeId { get; set; }

        [StringLength(100)]
        public string vAliado { get; set; }
    }
}
