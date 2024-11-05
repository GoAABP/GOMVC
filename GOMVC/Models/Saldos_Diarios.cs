using System;
using System.ComponentModel.DataAnnotations;

public class Saldos_Diarios
{
    [Key]
    public int DBKEY { get; set; }
    [Required]
    public int Id_Credito { get; set; }
    [Required]
    [StringLength(50)]
    public required string Referencia { get; set; }
    [StringLength(100)]
    public string? Nombre { get; set; }
    public int? Id_Sucursal { get; set; }
    [StringLength(100)]
    public string? Sucursal { get; set; }
    public decimal? Id_Convenio { get; set; }
    [StringLength(100)]
    public string? Convenio { get; set; }
    [StringLength(50)]
    public string? Financiamiento { get; set; }
    [StringLength(10)]
    public string? Estatus_Inicial { get; set; }
    [StringLength(10)]
    public string? Estatus_Final { get; set; }
    public DateTime? Fecha_Apertura { get; set; }
    public DateTime? Fecha_Terminacion { get; set; }
    public decimal? Importe { get; set; }
    public int? Dias_Atraso { get; set; }
    public int? Cuotas_Atrasadas { get; set; }
    public int? Periodos_Atraso { get; set; }
    public int? Pagos_Sostenidos { get; set; }
    public decimal? Pago { get; set; }
    [StringLength(20)]
    public string? Frecuencia { get; set; }
    public DateTime? Fecha_Ultimo_Pago { get; set; }
    public decimal? Importe_Ultimo_Pago { get; set; }
    public decimal? Saldo_Inicial_Capital { get; set; }
    public decimal? Otorgado { get; set; }
    public decimal? Pagos { get; set; }
    public decimal? Ajuste_Cargo_Capital { get; set; }
    public decimal? Ajuste_Abono_Capital { get; set; }
    public decimal? Saldo_Final_Capital { get; set; }
    public decimal? Calculo { get; set; }
    public decimal? Diferencia { get; set; }
    public decimal? Capital_Vigente { get; set; }
    public decimal? Capital_Vencido { get; set; }
    public decimal? Saldo_Inicial_Interes { get; set; }
    public decimal? Devengamiento { get; set; }
    public decimal? Pagos_Interes { get; set; }
    public decimal? Ajuste_Cargo_Interes { get; set; }
    public decimal? Ajuste_Abono_Interes { get; set; }
    public decimal? Interes_No_Devengado { get; set; }
    public decimal? Saldo_Final_Interes { get; set; }
    public decimal? Calculo_Interes { get; set; }
    public decimal? Diferencia_Interes { get; set; }
    public decimal? Interes_Devengado { get; set; }
    public decimal? IVA_Interes_Devengado { get; set; }
    public decimal? Interes_No_DevengadoB { get; set; }
    public DateTime? Fecha_Cartera_Vencida { get; set; }
    public decimal? Saldo_Contable { get; set; }
    public decimal? Saldo_Insoluto { get; set; }
    public decimal? Porc_Provision { get; set; }
    public decimal? Reserva { get; set; }
    public decimal? nCAT { get; set; }
    [StringLength(50)]
    public string? vOpTable { get; set; }
    [StringLength(20)]
    public string? Status { get; set; }
    public DateTime? FechaGenerado { get; set; }
}
