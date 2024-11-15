using System;
using System.ComponentModel.DataAnnotations;

namespace GOMVC.Models
{
    public class Gestiones
    {
        public string AgenciaRegistro { get; set; }
        public string? CausaNoPago { get; set; }
        public string? CausaNoDomiciliacion { get; set; }
        public int CodigoAccion { get; set; }
        public int CodigoResultado { get; set; }
        public string Comentarios { get; set; }
        public string ContactoGenerado { get; set; }
        public string? Coordenadas { get; set; }
        public int Credito { get; set; }
        public string? EstatusPromesa { get; set; }
        public DateTime FechaActividad { get; set; }
        public DateTime? FechaPromesa { get; set; }
        public decimal? MontoPromesa { get; set; }
        public string Origen { get; set; }
        public string Producto { get; set; }
        public string Resultado { get; set; }
        public string? Telefono { get; set; }
        public string? TipoPago { get; set; }
        public string UsuarioRegistro { get; set; }
    }
}
