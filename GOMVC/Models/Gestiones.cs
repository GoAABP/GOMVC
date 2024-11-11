using System;
using System.ComponentModel.DataAnnotations;

namespace GOMVC.Models
{
    public class Gestiones
    {
        public int Indice { get; set; }
        public int Credito { get; set; }
        public DateTime FechaActividad { get; set; }
        public required string UsuarioRegistro { get; set; }
        public DateTime? FechaPromesa { get; set; }
        public decimal? MontoPromesa { get; set; }
        public int Resultado { get; set; }
        public string? CausaNoPago { get; set; }
        public int CodigoAccion { get; set; }
        public required string Comentarios { get; set; }
        public required string Producto { get; set; }
        public required string Origen { get; set; }
        public string? CausaNoDomiciliacion { get; set; }
        public required string ContactoGenerado { get; set; }
        public string? Coordenadas { get; set; }
    }
}
