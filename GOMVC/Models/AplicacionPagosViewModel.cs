using System.Collections.Generic;

namespace GOMVC.Models
{
    public class AplicacionPagosViewModel
    {
        public required List<Aplicacion_Pagos> Aplicacion_Pagos { get; set; }
        public int PageNumber { get; set; }
        public int PageSize { get; set; }
        public int TotalItems { get; set; }
    }
}
