using System.Collections.Generic;

namespace GOMVC.Models
{
    public class GestionesViewModel
    {
        public required List<Gestiones> Gestiones { get; set; }
        public int PageNumber { get; set; }
        public int PageSize { get; set; }
        public int TotalItems { get; set; }
    }
}
