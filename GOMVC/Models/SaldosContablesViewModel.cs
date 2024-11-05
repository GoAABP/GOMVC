using System.Collections.Generic;

namespace GOMVC.Models
{
    public class SaldosContablesViewModel
    {
        public required List<Saldos_Contables> Saldos_Contables { get; set; }
        public int PageNumber { get; set; }
        public int PageSize { get; set; }
        public int TotalItems { get; set; }
    }
}
