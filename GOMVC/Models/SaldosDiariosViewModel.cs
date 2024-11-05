using System.Collections.Generic;

namespace GOMVC.Models
{
    public class SaldosDiariosViewModel
    {
        public required List<Saldos_Diarios> Saldos_Diarios { get; set; }
        public int PageNumber { get; set; }
        public int PageSize { get; set; }
        public int TotalItems { get; set; }
    }
}
