using System.Collections.Generic;

namespace GOMVC.Models
{
    public class SaldosCarteraViewModel
    {
        public required List<Saldos_Cartera> SaldosCartera { get; set; }
        public int PageNumber { get; set; }
        public int PageSize { get; set; }
        public int TotalItems { get; set; }
    }
}
