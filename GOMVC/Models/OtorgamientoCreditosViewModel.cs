using System.Collections.Generic;

namespace GOMVC.Models
{
    public class OtorgamientoCreditosViewModel
    {
        public List<Otorgamiento_Creditos> Otorgamiento_Creditos { get; set; }
        public int PageNumber { get; set; }
        public int PageSize { get; set; }
        public int TotalItems { get; set; }
    }
}
