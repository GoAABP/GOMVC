using System.ComponentModel.DataAnnotations;

namespace GOMVC.Models
{
    public class User
    {
        [Key]
        public int DBKEY { get; set; }
        public string? NAME { get; set; }
        public required string USERNAME { get; set; }
        public string? EMAIL { get; set; }
        public required string PASSWORD { get; set; }
    }
}
