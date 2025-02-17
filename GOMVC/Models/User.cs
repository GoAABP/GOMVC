using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace GOMVC.Models
{
    public class User
    {
        [Key]
        [Column("User_Key")]
        public int UserKey { get; set; }

        [Column("NAME")]
        public string? Name { get; set; }  // Allow null

        [Required]
        [Column("USERNAME")]
        public string Username { get; set; }

        [Column("EMAIL")]
        public string? Email { get; set; }  // Allow null

        [Required]
        [DataType(DataType.Password)]
        [Column("PASSWORD")]
        public string Password { get; set; }
    }
}
