using GOMVC.Models;
using Microsoft.EntityFrameworkCore;

namespace GOMVC.Data
{
    public class AppDbContext : DbContext
    {
        public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

        public DbSet<SaldoPrueba> SaldosPruebas { get; set; }
        public DbSet<Saldos_Cartera> Saldos_Cartera { get; set; }
        
        public DbSet<User> Users { get; set; }


    }
}
