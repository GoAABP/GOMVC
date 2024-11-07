using GOMVC.Models;
using Microsoft.EntityFrameworkCore;

namespace GOMVC.Data
{
    public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
    {
        public DbSet<Saldos_Cartera> Saldos_Cartera { get; set; }
        public DbSet<Saldos_Contables> Saldos_Contables { get; set; }
        public DbSet<Aplicacion_Pagos> Aplicacion_Pagos { get; set; }

        public DbSet<User> Users { get; set; }
        //public object? SaldosContables { get; internal set; }
    }
}
