using GOMVC.Models;
using Microsoft.EntityFrameworkCore;

namespace GOMVC.Data
{
    public class AppDbContext : DbContext
    {
        public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

        public DbSet<Saldos_Cartera> Saldos_Cartera { get; set; }
        public DbSet<Saldos_Diarios> Saldos_Diarios { get; set; }

        public DbSet<User> Users { get; set; }


    }
}
