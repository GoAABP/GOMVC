using GOMVC.Models;
using Microsoft.EntityFrameworkCore;

namespace GOMVC.Data
{
    public class AppDbContext : DbContext
    {
        public AppDbContext(DbContextOptions<AppDbContext> options)
            : base(options)
        {
        }

        public DbSet<Saldos_Cartera> Saldos_Cartera { get; set; }
        public DbSet<Saldos_Contables> Saldos_Contables { get; set; }
        public DbSet<Aplicacion_Pagos> Aplicacion_Pagos { get; set; }
        public DbSet<Otorgamiento_Creditos> Otorgamiento_Creditos { get; set; }
        public DbSet<Gestiones> Gestiones { get; set; }
        public DbSet<User> Users { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            // Configure keyless entities (for read-only views or queries)
            modelBuilder.Entity<Gestiones>().HasNoKey();
            modelBuilder.Entity<Otorgamiento_Creditos>().HasNoKey();

            // Define the primary key for the User entity using the new property name
            modelBuilder.Entity<User>()
                .HasKey(u => u.UserKey);
        }
    }
}
