using GOMVC.Data;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;

public class ScheduledLoadService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;

    public ScheduledLoadService(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromHours(24), stoppingToken); // Adjust the interval as needed

            using (var scope = _serviceProvider.CreateScope())
            {
                var context = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                // Load data from flat file and insert into Stage_Saldos_Cartera
                // Then move data from Stage_Saldos_Cartera to Saldos_Cartera
            }
        }
    }
}
