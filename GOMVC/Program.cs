using Microsoft.EntityFrameworkCore;
using GOMVC.Data;
using Microsoft.AspNetCore.Authentication.Cookies;
using Serilog;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.ApplicationInsights.Extensibility;
using GOMVC.Controllers;

var builder = WebApplication.CreateBuilder(args);

// --- Configurar Serilog con impacto mínimo (solo consola) ---
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information() // Ajusta el nivel según necesidad
    .WriteTo.Console()
    .CreateLogger();

builder.Host.UseSerilog();

// --- Configurar Application Insights con muestreo adaptativo usando ConnectionString ---
builder.Services.AddApplicationInsightsTelemetry(options =>
{
    options.EnableAdaptiveSampling = true; // Reduce la cantidad de telemetría enviada
    options.ConnectionString = builder.Configuration["ApplicationInsights:ConnectionString"];
});

// --- Agregar servicios al contenedor ---
builder.Services.AddControllersWithViews();
builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseMySql(
        builder.Configuration.GetConnectionString("DefaultConnection"),
        new MySqlServerVersion(new Version(8, 0, 21))
    )
);

// --- Agregar servicios de autenticación ---
builder.Services.AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
    .AddCookie(options =>
    {
        options.LoginPath = "/User/Index";
        options.ExpireTimeSpan = TimeSpan.FromMinutes(30);
    });

// --- Configurar límites para carga de archivos ---
builder.Services.Configure<FormOptions>(options =>
{
    options.MultipartBodyLengthLimit = 104857600; // Límite de 100MB
});

// --- Agregar Health Checks (con chequeo de la base de datos) ---
builder.Services.AddHealthChecks()
    .AddDbContextCheck<AppDbContext>("Database");

// --- Registrar los controladores especializados ---
builder.Services.AddScoped<Backup_Zell_Controller>();
builder.Services.AddScoped<B2_Amortizacion_Controller>();
builder.Services.AddScoped<D1_Saldos_Cartera_Controller>();
builder.Services.AddScoped<D2_Saldos_Contables_Controller>();
builder.Services.AddScoped<D3_Aplicaciones_Pagos_Controller>();
builder.Services.AddScoped<D4_Otorgamiento_Creditos_Controller>();
builder.Services.AddScoped<D5_Gestiones_Controller>();
builder.Services.AddScoped<D6_Quebrantos_Controller>();
builder.Services.AddScoped<D7_Juicios_Controller>();
builder.Services.AddScoped<D8_Sistema_Controller>();
builder.Services.AddScoped<I2_Campaña_Quebrantos_Controller>();
builder.Services.AddScoped<INT_MDC_CONTROLLER>();
builder.Services.AddScoped<R3_LayoutMc_Controller>();

var app = builder.Build();

// --- Configurar el pipeline HTTP ---
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    app.UseHsts();
}

// --- Usar middleware personalizado para manejo global de excepciones ---
app.UseMiddleware<ExceptionHandlingMiddleware>();

app.UseHttpsRedirection();
app.UseStaticFiles();
app.UseRouting();
app.UseAuthentication();
app.UseAuthorization();

// --- Mapear endpoints para controladores y health checks ---
app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}");

app.MapHealthChecks("/health");

app.Run();


// --- Middleware para manejo global de excepciones ---
public class ExceptionHandlingMiddleware
{
    private readonly RequestDelegate _next;
    public ExceptionHandlingMiddleware(RequestDelegate next)
    {
        _next = next;
    }
    
    public async Task InvokeAsync(HttpContext httpContext)
    {
        try
        {
            await _next(httpContext);
        }
        catch (Exception ex)
        {
            // Registrar el error con Serilog
            Log.Error(ex, "Error no controlado en la aplicación");
            // Aquí se podría integrar lógica adicional para enviar alertas
            throw;
        }
    }
}
