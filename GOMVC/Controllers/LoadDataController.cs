using Microsoft.AspNetCore.Mvc;
using OfficeOpenXml;
using MySql.Data.MySqlClient;
using System.Text;
using System.Text.RegularExpressions;
using System.Net;
using MimeKit;
using MailKit.Security;
using SharpCompress.Archives;
using SharpCompress.Common;
using System.Data;
using Org.BouncyCastle.Asn1.Misc;


public class LoadDataController : Controller
{
 private readonly ILogger<LoadDataController> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
    private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";
    private static readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    public LoadDataController(ILogger<LoadDataController> logger, IConfiguration configuration)
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    {
        _logger = logger;
        _configuration = configuration;
#pragma warning disable CS8601 // Possible null reference assignment.
        _connectionString = _configuration.GetConnectionString("DefaultConnection");
#pragma warning restore CS8601 // Possible null reference assignment.
    }

    [HttpGet]
    public IActionResult Index()
    {
        var activities = new List<string>
        {
            "D1_Saldos_Cartera",
            "D2-Saldos_Contables",
            "D3_Aplicacion_Pagos",
            "D4_Otorgamiento_Creditos",
            "D5_Gestiones",
            "D6_Quebrantos",
            "D7_Juicios",
            "D8_Sistema",
            "D9_Gestores_Area",
            "C1_Dependencias",
            "C2_Financiamientos",
            "C3_Motios",
            "C4_Bancos",
            "C6_Resultados_Avance",
            "15-LoadDemograficos"
        };
        return View(activities);
    }

    [HttpPost("HandleActivity")]
    public async Task<IActionResult> HandleActivity(string activityName)
    {
        try
        {
            IActionResult result;

            switch (activityName.ToLower())
            {
                case "d1_saldos_cartera":
                    var saldosCarteraController = new D1_Saldos_Cartera_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D1_Saldos_Cartera_Controller>>(),
                        _configuration);
                    result = await saldosCarteraController.D1_ProcessSaldosCartera();
                    break;

                case "d2-saldos_contables":
                    var saldosContablesController = new D2_Saldos_Contables_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D2_Saldos_Contables_Controller>>(),
                        _configuration);
                    result = await saldosContablesController.D2_ProcessSaldosContables();
                    break;

                case "d3_aplicacion_pagos":
                    var aplicacionesPagosController = new D3_Aplicaciones_Pagos_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D3_Aplicaciones_Pagos_Controller>>(),
                        _configuration);
                    result = await aplicacionesPagosController.D3_ProcessAplicacionPagos();
                    break;

                case "d4_otorgamiento_creditos":
                    var otorgamientoCreditosController = new D4_Otorgamiento_Creditos_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D4_Otorgamiento_Creditos_Controller>>(),
                        _configuration);
                    result = await otorgamientoCreditosController.D4_ProcessOtorgamientoCreditos();
                    break;

                case "d5_gestiones":
                    var gestionesController = new D5_Gestiones_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D5_Gestiones_Controller>>(),
                        _configuration);
                    result = await gestionesController.D5_ProcessFile();
                    break;

                case "d6_quebrantos":
                    var quebrantosController = new D6_Quebrantos_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D6_Quebrantos_Controller>>(),
                        _configuration);
                    result = await quebrantosController.D6_ProcessQuebrantos();
                    break;

                default:
                    _logger.LogError($"Unknown activity: {activityName}");
                    return BadRequest($"Unknown activity: {activityName}");
            }

            // Return the actual result of the activity
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error processing activity: {activityName}");
            return StatusCode(500, $"Internal server error while processing activity: {activityName}");
        }
    }
}