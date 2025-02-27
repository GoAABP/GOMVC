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
using GOMVC.Controllers;


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
            "Backup Zell",
            "C1_Dependencias",
            "C2_Financiamientos",
            "C3_Motios",
            "C4_Bancos",
            "C6_Resultados_Avance",
            "D1_Saldos_Cartera",
            "D1B_Saldos_Cartera",
            "D2_Saldos_Contables",
            "D2B_Saldos_Contables",
            "D3_Aplicacion_Pagos",
            "D4_Otorgamiento_Creditos",
            "D5_Gestiones",
            "D6_Quebrantos",
            "D7_Juicios",
            "D8_Sistema",
            "D9_Gestores_Area",
            "I2_Campaña_Quebrantos",
            "R1_Quebrantos_Calculado",
            "R3_LayoutMc"
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
                case "backup zell":
                    var demograficsController = new Backup_Zell_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<Backup_Zell_Controller>>(),
                        _configuration);
                    result = await demograficsController.ProcessBackup();
                    break;

                case "d1_saldos_cartera":
                    var saldosCarteraController = new D1_Saldos_Cartera_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D1_Saldos_Cartera_Controller>>(),
                        _configuration);
                    result = await saldosCarteraController.D1_ProcessSaldosCartera();
                    break;

                case "d1b_saldos_cartera":
                    var saldosCarteraControllerb = new D1_Saldos_Cartera_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D1_Saldos_Cartera_Controller>>(),
                        _configuration);
                    result = await saldosCarteraControllerb.D1_ProcessHistoricSaldosCartera();
                    break;    

                case "d2_saldos_contables":
                    var saldosContablesController = new D2_Saldos_Contables_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D2_Saldos_Contables_Controller>>(),
                        _configuration);
                    result = await saldosContablesController.D2_ProcessSaldosContables();
                    break;

                case "d2b_saldos_contables":
                    var saldosContablesControllerb = new D2_Saldos_Contables_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D2_Saldos_Contables_Controller>>(),
                        _configuration);
                    result = await saldosContablesControllerb.D2_ProcessHistoricSaldosContables();
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
                    result = await gestionesController.D5_ProcessGestiones();
                    break;

                case "d6_quebrantos":
                    var quebrantosController = new D6_Quebrantos_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D6_Quebrantos_Controller>>(),
                        _configuration);
                    result = await quebrantosController.D6_ProcessQuebrantos();
                    break;
                
                case "d7_juicios":
                    var juiciosController = new D7_Juicios_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D7_Juicios_Controller>>(),
                        _configuration);
                    result = await juiciosController.D7_ProcessJuicios();
                    break;

                case "d8_sistema":
                    var sistemasController = new D8_Sistema_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D8_Sistema_Controller>>(),
                        _configuration);
                    result = await sistemasController.D8_ProcessSistema();
                    break;
                
                case "i2_campaña_quebrantos":
                    var campañasController = new I2_Campaña_Quebrantos_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<I2_Campaña_Quebrantos_Controller>>(),
                        _configuration);
                    result = await campañasController.Process();
                    break;

                case "r1_quebrantos_calculado":
                    var quebrantosControllerExport = new D6_Quebrantos_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<D6_Quebrantos_Controller>>(),
                        _configuration);
                    result = await quebrantosControllerExport.D6_ProcessQuebrantosCalculationsAndExport();
                    break;
                
                case "r3_layoutmc":
                    var layout_Mc_Controller = new R3_LayoutMc_Controller(
                        HttpContext.RequestServices.GetRequiredService<ILogger<R3_LayoutMc_Controller>>(),
                        _configuration);
                    result = await layout_Mc_Controller.R3_ProcessLayout();
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