using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace GOMVC.Controllers
{
    public class LoadDataController : Controller
    {
        private readonly ILogger<LoadDataController> _logger;
        private readonly IConfiguration _configuration;
        private readonly string _connectionString;
        private readonly string _filePath = @"C:\Users\Go Credit\Documents\DATA\FLAT FILES";
        private readonly string _historicFilePath = @"C:\Users\Go Credit\Documents\DATA\HISTORIC FILES";
        private static readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

        private readonly Backup_Zell_Controller _backupZellController;
        private readonly B2_Amortizacion_Controller _b2AmortizacionController;
        private readonly D1_Saldos_Cartera_Controller _d1SaldosCarteraController;
        private readonly D2_Saldos_Contables_Controller _d2SaldosContablesController;
        private readonly D3_Aplicaciones_Pagos_Controller _d3AplicacionesPagosController;
        private readonly D4_Otorgamiento_Creditos_Controller _d4OtorgamientoCreditosController;
        private readonly D5_Gestiones_Controller _d5GestionesController;
        private readonly D6_Quebrantos_Controller _d6QuebrantosController;
        private readonly D7_Juicios_Controller _d7JuiciosController;
        private readonly D8_Sistema_Controller _d8SistemaController;
        private readonly I2_Campaña_Quebrantos_Controller _i2CampañaQuebrantosController;
        private readonly INT_MDC_CONTROLLER _intMdcController;
        private readonly R3_LayoutMc_Controller _r3LayoutMcController;

        public LoadDataController(
            ILogger<LoadDataController> logger,
            IConfiguration configuration,
            Backup_Zell_Controller backupZellController,
            B2_Amortizacion_Controller b2AmortizacionController,
            D1_Saldos_Cartera_Controller d1SaldosCarteraController,
            D2_Saldos_Contables_Controller d2SaldosContablesController,
            D3_Aplicaciones_Pagos_Controller d3AplicacionesPagosController,
            D4_Otorgamiento_Creditos_Controller d4OtorgamientoCreditosController,
            D5_Gestiones_Controller d5GestionesController,
            D6_Quebrantos_Controller d6QuebrantosController,
            D7_Juicios_Controller d7JuiciosController,
            D8_Sistema_Controller d8SistemaController,
            I2_Campaña_Quebrantos_Controller i2CampañaQuebrantosController,
            INT_MDC_CONTROLLER intMdcController,
            R3_LayoutMc_Controller r3LayoutMcController)
        {
            _logger = logger;
            _configuration = configuration;
            _connectionString = _configuration.GetConnectionString("DefaultConnection");

            _backupZellController = backupZellController;
            _b2AmortizacionController = b2AmortizacionController;
            _d1SaldosCarteraController = d1SaldosCarteraController;
            _d2SaldosContablesController = d2SaldosContablesController;
            _d3AplicacionesPagosController = d3AplicacionesPagosController;
            _d4OtorgamientoCreditosController = d4OtorgamientoCreditosController;
            _d5GestionesController = d5GestionesController;
            _d6QuebrantosController = d6QuebrantosController;
            _d7JuiciosController = d7JuiciosController;
            _d8SistemaController = d8SistemaController;
            _i2CampañaQuebrantosController = i2CampañaQuebrantosController;
            _intMdcController = intMdcController;
            _r3LayoutMcController = r3LayoutMcController;
        }

        [HttpGet]
        public IActionResult Index()
        {
            var activities = new List<string>
            {
                "Backup Zell",
                "B2_Amortizaciones",
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
                "D6B_Quebrantos",
                "D7_Juicios",
                "D8_Sistema",
                "D9_Gestores_Area",
                "I2_Campaña_Quebrantos",
                "INT_MDC",
                "INT2_MDC",
                "R1_Quebrantos_Calculado_Most_Recent",
                "R1_Quebrantos_Calculado_Specific_Date",
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
                        result = await _backupZellController.ProcessBackup();
                        break;
                    case "b2_amortizaciones":
                        result = await _b2AmortizacionController.B2_Process();
                        break;
                    case "d1_saldos_cartera":
                        result = await _d1SaldosCarteraController.D1_ProcessSaldosCartera();
                        break;
                    case "d1b_saldos_cartera":
                        result = await _d1SaldosCarteraController.D1_ProcessHistoricSaldosCartera();
                        break;
                    case "d2_saldos_contables":
                        result = await _d2SaldosContablesController.D2_ProcessSaldosContables();
                        break;
                    case "d2b_saldos_contables":
                        result = await _d2SaldosContablesController.D2_ProcessHistoricSaldosContables();
                        break;
                    case "d3_aplicacion_pagos":
                        result = await _d3AplicacionesPagosController.D3_ProcessAplicacionPagos();
                        break;
                    case "d4_otorgamiento_creditos":
                        result = await _d4OtorgamientoCreditosController.D4_ProcessOtorgamientoCreditos();
                        break;
                    case "d5_gestiones":
                        result = await _d5GestionesController.D5_ProcessGestiones();
                        break;
                    case "d6_quebrantos":
                        result = await _d6QuebrantosController.D6_ProcessQuebrantos();
                        break;
                    case "d6b_quebrantos":
                        result = await _d6QuebrantosController.D6_ProcessHistoricQuebrantos();
                        break;
                    case "d7_juicios":
                        result = await _d7JuiciosController.D7_ProcessJuicios();
                        break;
                    case "d8_sistema":
                        result = await _d8SistemaController.D8_ProcessSistema();
                        break;
                    case "i2_campaña_quebrantos":
                        result = await _i2CampañaQuebrantosController.Process();
                        break;
                    case "int_mdc":
                        result = await _intMdcController.ProcessAll();
                        break;
                    case "int2_mdc":
                        result = await _intMdcController.ProcessSC();
                        break;
                    case "r1_quebrantos_calculado_most_recent":
                        result = await _d6QuebrantosController.D6_ProcessQuebrantosCalculationsAndExport();
                        break;
                    case "r3_layoutmc":
                        result = await _r3LayoutMcController.R3_ProcessLayout();
                        break;
                    default:
                        _logger.LogError("Unknown activity: {ActivityName}", activityName);
                        return BadRequest($"Unknown activity: {activityName}");
                }
                return result;
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, "Error processing activity: {ActivityName}", activityName);
                return StatusCode(500, $"Internal server error while processing activity: {activityName}");
            }
        }
    }
}
