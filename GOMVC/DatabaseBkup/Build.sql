-- Create database
create database godatabase;
-- Ensure the database is on use
use godatabase;

CREATE TABLE Users (
    DBKEY INT AUTO_INCREMENT PRIMARY KEY,
    NAME VARCHAR(255),
    USERNAME VARCHAR(255) NOT NULL,
    EMAIL VARCHAR(255),
    PASSWORD VARCHAR(255) NOT NULL
);

CREATE TABLE Stage_Saldos_Cartera (
    Id_Solicitud INT,
    Id_Credito INT,
    IdPersona INT,
    Referencia VARCHAR(50) NULL,
    Afiliado VARCHAR(50) NULL,
    Nombre VARCHAR(100) NULL,
    Monto DECIMAL(10,2) NULL,
    Comision DECIMAL(10,2) NULL,
    Intereses_Totales DECIMAL(10,2) NULL,
    Monto_Total DECIMAL(10,2) NULL,
    Pagos INT NULL,
    Amort_Pagadas INT NULL,
    Capital_Pagado DECIMAL(10,2) NULL,
    Interes_Pagado DECIMAL(10,2) NULL,
    IVA_Int_Pagado DECIMAL(10,2) NULL,
    Cargo_PTardio_Pagado DECIMAL(10,2) NULL,
    Moratorio_Pagado DECIMAL(10,2) NULL,
    Pago_en_Exceso DECIMAL(10,2) NULL,
    Comision_Pagada DECIMAL(10,2) NULL,
    Total_Pagado DECIMAL(10,2) NULL,
    Ajustes_Capital DECIMAL(10,2) NULL,
    Saldo_Capital DECIMAL(10,2) NULL,
    Saldo_Interes DECIMAL(10,2) NULL,
    Saldo_IVA_Int DECIMAL(10,2) NULL,
    Saldo_Cargo_PTardio DECIMAL(10,2) NULL,
    Saldo_Moratorios DECIMAL(10,2) NULL,
    Saldo_Pago_Exceso DECIMAL(10,2) NULL,
    Saldo_Comision DECIMAL(10,2) NULL,
    Saldo_Total DECIMAL(10,2) NULL,
    Importe_de_Pago DECIMAL(10,2) NULL,
    Id_Convenio DECIMAL(10,2) NULL,
    Dependencia VARCHAR(100) NULL,
    Primer_Pago_Teorico TEXT NULL,
    Ultimo_Pago TEXT NULL,
    Tipo_Financiamiento VARCHAR(50) NULL,
    Capital_Vigente DECIMAL(10,2) NULL,
    Capital_Vencido DECIMAL(10,2) NULL,
    Intereses_Vencidos DECIMAL(10,2) NULL,
    Vencido DECIMAL(10,2) NULL,
    Sdo_Insoluto DECIMAL(10,2) NULL,
    Sdo_Total_c_ListasCobro DECIMAL(10,2) NULL,
    Sdo_Vencido_c_ListCobro DECIMAL(10,2) NULL,
    Estatus_Cartera VARCHAR(50) NULL,
    Estatus VARCHAR(20) NULL,
    Sucursal VARCHAR(100) NULL,
    Fecha_Desembolso TEXT NULL,
    Frecuencia VARCHAR(20) NULL,
    Primer_Pago_Real TEXT NULL,
    Ultimo_Pago_c_ListaCobro TEXT NULL,
    Ultimo_Pago_Aplicado TEXT NULL,
    Dias_Ultimo_Pago INT NULL,
    Dias_Atraso INT NULL,
    Cuotas_Atraso INT NULL,
    Periodos_Atraso INT NULL,
    Pago DECIMAL(10,2) NULL,
    Monto_Ultimo_Pago DECIMAL(10,2) NULL,
    Tasa_Int_Anual DECIMAL(10,2) NULL,
    Gestor VARCHAR(100) NULL,
    Motivo VARCHAR(100) NULL,
    Banco VARCHAR(100) NULL,
    Estado VARCHAR(100) NULL,
    Ciudad VARCHAR(100) NULL,
    Com_Vigente DECIMAL(10,2) NULL,
    Com_Vencida DECIMAL(10,2) NULL,
    Clabe VARCHAR(50) NULL,
    Sig_Pago TEXT NULL,
    Monto_Sig_Pago DECIMAL(10,2) NULL,
    vFondeador VARCHAR(100) NULL,
    Valida_Domi VARCHAR(100) NULL,
    vAfiliateIdO VARCHAR(100) NULL,
    vAfiliateO VARCHAR(100) NULL,
    Saldo_Retencion_Adm DECIMAL(10,2) NULL,
    RFC VARCHAR(20) NULL,
    vMotiveExt VARCHAR(100) NULL,
    iPeriodsExt INT NULL,
    vCommentExt VARCHAR(255) NULL,
    nRetencion DECIMAL(10,2) NULL,
    nJoPay DECIMAL(10,2) NULL,
    iMaxDays INT NULL,
    vMaxDate TEXT NULL,
    nLiquidate DECIMAL(10,2) NULL,
    nLiqPrin DECIMAL(10,2) NULL,
    nLiqInt DECIMAL(10,2) NULL,
    nLiqMor DECIMAL(10,2) NULL,
    nLiqCha DECIMAL(10,2) NULL,
    nLiqPrinTran DECIMAL(10,2) NULL,
    nLiqIntTran DECIMAL(10,2) NULL,
    nLiqMorTran DECIMAL(10,2) NULL,
    nLiqChaTran DECIMAL(10,2) NULL,
    nLiqRetTran DECIMAL(10,2) NULL,
    vScoreBuro VARCHAR(100) NULL,
    vCollectStatus VARCHAR(100) NULL,
    nCAT DECIMAL(10,2) NULL,
    vOpTable VARCHAR(50) NULL
);

CREATE TABLE Saldos_Cartera (
    DBKEY int auto_increment primary key not null,
    Id_Solicitud INT,
    Id_Credito INT,
    IdPersona INT,
    Referencia VARCHAR(50) NULL,
    Afiliado VARCHAR(50) NULL,
    Nombre VARCHAR(100) NULL,
    Monto DECIMAL(10,2) NULL,
    Comision DECIMAL(10,2) NULL,
    Intereses_Totales DECIMAL(10,2) NULL,
    Monto_Total DECIMAL(10,2) NULL,
    Pagos INT NULL,
    Amort_Pagadas INT NULL,
    Capital_Pagado DECIMAL(10,2) NULL,
    Interes_Pagado DECIMAL(10,2) NULL,
    IVA_Int_Pagado DECIMAL(10,2) NULL,
    Cargo_PTardio_Pagado DECIMAL(10,2) NULL,
    Moratorio_Pagado DECIMAL(10,2) NULL,
    Pago_en_Exceso DECIMAL(10,2) NULL,
    Comision_Pagada DECIMAL(10,2) NULL,
    Total_Pagado DECIMAL(10,2) NULL,
    Ajustes_Capital DECIMAL(10,2) NULL,
    Saldo_Capital DECIMAL(10,2) NULL,
    Saldo_Interes DECIMAL(10,2) NULL,
    Saldo_IVA_Int DECIMAL(10,2) NULL,
    Saldo_Cargo_PTardio DECIMAL(10,2) NULL,
    Saldo_Moratorios DECIMAL(10,2) NULL,
    Saldo_Pago_Exceso DECIMAL(10,2) NULL,
    Saldo_Comision DECIMAL(10,2) NULL,
    Saldo_Total DECIMAL(10,2) NULL,
    Importe_de_Pago DECIMAL(10,2) NULL,
    Id_Convenio DECIMAL(10,2) NULL,
    Dependencia VARCHAR(100) NULL,
    Primer_Pago_Teorico DATE NULL,
    Ultimo_Pago DATE NULL,
    Tipo_Financiamiento VARCHAR(50) NULL,
    Capital_Vigente DECIMAL(10,2) NULL,
    Capital_Vencido DECIMAL(10,2) NULL,
    Intereses_Vencidos DECIMAL(10,2) NULL,
    Vencido DECIMAL(10,2) NULL,
    Sdo_Insoluto DECIMAL(10,2) NULL,
    Sdo_Total_c_ListasCobro DECIMAL(10,2) NULL,
    Sdo_Vencido_c_ListCobro DECIMAL(10,2) NULL,
    Estatus_Cartera VARCHAR(50) NULL,
    Estatus VARCHAR(20) NULL,
    Sucursal VARCHAR(100) NULL,
    Fecha_Desembolso DATE NULL,
    Frecuencia VARCHAR(20) NULL,
    Primer_Pago_Real DATE NULL,
    Ultimo_Pago_c_ListaCobro DATE NULL,
    Ultimo_Pago_Aplicado DATE NULL,
    Dias_Ultimo_Pago INT NULL,
    Dias_Atraso INT NULL,
    Cuotas_Atraso INT NULL,
    Periodos_Atraso INT NULL,
    Pago DECIMAL(10,2) NULL,
    Monto_Ultimo_Pago DECIMAL(10,2) NULL,
    Tasa_Int_Anual DECIMAL(10,2) NULL,
    Gestor VARCHAR(100) NULL,
    Motivo VARCHAR(100) NULL,
    Banco VARCHAR(100) NULL,
    Estado VARCHAR(100) NULL,
    Ciudad VARCHAR(100) NULL,
    Com_Vigente DECIMAL(10,2) NULL,
    Com_Vencida DECIMAL(10,2) NULL,
    Clabe VARCHAR(50) NULL,
    Sig_Pago DATE NULL,
    Monto_Sig_Pago DECIMAL(10,2) NULL,
    vFondeador VARCHAR(100) NULL,
    Valida_Domi VARCHAR(100) NULL,
    vAfiliateIdO VARCHAR(100) NULL,
    vAfiliateO VARCHAR(100) NULL,
    Saldo_Retencion_Adm DECIMAL(10,2) NULL,
    RFC VARCHAR(20) NULL,
    vMotiveExt VARCHAR(100) NULL,
    iPeriodsExt INT NULL,
    vCommentExt VARCHAR(255) NULL,
    nRetencion DECIMAL(10,2) NULL,
    nJoPay DECIMAL(10,2) NULL,
    iMaxDays INT NULL,
    vMaxDate DATE NULL,
    nLiquidate DECIMAL(10,2) NULL,
    nLiqPrin DECIMAL(10,2) NULL,
    nLiqInt DECIMAL(10,2) NULL,
    nLiqMor DECIMAL(10,2) NULL,
    nLiqCha DECIMAL(10,2) NULL,
    nLiqPrinTran DECIMAL(10,2) NULL,
    nLiqIntTran DECIMAL(10,2) NULL,
    nLiqMorTran DECIMAL(10,2) NULL,
    nLiqChaTran DECIMAL(10,2) NULL,
    nLiqRetTran DECIMAL(10,2) NULL,
    vScoreBuro VARCHAR(100) NULL,
    vCollectStatus VARCHAR(100) NULL,
    nCAT DECIMAL(10,2) NULL,
    vOpTable VARCHAR(50) NULL,
    FechaGenerado DATETIME
);

CREATE TABLE Stage_Saldos_Contables (
    Id_Credito INT not null,
    Referencia VARCHAR(50) not null,
    Nombre VARCHAR(100) NULL,
    Id_Sucursal INT NULL,
    Sucursal VARCHAR(100) NULL,
    Id_Convenio DECIMAL(10,2) NULL,
    Convenio VARCHAR(100) NULL,
    Financiamiento VARCHAR(50) NULL,
    Estatus_Inicial VARCHAR(10) NULL,
    Estatus_Final VARCHAR(10) NULL,
    Fecha_Apertura TEXT NULL,
    Fecha_Terminacion TEXT NULL,
    Importe DECIMAL(10,2) NULL,
    Dias_Atraso INT NULL,
    Cuotas_Atrasadas INT NULL,
    Periodos_Atraso INT NULL,
    Pagos_Sostenidos INT NULL,
    Pago DECIMAL(10,2) NULL,
    Frecuencia VARCHAR(20) NULL,
    Fecha_Ultimo_Pago TEXT NULL,
    Importe_Ultimo_Pago DECIMAL(10,2) NULL,
    Saldo_Inicial_Capital DECIMAL(10,2) NULL,
    Otorgado DECIMAL(10,2) NULL,
    Pagos DECIMAL(10,2) NULL,
    Ajuste_Cargo_Capital DECIMAL(10,2) NULL,
    Ajuste_Abono_Capital DECIMAL(10,2) NULL,
    Saldo_Final_Capital DECIMAL(10,2) NULL,
    Calculo DECIMAL(10,2) NULL,
    Diferencia DECIMAL(10,2) NULL,
    Capital_Vigente DECIMAL(10,2) NULL,
    Capital_Vencido DECIMAL(10,2) NULL,
    Saldo_Inicial_Interes DECIMAL(10,2) NULL,
    Devengamiento DECIMAL(10,2) NULL,
    Pagos_Interes DECIMAL(10,2) NULL,
    Ajuste_Cargo_Interes DECIMAL(10,2) NULL,
    Ajuste_Abono_Interes DECIMAL(10,2) NULL,
    Interes_No_Devengado DECIMAL(10,2) NULL,
    Saldo_Final_Interes DECIMAL(10,2) NULL,
    Calculo_Interes DECIMAL(10,2) NULL,
    Diferencia_Interes DECIMAL(10,2) NULL,
    Interes_Devengado DECIMAL(10,2) NULL,
    IVA_Interes_Devengado DECIMAL(10,2) NULL,
    Interes_No_DevengadoB DECIMAL(10,2) NULL,
    Fecha_Cartera_Vencida TEXT NULL,
    Saldo_Contable DECIMAL(10,2) NULL,
    Saldo_Insoluto DECIMAL(10,2) NULL,
    Porc_Provision DECIMAL(10,2) NULL,
    Reserva DECIMAL(10,2) NULL,
    nCAT DECIMAL(10,2) NULL,
    vOpTable VARCHAR(50) NULL,
    Status VARCHAR(20) NULL
);

CREATE TABLE Saldos_Contables (
    DBKEY int auto_increment primary key not null,
    Id_Credito INT not null,
    Referencia VARCHAR(50) not null,
    Nombre VARCHAR(100) NULL,
    Id_Sucursal INT NULL,
    Sucursal VARCHAR(100) NULL,
    Id_Convenio DECIMAL(10,2) NULL,
    Convenio VARCHAR(100) NULL,
    Financiamiento VARCHAR(50) NULL,
    Estatus_Inicial VARCHAR(10) NULL,
    Estatus_Final VARCHAR(10) NULL,
    Fecha_Apertura DATE NULL,
    Fecha_Terminacion DATE NULL,
    Importe DECIMAL(10,2) NULL,
    Dias_Atraso INT NULL,
    Cuotas_Atrasadas INT NULL,
    Periodos_Atraso INT NULL,
    Pagos_Sostenidos INT NULL,
    Pago DECIMAL(10,2) NULL,
    Frecuencia VARCHAR(20) NULL,
    Fecha_Ultimo_Pago DATE NULL,
    Importe_Ultimo_Pago DECIMAL(10,2) NULL,
    Saldo_Inicial_Capital DECIMAL(10,2) NULL,
    Otorgado DECIMAL(10,2) NULL,
    Pagos DECIMAL(10,2) NULL,
    Ajuste_Cargo_Capital DECIMAL(10,2) NULL,
    Ajuste_Abono_Capital DECIMAL(10,2) NULL,
    Saldo_Final_Capital DECIMAL(10,2) NULL,
    Calculo DECIMAL(10,2) NULL,
    Diferencia DECIMAL(10,2) NULL,
    Capital_Vigente DECIMAL(10,2) NULL,
    Capital_Vencido DECIMAL(10,2) NULL,
    Saldo_Inicial_Interes DECIMAL(10,2) NULL,
    Devengamiento DECIMAL(10,2) NULL,
    Pagos_Interes DECIMAL(10,2) NULL,
    Ajuste_Cargo_Interes DECIMAL(10,2) NULL,
    Ajuste_Abono_Interes DECIMAL(10,2) NULL,
    Interes_No_Devengado DECIMAL(10,2) NULL,
    Saldo_Final_Interes DECIMAL(10,2) NULL,
    Calculo_Interes DECIMAL(10,2) NULL,
    Diferencia_Interes DECIMAL(10,2) NULL,
    Interes_Devengado DECIMAL(10,2) NULL,
    IVA_Interes_Devengado DECIMAL(10,2) NULL,
    Interes_No_DevengadoB DECIMAL(10,2) NULL,
    Fecha_Cartera_Vencida DATE NULL,
    Saldo_Contable DECIMAL(10,2) NULL,
    Saldo_Insoluto DECIMAL(10,2) NULL,
    Porc_Provision DECIMAL(10,2) NULL,
    Reserva DECIMAL(10,2) NULL,
    nCAT DECIMAL(10,2) NULL,
    vOpTable VARCHAR(50) NULL,
    Status VARCHAR(20) NULL,
    FechaGenerado DATETIME not null
);

CREATE TABLE Stage_Aplicacion_Pagos (
    Id_Credito INT NULL,
    Id_Convenio INT NULL,
    Convenio VARCHAR(255) NULL,
    Referencia VARCHAR(255) NULL,
    Id_Pago INT NOT NULL,
    Nombre_Cliente VARCHAR(255) NULL,
    Financiamiento VARCHAR(255) NULL,
    Origen_de_Movimiento VARCHAR(255) NULL,
    Fecha_Pago TEXT NULL,
    Fecha_Aplicacion TEXT NULL,
    Fecha_Deposito TEXT NULL,
    Status VARCHAR(50) NULL,
    Pago DECIMAL(10, 2) NULL,
    Capital DECIMAL(10, 2) NULL,
    Interes DECIMAL(10, 2) NULL,
    IVA_Int DECIMAL(10, 2) NULL,
    Comision_Financiada DECIMAL(10, 2) NULL,
    IVA_Comision_Financ DECIMAL(10, 2) NULL,
    Moratorios DECIMAL(10, 2) NULL,
    IVA_Mora DECIMAL(10, 2) NULL,
    Pago_Tardio DECIMAL(10, 2) NULL,
    IVA_PagoTardio DECIMAL(10, 2) NULL,
    Recuperacion DECIMAL(10, 2) NULL,
    IVA_Recup DECIMAL(10, 2) NULL,
    Com_Liquidacion DECIMAL(10, 2) NULL,
    IVA_Com_Liquidacion DECIMAL(10, 2) NULL,
    Retencion_X_Admon DECIMAL(10, 2) NULL,
    IVA_Retencion_X_Admon DECIMAL(10, 2) NULL,
    Pago_Exceso DECIMAL(10, 2) NULL,
    Gestor VARCHAR(255) NULL,
    Forma_de_pago VARCHAR(255) NULL,
    vMotive VARCHAR(255) NULL,
    PRIMARY KEY (Id_Pago)
);

CREATE TABLE Aplicacion_Pagos (
    Id_Credito INT NULL,
    Id_Convenio INT NULL,
    Convenio VARCHAR(255) NULL,
    Referencia VARCHAR(255) NULL,
    Id_Pago INT NOT NULL,
    Nombre_Cliente VARCHAR(255) NULL,
    Financiamiento VARCHAR(255) NULL,
    Origen_de_Movimiento VARCHAR(255) NULL,
    Fecha_Pago DATE NULL,
    Fecha_Aplicacion DATE NULL,
    Fecha_Deposito DATE NULL,
    Status VARCHAR(50) NULL,
    Pago DECIMAL(10, 2) NULL,
    Capital DECIMAL(10, 2) NULL,
    Interes DECIMAL(10, 2) NULL,
    IVA_Int DECIMAL(10, 2) NULL,
    Comision_Financiada DECIMAL(10, 2) NULL,
    IVA_Comision_Financ DECIMAL(10, 2) NULL,
    Moratorios DECIMAL(10, 2) NULL,
    IVA_Mora DECIMAL(10, 2) NULL,
    Pago_Tardio DECIMAL(10, 2) NULL,
    IVA_PagoTardio DECIMAL(10, 2) NULL,
    Recuperacion DECIMAL(10, 2) NULL,
    IVA_Recup DECIMAL(10, 2) NULL,
    Com_Liquidacion DECIMAL(10, 2) NULL,
    IVA_Com_Liquidacion DECIMAL(10, 2) NULL,
    Retencion_X_Admon DECIMAL(10, 2) NULL,
    IVA_Retencion_X_Admon DECIMAL(10, 2) NULL,
    Pago_Exceso DECIMAL(10, 2) NULL,
    Gestor VARCHAR(255) NULL,
    Forma_de_pago VARCHAR(255) NULL,
    vMotive VARCHAR(255) NULL,
    PRIMARY KEY (Id_Pago)
);

CREATE TABLE Stage_Otorgamiento_Creditos (
    Id_Credito INT PRIMARY KEY,
    Referencia VARCHAR(50) NULL,
    Nombre VARCHAR(100) NULL,
    Fecha_Apertura TEXT NULL,
    F_Cobro TEXT NULL,
    Id_Convenio INT NULL,
    Convenio VARCHAR(100) NULL,
    Id_Sucursal INT NULL,
    Sucursal VARCHAR(100) NULL,
    Capital DECIMAL(15, 2) NULL,
    Primer_Pago TEXT NULL,
    Comision DECIMAL(15, 2) NULL,
    IVA DECIMAL(15, 2) NULL,
    Cobertura DECIMAL(15, 2) NULL,
    IVA_Cobertura DECIMAL(15, 2) NULL,
    Disposicion DECIMAL(15, 2) NULL,
    Monto_Retenido DECIMAL(15, 2) NULL,
    Pago_de_Deuda DECIMAL(15, 2) NULL,
    Comision_Financiada DECIMAL(15, 2) NULL,
    IVA_Comision_Financiada DECIMAL(15, 2) NULL,
    Solicitud INT NULL,
    Vendedor INT NULL,
    Nombre_Vendedor VARCHAR(100) NULL,
    TipoVendedor VARCHAR(50) NULL,
    vSupervisorId INT NULL,
    vSupName VARCHAR(100) NULL,
    Producto VARCHAR(100) NULL,
    Descripcion_Tasa VARCHAR(100) NULL,
    Persona DECIMAL(15, 2) NULL,
    Plazo INT NULL,
    Id_Producto INT NULL,
    vCampaign VARCHAR(100) NULL,
    Tipo_de_Financiamiento VARCHAR(100) NULL,
    vFinancingTypeId INT NULL,
    vAliado VARCHAR(100) NULL
);

CREATE TABLE Otorgamiento_Creditos (
    Id_Credito INT PRIMARY KEY,
    Referencia VARCHAR(50) NULL,
    Nombre VARCHAR(100) NULL,
    Fecha_Apertura DATE NULL,
    F_Cobro DATE NULL,
    Id_Convenio INT NULL,
    Convenio VARCHAR(100) NULL,
    Id_Sucursal INT NULL,
    Sucursal VARCHAR(100) NULL,
    Capital DECIMAL(15, 2) NULL,
    Primer_Pago DATE NULL,
    Comision DECIMAL(15, 2) NULL,
    IVA DECIMAL(15, 2) NULL,
    Cobertura DECIMAL(15, 2) NULL,
    IVA_Cobertura DECIMAL(15, 2) NULL,
    Disposicion DECIMAL(15, 2) NULL,
    Monto_Retenido DECIMAL(15, 2) NULL,
    Pago_de_Deuda DECIMAL(15, 2) NULL,
    Comision_Financiada DECIMAL(15, 2) NULL,
    IVA_Comision_Financiada DECIMAL(15, 2) NULL,
    Solicitud INT NULL,
    Vendedor INT NULL,
    Nombre_Vendedor VARCHAR(100) NULL,
    TipoVendedor VARCHAR(50) NULL,
    vSupervisorId INT NULL,
    vSupName VARCHAR(100) NULL,
    Producto VARCHAR(100) NULL,
    Descripcion_Tasa VARCHAR(100) NULL,
    Persona DECIMAL(15, 2) NULL,
    Plazo INT NULL,
    Id_Producto INT NULL,
    vCampaign VARCHAR(100) NULL,
    Tipo_de_Financiamiento VARCHAR(100) NULL,
    vFinancingTypeId INT NULL,
    vAliado VARCHAR(100) NULL
);

CREATE TABLE Stage_Gestiones (
    AgenciaRegistro VARCHAR(255),
    CausaNoPago VARCHAR(255),
    CausaNoDomiciliacion VARCHAR(255),
    CodigoAccion INT,
    CodigoResultado INT,
    Comentarios TEXT,
    ContactoGenerado VARCHAR(255),
    Coordenadas VARCHAR(255),
    Credito INT,
    EstatusPromesa VARCHAR(50),
    FechaActividad TEXT,
    FechaPromesa TEXT,
    MontoPromesa DECIMAL(10, 2),
    Origen VARCHAR(50),
    Producto VARCHAR(50),
    Resultado VARCHAR(50),
    Telefono VARCHAR(20),
    TipoPago VARCHAR(50),
    UsuarioRegistro VARCHAR(50)
);

CREATE TABLE Gestiones (
    AgenciaRegistro VARCHAR(255),
    CausaNoPago VARCHAR(255),
    CausaNoDomiciliacion VARCHAR(255),
    CodigoAccion INT,
    CodigoResultado INT,
    Comentarios TEXT,
    ContactoGenerado VARCHAR(255),
    Coordenadas VARCHAR(255),
    Credito INT,
    EstatusPromesa VARCHAR(50),
    FechaActividad DATETIME,
    FechaPromesa DATE,
    MontoPromesa DECIMAL(10, 2),
    Origen VARCHAR(50),
    Producto VARCHAR(50),
    Resultado VARCHAR(50),
    Telefono VARCHAR(20),
    TipoPago VARCHAR(50),
    UsuarioRegistro VARCHAR(50)
);

CREATE TABLE Stage_Quebrantos (
    Operacion INT,
    Referencia INT,
    Nombre VARCHAR(255),
    Convenio VARCHAR(255),
    vFinancingtypeid VARCHAR(50),
    KVigente DECIMAL(10, 2),
    KVencido DECIMAL(10, 2),
    IntVencido DECIMAL(10, 2),
    IVAIntVencido DECIMAL(10, 2),
    IntVencidoCO DECIMAL(10, 2),
    IVAIntVencidoCO DECIMAL(10, 2),
    TotalQuebranto DECIMAL(10, 2),
    PagosRealizados DECIMAL(10, 2),
    SdoPendiente DECIMAL(10, 2),
    IntXDevengar DECIMAL(10, 2),
    SdoTotalXPagar DECIMAL(10, 2),
    FechaQuebranto VARCHAR(10),
    UltPagoTeorico VARCHAR(10),
    UltimoPago VARCHAR(10),
    UltPagoApl VARCHAR(10),
    Gestor VARCHAR(255),
    nCommission DECIMAL(10, 2),
    nCommTax DECIMAL(10, 2),
    vMotive VARCHAR(255)
);

CREATE TABLE Quebrantos (
    dbkey INT AUTO_INCREMENT PRIMARY KEY,
    Operacion INT,
    Referencia INT,
    Nombre VARCHAR(255),
    Convenio VARCHAR(255),
    vFinancingtypeid VARCHAR(50),
    KVigente DECIMAL(10, 2),
    KVencido DECIMAL(10, 2),
    IntVencido DECIMAL(10, 2),
    IVAIntVencido DECIMAL(10, 2),
    IntVencidoCO DECIMAL(10, 2),
    IVAIntVencidoCO DECIMAL(10, 2),
    TotalQuebranto DECIMAL(10, 2),
    PagosRealizados DECIMAL(10, 2),
    SdoPendiente DECIMAL(10, 2),
    IntXDevengar DECIMAL(10, 2),
    SdoTotalXPagar DECIMAL(10, 2),
    FechaQuebranto VARCHAR(10),
    UltPagoTeorico VARCHAR(10),
    UltimoPago VARCHAR(10),
    UltPagoApl VARCHAR(10),
    Gestor VARCHAR(255),
    nCommission DECIMAL(10, 2),
    nCommTax DECIMAL(10, 2),
    vMotive VARCHAR(255),
    Estrategias DECIMAL(10,2),
    SaldoReal DECIMAL(10, 2),
    CapitalQuebrantado DECIMAL(10, 2),
    Recuperacion DECIMAL(10, 2),
    Mes TINYINT,
    Año YEAR,
    QuebrantoContable DECIMAL(10,2),
    Producto VARCHAR(255),
    Financiamiento VARCHAR(255),
    Valid TINYINT,
    SaldoCapital DECIMAL(10,2),
    Motivo DECIMAL(10,2),
    FechaGenerado DATETIME
);

CREATE TABLE ESTRATEGIA (
    Id_Pago INT NOT NULL
);

CREATE TABLE Stage_Juicios (
    Credito_MC INT,
    Decla VARCHAR(255),
    Descripcion_Cierre VARCHAR(255),
    Dias_Activo INT,
    Dias_Caducar INT,
    Estatus VARCHAR(50),
    Etapa_Procesal VARCHAR(50),
    Expediente INT,
    Fecha_Actualizacion TEXT,
    Fecha_Carga_Inicial TEXT,
    Fecha_Cierre TEXT,
    Fecha_Ultima_Act TEXT,
    Id_Juicio INT PRIMARY KEY,
    Juzgado VARCHAR(255),
    Motivo_Cierre VARCHAR(255),
    Producto_MC VARCHAR(50),
    Tipo_Juicio VARCHAR(50),
    Validar_Cierre VARCHAR(50)
);

CREATE TABLE Juicios (
    Credito_MC INT,
    Decla VARCHAR(255),
    Descripcion_Cierre VARCHAR(255),
    Dias_Activo INT,
    Dias_Caducar INT,
    Estatus VARCHAR(50),
    Etapa_Procesal VARCHAR(50),
    Expediente INT,
    Fecha_Actualizacion DATETIME,
    Fecha_Carga_Inicial DATETIME,
    Fecha_Cierre DATETIME,
    Fecha_Ultima_Act DATETIME,
    Id_Juicio INT PRIMARY KEY,
    Juzgado VARCHAR(255),
    Motivo_Cierre VARCHAR(255),
    Producto_MC VARCHAR(50),
    Tipo_Juicio VARCHAR(50),
    Validar_Cierre VARCHAR(50)
);

CREATE TABLE Stage_Sistema (
    Agencia_Asignada_MC VARCHAR(255),
    Agencia_MC INT,
    Bandera_PP_Juicio INT,
    Codigo_MC INT,
    Credito_MC INT,
    Cuenta_Al_Corriente VARCHAR(3),
    Dias_en_la_instancia_actual INT,
    Dias_Para_Siguiente_Pago INT,
    Estatus_MC VARCHAR(50),
    Estrategia VARCHAR(255),
    Excepciones_MC VARCHAR(3),
    Fecha_de_Asignacion_CallCenter TEXT,
    Fecha_de_Asignacion_Visita TEXT,
    Fecha_De_Captura_de_Juicio TEXT,
    Fecha_de_Ultima_Visita TEXT,
    Fecha_Promesa_MC TEXT,
    Fecha_Ult_Gestion_MC TEXT,
    Importe_Pago_X2 DECIMAL(10,2),
    Importe_Pago_X3 DECIMAL(10,2),
    Importe_Pago_X4 DECIMAL(10,2),
    Importe_Pago_X6 DECIMAL(10,2),
    Monto_Promesa_MC DECIMAL(10,2),
    No_Gestiones INT,
    No_Visitas INT,
    Nombre_Agencia_MC VARCHAR(255),
    Nombre_Del_Deudor_MC VARCHAR(255),
    Nombre_Instancia_MC VARCHAR(255),
    Producto_MC VARCHAR(50),
    Quita_Exclusiva VARCHAR(3),
    Resultado_MC VARCHAR(255),
    Resultado_Visita_MC VARCHAR(255),
    Saldo_Menor DECIMAL(10,2),
    Semaforo_Gestion VARCHAR(50),
    Ult_Causa_No_Domiciliacion VARCHAR(255),
    Ult_Causa_No_Pago VARCHAR(255),
    Usuario_Asignado VARCHAR(255),
    Usuario_Asignado_Extrajudicial VARCHAR(255)
);

CREATE TABLE Sistema (
    Agencia_Asignada_MC VARCHAR(255),
    Agencia_MC INT,
    Bandera_PP_Juicio INT,
    Codigo_MC INT,
    Credito_MC INT,
    Cuenta_Al_Corriente VARCHAR(3),
    Dias_en_la_instancia_actual INT,
    Dias_Para_Siguiente_Pago INT,
    Estatus_MC VARCHAR(50),
    Estrategia VARCHAR(255),
    Excepciones_MC VARCHAR(3),
    Fecha_de_Asignacion_CallCenter DATETIME,
    Fecha_de_Asignacion_Visita DATETIME,
    Fecha_De_Captura_de_Juicio DATETIME,
    Fecha_de_Ultima_Visita DATETIME,
    Fecha_Promesa_MC DATETIME,
    Fecha_Ult_Gestion_MC DATETIME,
    Importe_Pago_X2 DECIMAL(10,2),
    Importe_Pago_X3 DECIMAL(10,2),
    Importe_Pago_X4 DECIMAL(10,2),
    Importe_Pago_X6 DECIMAL(10,2),
    Monto_Promesa_MC DECIMAL(10,2),
    No_Gestiones INT,
    No_Visitas INT,
    Nombre_Agencia_MC VARCHAR(255),
    Nombre_Del_Deudor_MC VARCHAR(255),
    Nombre_Instancia_MC VARCHAR(255),
    Producto_MC VARCHAR(50),
    Quita_Exclusiva VARCHAR(3),
    Resultado_MC VARCHAR(255),
    Resultado_Visita_MC VARCHAR(255),
    Saldo_Menor DECIMAL(10,2),
    Semaforo_Gestion VARCHAR(50),
    Ult_Causa_No_Domiciliacion VARCHAR(255),
    Ult_Causa_No_Pago VARCHAR(255),
    Usuario_Asignado VARCHAR(255),
    Usuario_Asignado_Extrajudicial VARCHAR(255)
);
-- --------------------------------Catalogos-----------------------------------------------------------------------
CREATE TABLE  Dependencia(
    Dependencia VARCHAR(100) NULL,
    Abreviatura VARCHAR(100) NULL
);

CREATE TABLE  Financiamiento(
    Tipo_Financiamiento VARCHAR(50) NULL,
    Financiamiento VARCHAR(50) NULL,
    Producto VARCHAR(50) NULL
);

CREATE TABLE Motivo(
    Motivo VARCHAR(100) NULL,
    BanderaPago VARCHAR(50) NULL,
    BanderaDependendia VARCHAR(50) NULL,
    Abreviatura VARCHAR(50) NULL
);

CREATE TABLE CatalogoBancos(
	Clave VARCHAR(100) NULL,
	NombreCorto VARCHAR(100) NULL,
	RazonSocial VARCHAR(100) NULL
);

CREATE TABLE CatalogoGestoresArea(
	UsuarioMC VARCHAR(50) NULL,
    Area VARCHAR(50) NULL,
    Estatus Int NULL,
    Perfil VARCHAR(50) NULL
);

CREATE TABLE CatalogosResultadoAvance(
	Clave VARCHAR(50) NULL,
    Resultado VARCHAR(50) NULL
);

CREATE TABLE Demograficos(
);

CREATE TABLE PagosEstrategiaAcumulados(
	IdPago INT NULL
);

CREATE TABLE EstatusDomi(
	IdCredito INT NULL,
    Rechazo VARCHAR(50) NULL,
    MES DATE NULL
);

-- --------------------------------Procesos Almacenados------------------------------------------------------------
DELIMITER //

CREATE PROCEDURE InsertSaldosCarteras()
BEGIN
    DECLARE current_period TIME;

    -- Declare exit handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- Error
        SELECT 0 AS Status;
        ROLLBACK;
    END;

    -- Define current time period
    SET current_period = CASE
        WHEN TIME(NOW()) BETWEEN '00:00:00' AND '07:00:00' THEN '07:00:00'
        WHEN TIME(NOW()) BETWEEN '07:01:00' AND '18:00:00' THEN '18:00:00'
        WHEN TIME(NOW()) BETWEEN '18:01:00' AND '23:59:59' THEN '23:59:59'
    END;

    -- Check if a record with today's date and the current period already exists
    IF EXISTS (
        SELECT 1
        FROM Saldos_Cartera
        WHERE DATE(FechaGenerado) = CURDATE()
        AND TIME(FechaGenerado) = current_period
    ) THEN
        -- Validation failed, return 2
        SELECT 2 AS Status;
    ELSE
        START TRANSACTION;

    -- Insert data from Stage_Saldos_Cartera to Saldos_Cartera if no matching record exists
    INSERT INTO Saldos_Cartera (
        Id_Solicitud, Id_Credito, IdPersona, Referencia, Afiliado, Nombre, Monto, Comision, Intereses_Totales, Monto_Total, 
        Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, 
        Comision_Pagada, Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
        Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, Id_Convenio, Dependencia, 
        Primer_Pago_Teorico, Ultimo_Pago, Tipo_Financiamiento, Capital_Vigente, Capital_Vencido, Intereses_Vencidos, Vencido, 
        Sdo_Insoluto, Sdo_Total_c_ListasCobro, Sdo_Vencido_c_ListCobro, Estatus_Cartera, Estatus, Sucursal, Fecha_Desembolso, 
        Frecuencia, Primer_Pago_Real, Ultimo_Pago_c_ListaCobro, Ultimo_Pago_Aplicado, Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, 
        Periodos_Atraso, Pago, Monto_Ultimo_Pago, Tasa_Int_Anual, Gestor, Motivo, Banco, Estado, Ciudad, Com_Vigente, Com_Vencida, 
        Clabe, Sig_Pago, Monto_Sig_Pago, vFondeador, Valida_Domi, vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm, RFC, vMotiveExt, 
        iPeriodsExt, vCommentExt, nRetencion, nJoPay, iMaxDays, vMaxDate, nLiquidate, nLiqPrin, nLiqInt, nLiqMor, nLiqCha, 
        nLiqPrinTran, nLiqIntTran, nLiqMorTran, nLiqChaTran, nLiqRetTran, vScoreBuro, vCollectStatus, nCAT, vOpTable, FechaGenerado
    )
    SELECT 
        Id_Solicitud, Id_Credito, IdPersona, Referencia, Afiliado, Nombre, Monto, Comision, Intereses_Totales, Monto_Total, 
        Pagos, Amort_Pagadas, Capital_Pagado, Interes_Pagado, IVA_Int_Pagado, Cargo_PTardio_Pagado, Moratorio_Pagado, Pago_en_Exceso, 
        Comision_Pagada, Total_Pagado, Ajustes_Capital, Saldo_Capital, Saldo_Interes, Saldo_IVA_Int, Saldo_Cargo_PTardio, 
        Saldo_Moratorios, Saldo_Pago_Exceso, Saldo_Comision, Saldo_Total, Importe_de_Pago, Id_Convenio, Dependencia, 
        CASE 
            WHEN Primer_Pago_Teorico = '0000-00-00' OR Primer_Pago_Teorico = '' OR Primer_Pago_Teorico = '0.00' THEN NULL 
            ELSE STR_TO_DATE(Primer_Pago_Teorico, '%Y-%m-%d') 
        END AS Primer_Pago_Teorico, 
        CASE 
            WHEN Ultimo_Pago = '0000-00-00' OR Ultimo_Pago = '' OR Ultimo_Pago = '0.00' THEN NULL 
            ELSE STR_TO_DATE(Ultimo_Pago, '%Y-%m-%d') 
        END AS Ultimo_Pago, 
        Tipo_Financiamiento, Capital_Vigente, Capital_Vencido, Intereses_Vencidos, Vencido, Sdo_Insoluto, 
        Sdo_Total_c_ListasCobro, Sdo_Vencido_c_ListCobro, Estatus_Cartera, Estatus, Sucursal, 
        CASE 
            WHEN Fecha_Desembolso = '0000-00-00' OR Fecha_Desembolso = '' OR Fecha_Desembolso = '0.00' THEN NULL 
            ELSE STR_TO_DATE(Fecha_Desembolso, '%Y-%m-%d') 
        END AS Fecha_Desembolso, 
        Frecuencia, 
        CASE 
            WHEN Primer_Pago_Real = '0000-00-00' OR Primer_Pago_Real = '' OR Primer_Pago_Real = '0.00' THEN NULL 
            ELSE STR_TO_DATE(Primer_Pago_Real, '%Y-%m-%d') 
        END AS Primer_Pago_Real, 
        CASE 
            WHEN Ultimo_Pago_c_ListaCobro = '0000-00-00' OR Ultimo_Pago_c_ListaCobro = '' OR Ultimo_Pago_c_ListaCobro = '0.00' THEN NULL 
            ELSE STR_TO_DATE(Ultimo_Pago_c_ListaCobro, '%Y-%m-%d') 
        END AS Ultimo_Pago_c_ListaCobro, 
        CASE 
            WHEN Ultimo_Pago_Aplicado = '0000-00-00' OR Ultimo_Pago_Aplicado = '' OR Ultimo_Pago_Aplicado = '0.00' THEN NULL 
            ELSE STR_TO_DATE(Ultimo_Pago_Aplicado, '%Y-%m-%d') 
        END AS Ultimo_Pago_Aplicado, 
        Dias_Ultimo_Pago, Dias_Atraso, Cuotas_Atraso, Periodos_Atraso, Pago, Monto_Ultimo_Pago, Tasa_Int_Anual, Gestor, Motivo, Banco, 
        Estado, Ciudad, Com_Vigente, Com_Vencida, Clabe, 
        CASE 
            WHEN Sig_Pago = '0000-00-00' OR Sig_Pago = '' OR Sig_Pago = '0.00' THEN NULL 
            ELSE STR_TO_DATE(Sig_Pago, '%Y-%m-%d') 
        END AS Sig_Pago, 
        Monto_Sig_Pago, vFondeador, Valida_Domi, vAfiliateIdO, vAfiliateO, Saldo_Retencion_Adm, RFC, vMotiveExt, iPeriodsExt, 
        vCommentExt, nRetencion, nJoPay, iMaxDays, 
        CASE 
            WHEN vMaxDate = '0000-00-00' OR vMaxDate = '' OR vMaxDate = '0.00' THEN NULL 
            ELSE STR_TO_DATE(vMaxDate, '%Y-%m-%d') 
        END AS vMaxDate, 
        nLiquidate, nLiqPrin, nLiqInt, nLiqMor, nLiqCha, nLiqPrinTran, nLiqIntTran, nLiqMorTran, nLiqChaTran, nLiqRetTran, 
        vScoreBuro, vCollectStatus, nCAT, vOpTable, CONCAT(CURDATE(), ' ', current_period) AS FechaGenerado
    FROM Stage_Saldos_Cartera;

        -- Success
        SELECT 1 AS Status;

        COMMIT;
    END IF;
END //

DELIMITER ;

DELIMITER //

CREATE PROCEDURE InsertSaldosContables()
BEGIN
    DECLARE current_period TIME;

    -- Declare exit handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- Error
        SELECT 0 AS Status;
        ROLLBACK;
    END;

    -- Define current time period
    SET current_period = CASE
        WHEN TIME(NOW()) BETWEEN '00:00:00' AND '07:00:00' THEN '07:00:00'
        WHEN TIME(NOW()) BETWEEN '07:01:00' AND '18:00:00' THEN '18:00:00'
        WHEN TIME(NOW()) BETWEEN '18:01:00' AND '23:59:59' THEN '23:59:59'
    END;

    -- Check if a record with today's date and the current period already exists
    IF EXISTS (
        SELECT 1
        FROM Saldos_Contables
        WHERE DATE(FechaGenerado) = CURDATE()
        AND TIME(FechaGenerado) = current_period
    ) THEN
        -- Validation failed, return 2
        SELECT 2 AS Status;
    ELSE
        START TRANSACTION;

    -- Insert data from Stage_Saldos_Contables to Saldos_Contables if no matching record exists
    INSERT INTO Saldos_Contables (
        Id_Credito, Referencia, Nombre, Id_Sucursal, Sucursal, Id_Convenio, Convenio, Financiamiento, Estatus_Inicial, 
        Estatus_Final, Fecha_Apertura, Fecha_Terminacion, Importe, Dias_Atraso, Cuotas_Atrasadas, Periodos_Atraso, 
        Pagos_Sostenidos, Pago, Frecuencia, Fecha_Ultimo_Pago, Importe_Ultimo_Pago, Saldo_Inicial_Capital, Otorgado, Pagos, 
        Ajuste_Cargo_Capital, Ajuste_Abono_Capital, Saldo_Final_Capital, Calculo, Diferencia, Capital_Vigente, Capital_Vencido, 
        Saldo_Inicial_Interes, Devengamiento, Pagos_Interes, Ajuste_Cargo_Interes, Ajuste_Abono_Interes, Interes_No_Devengado, 
        Saldo_Final_Interes, Calculo_Interes, Diferencia_Interes, Interes_Devengado, IVA_Interes_Devengado, Interes_No_DevengadoB, 
        Fecha_Cartera_Vencida, Saldo_Contable, Saldo_Insoluto, Porc_Provision, Reserva, nCAT, vOpTable, Status, FechaGenerado
    )
    SELECT 
        Id_Credito, Referencia, Nombre, Id_Sucursal, Sucursal, Id_Convenio, Convenio, Financiamiento, Estatus_Inicial, 
        Estatus_Final, 
        CASE 
            WHEN Fecha_Apertura = '0000-00-00' OR Fecha_Apertura = '' OR Fecha_Apertura = '0.00' THEN NULL 
            ELSE STR_TO_DATE(Fecha_Apertura, '%Y-%m-%d') 
        END AS Fecha_Apertura, 
        CASE 
            WHEN Fecha_Terminacion = '0000-00-00' OR Fecha_Terminacion = '' OR Fecha_Terminacion = '0.00' THEN NULL 
            ELSE STR_TO_DATE(Fecha_Terminacion, '%Y-%m-%d') 
        END AS Fecha_Terminacion, 
        Importe, Dias_Atraso, Cuotas_Atrasadas, Periodos_Atraso, Pagos_Sostenidos, Pago, Frecuencia, 
        CASE 
            WHEN Fecha_Ultimo_Pago = '0000-00-00' OR Fecha_Ultimo_Pago = '' OR Fecha_Ultimo_Pago = '0.00' THEN NULL 
            ELSE STR_TO_DATE(Fecha_Ultimo_Pago, '%Y-%m-%d') 
        END AS Fecha_Ultimo_Pago, 
        Importe_Ultimo_Pago, Saldo_Inicial_Capital, Otorgado, Pagos, Ajuste_Cargo_Capital, Ajuste_Abono_Capital, 
        Saldo_Final_Capital, Calculo, Diferencia, Capital_Vigente, Capital_Vencido, Saldo_Inicial_Interes, Devengamiento, 
        Pagos_Interes, Ajuste_Cargo_Interes, Ajuste_Abono_Interes, Interes_No_Devengado, Saldo_Final_Interes, Calculo_Interes, 
        Diferencia_Interes, Interes_Devengado, IVA_Interes_Devengado, Interes_No_DevengadoB, 
        CASE 
            WHEN Fecha_Cartera_Vencida = '0000-00-00' OR Fecha_Cartera_Vencida = '' OR Fecha_Cartera_Vencida = '0.00' THEN NULL 
            ELSE STR_TO_DATE(Fecha_Cartera_Vencida, '%Y-%m-%d') 
        END AS Fecha_Cartera_Vencida, 
        Saldo_Contable, Saldo_Insoluto, Porc_Provision, Reserva, nCAT, vOpTable, Status, 
        CONCAT(CURDATE(), ' ', current_period) AS FechaGenerado
    FROM Stage_Saldos_Contables;

        -- Success
        SELECT 1 AS Status;

        COMMIT;
    END IF;
END //

DELIMITER ;

DELIMITER //

CREATE PROCEDURE InsertAplicacionPagos()
BEGIN
    -- Declare exit handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- Error
        SELECT 0 AS Status;
        ROLLBACK;
    END;

    START TRANSACTION;

    -- Insert data from Stage_Aplicacion_Pagos to Aplicacion_Pagos if no matching record exists
    INSERT INTO Aplicacion_Pagos (
        Id_Credito, Id_Convenio, Convenio, Referencia, Id_Pago, Nombre_Cliente, Financiamiento, Origen_de_Movimiento, 
        Fecha_Pago, Fecha_Aplicacion, Fecha_Deposito, Status, Pago, Capital, Interes, IVA_Int, Comision_Financiada, 
        IVA_Comision_Financ, Moratorios, IVA_Mora, Pago_Tardio, IVA_PagoTardio, Recuperacion, IVA_Recup, Com_Liquidacion, 
        IVA_Com_Liquidacion, Retencion_X_Admon, IVA_Retencion_X_Admon, Pago_Exceso, Gestor, Forma_de_pago, vMotive
    )
    SELECT 
        Id_Credito, Id_Convenio, Convenio, Referencia, Id_Pago, Nombre_Cliente, Financiamiento, Origen_de_Movimiento, 
        CASE 
            WHEN Fecha_Pago = '' THEN NULL 
            WHEN Fecha_Pago LIKE '____-__-__' THEN Fecha_Pago
            ELSE STR_TO_DATE(Fecha_Pago, '%d/%m/%Y') 
        END AS Fecha_Pago, 
        CASE 
            WHEN Fecha_Aplicacion = '' THEN NULL 
            WHEN Fecha_Aplicacion LIKE '____-__-__' THEN Fecha_Aplicacion
            ELSE STR_TO_DATE(Fecha_Aplicacion, '%d/%m/%Y') 
        END AS Fecha_Aplicacion, 
        CASE 
            WHEN Fecha_Deposito = '' THEN NULL 
            WHEN Fecha_Deposito LIKE '____-__-__' THEN Fecha_Deposito
            ELSE STR_TO_DATE(Fecha_Deposito, '%d/%m/%Y') 
        END AS Fecha_Deposito, 
        Status, Pago, Capital, Interes, IVA_Int, Comision_Financiada, IVA_Comision_Financ, Moratorios, IVA_Mora, 
        Pago_Tardio, IVA_PagoTardio, Recuperacion, IVA_Recup, Com_Liquidacion, IVA_Com_Liquidacion, Retencion_X_Admon, 
        IVA_Retencion_X_Admon, Pago_Exceso, Gestor, Forma_de_pago, vMotive
    FROM Stage_Aplicacion_Pagos
    WHERE Id_Pago NOT IN (SELECT Id_Pago FROM Aplicacion_Pagos);

    -- Success
    SELECT 1 AS Status;

    COMMIT;
END //

DELIMITER ;

DELIMITER //

CREATE PROCEDURE InsertOtorgamientoCreditos()
BEGIN
    -- Declare exit handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- Error
        SELECT 0 AS Status;
        ROLLBACK;
    END;

    START TRANSACTION;

    -- Create temporary table
    CREATE TEMPORARY TABLE Temp_Otorgamiento_Creditos AS
    SELECT * FROM Stage_Otorgamiento_Creditos;

    -- Insert data from Temp_Otorgamiento_Creditos to Otorgamiento_Creditos
    INSERT INTO Otorgamiento_Creditos (
        Id_Credito, Referencia, Nombre, Fecha_Apertura, F_Cobro, Id_Convenio, Convenio, Id_Sucursal, Sucursal, 
        Capital, Primer_Pago, Comision, IVA, Cobertura, IVA_Cobertura, Disposicion, Monto_Retenido, Pago_de_Deuda, 
        Comision_Financiada, IVA_Comision_Financiada, Solicitud, Vendedor, Nombre_Vendedor, TipoVendedor, 
        vSupervisorId, vSupName, Producto, Descripcion_Tasa, Persona, Plazo, Id_Producto, vCampaign, 
        Tipo_de_Financiamiento, vFinancingTypeId, vAliado
    )
    SELECT 
        t.Id_Credito, t.Referencia, t.Nombre, 
        CASE 
            WHEN t.Fecha_Apertura = '' THEN NULL 
            WHEN t.Fecha_Apertura LIKE '____-__-__' THEN t.Fecha_Apertura
            ELSE STR_TO_DATE(t.Fecha_Apertura, '%d/%m/%Y') 
        END AS Fecha_Apertura, 
        CASE 
            WHEN t.F_Cobro = '' THEN NULL 
            WHEN t.F_Cobro LIKE '____-__-__' THEN t.F_Cobro
            ELSE STR_TO_DATE(t.F_Cobro, '%d/%m/%Y') 
        END AS F_Cobro, 
        t.Id_Convenio, t.Convenio, t.Id_Sucursal, t.Sucursal, 
        t.Capital, 
        CASE 
            WHEN t.Primer_Pago = '' THEN NULL 
            WHEN t.Primer_Pago LIKE '____-__-__' THEN t.Primer_Pago
            ELSE STR_TO_DATE(t.Primer_Pago, '%d/%m/%Y') 
        END AS Primer_Pago, 
        t.Comision, t.IVA, t.Cobertura, t.IVA_Cobertura, t.Disposicion, t.Monto_Retenido, t.Pago_de_Deuda, 
        t.Comision_Financiada, t.IVA_Comision_Financiada, t.Solicitud, t.Vendedor, t.Nombre_Vendedor, t.TipoVendedor, 
        t.vSupervisorId, t.vSupName, t.Producto, t.Descripcion_Tasa, t.Persona, t.Plazo, t.Id_Producto, t.vCampaign, 
        t.Tipo_de_Financiamiento, t.vFinancingTypeId, t.vAliado
    FROM Temp_Otorgamiento_Creditos t
    LEFT JOIN Otorgamiento_Creditos o
    ON t.Id_Credito = o.Id_Credito
    WHERE o.Id_Credito IS NULL;

    -- Drop temporary table
    DROP TEMPORARY TABLE IF EXISTS Temp_Otorgamiento_Creditos;

    -- Success
    SELECT 1 AS Status;

    COMMIT;
END //

DELIMITER ;

DELIMITER //

CREATE PROCEDURE InsertGestiones()
BEGIN
    -- Declare exit handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- Error
        SELECT 0 AS Status;
        ROLLBACK;
    END;

    START TRANSACTION;

    -- Insert new entries from Stage_Gestiones to Gestiones
    INSERT INTO Gestiones (
        AgenciaRegistro,
        CausaNoPago,
        CausaNoDomiciliacion,
        CodigoAccion,
        CodigoResultado,
        Comentarios,
        ContactoGenerado,
        Coordenadas,
        Credito,
        EstatusPromesa,
        FechaActividad,
        FechaPromesa,
        MontoPromesa,
        Origen,
        Producto,
        Resultado,
        Telefono,
        TipoPago,
        UsuarioRegistro
    )
    SELECT
        s.AgenciaRegistro,
        s.CausaNoPago,
        s.CausaNoDomiciliacion,
        s.CodigoAccion,
        s.CodigoResultado,
        s.Comentarios,
        s.ContactoGenerado,
        s.Coordenadas,
        s.Credito,
        s.EstatusPromesa,
        STR_TO_DATE(s.FechaActividad, '%Y-%m-%d %H:%i:%s') AS FechaActividad,
        CASE
            WHEN s.FechaPromesa IS NULL OR s.FechaPromesa = '' THEN NULL
            ELSE STR_TO_DATE(s.FechaPromesa, '%Y-%m-%d %H:%i:%s')
        END AS FechaPromesa,
        s.MontoPromesa,
        s.Origen,
        s.Producto,
        s.Resultado,
        s.Telefono,
        s.TipoPago,
        s.UsuarioRegistro
    FROM Stage_Gestiones s
    LEFT JOIN Gestiones g ON 
        s.AgenciaRegistro = g.AgenciaRegistro AND
        s.CausaNoPago = g.CausaNoPago AND
        s.CausaNoDomiciliacion = g.CausaNoDomiciliacion AND
        s.CodigoAccion = g.CodigoAccion AND
        s.CodigoResultado = g.CodigoResultado AND
        s.Comentarios = g.Comentarios AND
        s.ContactoGenerado = g.ContactoGenerado AND
        s.Coordenadas = g.Coordenadas AND
        s.Credito = g.Credito AND
        s.EstatusPromesa = g.EstatusPromesa AND
        STR_TO_DATE(s.FechaActividad, '%Y-%m-%d %H:%i:%s') = g.FechaActividad AND
        (CASE
            WHEN s.FechaPromesa IS NULL OR s.FechaPromesa = '' THEN NULL
            ELSE STR_TO_DATE(s.FechaPromesa, '%Y-%m-%d %H:%i:%s')
        END) = g.FechaPromesa AND
        s.MontoPromesa = g.MontoPromesa AND
        s.Origen = g.Origen AND
        s.Producto = g.Producto AND
        s.Resultado = g.Resultado AND
        s.Telefono = g.Telefono AND
        s.TipoPago = g.TipoPago AND
        s.UsuarioRegistro = g.UsuarioRegistro
    WHERE g.AgenciaRegistro IS NULL;

    -- Success
    SELECT 1 AS Status;

    COMMIT;
END //

DELIMITER ;

DELIMITER //

CREATE PROCEDURE InsertJuicios()
BEGIN
    -- Declare exit handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- Error
        SELECT 0 AS Status;
        ROLLBACK;
    END;

    START TRANSACTION;

    -- Truncate the final table
    TRUNCATE TABLE Juicios;

    -- Insert data from Stage_Juicios to Juicios
    INSERT INTO Juicios (
        Credito_MC, Decla, Descripcion_Cierre, Dias_Activo, Dias_Caducar, Estatus, Etapa_Procesal, Expediente, 
        Fecha_Actualizacion, Fecha_Carga_Inicial, Fecha_Cierre, Fecha_Ultima_Act, Id_Juicio, Juzgado, Motivo_Cierre, 
        Producto_MC, Tipo_Juicio, Validar_Cierre
    )
    SELECT 
        Credito_MC, 
        Decla, 
        Descripcion_Cierre, 
        Dias_Activo, 
        Dias_Caducar, 
        Estatus, 
        Etapa_Procesal, 
        Expediente, 
        CASE 
            WHEN Fecha_Actualizacion = '' THEN NULL 
            ELSE Fecha_Actualizacion 
        END, 
        CASE 
            WHEN Fecha_Carga_Inicial = '' THEN NULL 
            ELSE Fecha_Carga_Inicial 
        END, 
        CASE 
            WHEN Fecha_Cierre = '' THEN NULL 
            ELSE Fecha_Cierre 
        END, 
        CASE 
            WHEN Fecha_Ultima_Act = '' THEN NULL 
            ELSE Fecha_Ultima_Act 
        END, 
        Id_Juicio, 
        Juzgado, 
        Motivo_Cierre, 
        Producto_MC, 
        Tipo_Juicio, 
        Validar_Cierre
    FROM Stage_Juicios;

    -- Success
    SELECT 1 AS Status;

    COMMIT;
END //

DELIMITER ;

DELIMITER //

CREATE PROCEDURE InsertSistema()
BEGIN
    -- Declare exit handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- Error
        SELECT 0 AS Status;
        ROLLBACK;
    END;

    START TRANSACTION;

    -- Truncate the final table
    TRUNCATE TABLE Sistema;

    -- Insert data from Stage_Sistema to Sistema
    INSERT INTO Sistema (
        Agencia_Asignada_MC, Agencia_MC, Bandera_PP_Juicio, Codigo_MC, Credito_MC, Cuenta_Al_Corriente, 
        Dias_en_la_instancia_actual, Dias_Para_Siguiente_Pago, Estatus_MC, Estrategia, Excepciones_MC, 
        Fecha_de_Asignacion_CallCenter, Fecha_de_Asignacion_Visita, Fecha_De_Captura_de_Juicio, 
        Fecha_de_Ultima_Visita, Fecha_Promesa_MC, Fecha_Ult_Gestion_MC, Importe_Pago_X2, Importe_Pago_X3, 
        Importe_Pago_X4, Importe_Pago_X6, Monto_Promesa_MC, No_Gestiones, No_Visitas, Nombre_Agencia_MC, 
        Nombre_Del_Deudor_MC, Nombre_Instancia_MC, Producto_MC, Quita_Exclusiva, Resultado_MC, 
        Resultado_Visita_MC, Saldo_Menor, Semaforo_Gestion, Ult_Causa_No_Domiciliacion, Ult_Causa_No_Pago, 
        Usuario_Asignado, Usuario_Asignado_Extrajudicial
    )
    SELECT 
        Agencia_Asignada_MC, Agencia_MC, Bandera_PP_Juicio, Codigo_MC, Credito_MC, Cuenta_Al_Corriente, 
        Dias_en_la_instancia_actual, Dias_Para_Siguiente_Pago, Estatus_MC, Estrategia, Excepciones_MC, 
        CASE 
            WHEN Fecha_de_Asignacion_CallCenter = '' THEN NULL 
            ELSE Fecha_de_Asignacion_CallCenter 
        END, 
        CASE 
            WHEN Fecha_de_Asignacion_Visita = '' THEN NULL 
            ELSE Fecha_de_Asignacion_Visita 
        END, 
        CASE 
            WHEN Fecha_De_Captura_de_Juicio = '' THEN NULL 
            ELSE Fecha_De_Captura_de_Juicio 
        END, 
        CASE 
            WHEN Fecha_de_Ultima_Visita = '' THEN NULL 
            ELSE Fecha_de_Ultima_Visita 
        END, 
        CASE 
            WHEN Fecha_Promesa_MC = '' THEN NULL 
            ELSE Fecha_Promesa_MC 
        END, 
        CASE 
            WHEN Fecha_Ult_Gestion_MC = '' THEN NULL 
            ELSE Fecha_Ult_Gestion_MC 
        END, 
        Importe_Pago_X2, Importe_Pago_X3, Importe_Pago_X4, Importe_Pago_X6, Monto_Promesa_MC, No_Gestiones, 
        No_Visitas, Nombre_Agencia_MC, Nombre_Del_Deudor_MC, Nombre_Instancia_MC, Producto_MC, Quita_Exclusiva, 
        Resultado_MC, Resultado_Visita_MC, Saldo_Menor, Semaforo_Gestion, Ult_Causa_No_Domiciliacion, 
        Ult_Causa_No_Pago, Usuario_Asignado, Usuario_Asignado_Extrajudicial
    FROM Stage_Sistema;

    -- Success
    SELECT 1 AS Status;

    COMMIT;
END //

DELIMITER ;








