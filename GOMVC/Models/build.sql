-- *****************************
-- BASE DE DATOS: godatabase
-- *****************************

CREATE DATABASE godatabase
CHARACTER SET utf8mb4
COLLATE utf8mb4_general_ci;
USE godatabase;

-- *****************************
-- TABLA: Users
-- *****************************
CREATE TABLE Users (
    User_Key INT AUTO_INCREMENT PRIMARY KEY,
    NAME VARCHAR(255),
    USERNAME VARCHAR(255) NOT NULL,
    EMAIL VARCHAR(255),
    PASSWORD VARCHAR(255) NOT NULL
);

INSERT INTO Users (USERNAME, PASSWORD) VALUES ('UserA','123456'), ('Userb','123456');


-- *****************************
-- TABLAS DE SALDOS Y CARTERA
-- *****************************

-- Staging de Saldos de Cartera (carga previa)
CREATE TABLE D1_Stage_Saldos_Cartera (
  Id_Solicitud INT,
  Id_Credito INT,
  Id_Persona INT,
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

-- Tabla final de Saldos de Cartera
CREATE TABLE D1_Saldos_Cartera (
    D1_Key INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    Id_Solicitud INT,
    Id_Credito INT,
    Id_Persona INT,
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
    FechaGenerado DATETIME NOT NULL
);

-- *****************************
-- TABLA: D2_Saldos_Contables
-- *****************************
CREATE TABLE D2_Saldos_Contables (
	D2_Key INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    Id_Credito INT NOT NULL,
    Referencia VARCHAR(50) NOT NULL,
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
    vOpTable VARCHAR(50) NULL
);

-- *****************************
-- TABLA: D3_Aplicacion_Pagos
-- *****************************
CREATE TABLE D3_Aplicacion_Pagos (
	D3_Key INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
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
    Pago DECIMAL(10,2) NULL,
    Capital DECIMAL(10,2) NULL,
    Interes DECIMAL(10,2) NULL,
    IVA_Int DECIMAL(10,2) NULL,
    Comision_Financiada DECIMAL(10,2) NULL,
    IVA_Comision_Financ DECIMAL(10,2) NULL,
    Moratorios DECIMAL(10,2) NULL,
    IVA_Mora DECIMAL(10,2) NULL,
    Pago_Tardio DECIMAL(10,2) NULL,
    IVA_PagoTardio DECIMAL(10,2) NULL,
    Recuperacion DECIMAL(10,2) NULL,
    IVA_Recup DECIMAL(10,2) NULL,
    Com_Liquidacion DECIMAL(10,2) NULL,
    IVA_Com_Liquidacion DECIMAL(10,2) NULL,
    Retencion_X_Admon DECIMAL(10,2) NULL,
    IVA_Retencion_X_Admon DECIMAL(10,2) NULL,
    Pago_Exceso DECIMAL(10,2) NULL,
    Gestor VARCHAR(255) NULL,
    Forma_de_pago VARCHAR(255) NULL,
    vMotive VARCHAR(255) NULL
);

-- *****************************
-- TABLA: D4_Otorgamiento_Creditos
-- *****************************
CREATE TABLE D4_Otorgamiento_Creditos (
	D4_Key INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    Id_Credito INT,
    Referencia VARCHAR(50) NULL,
    Nombre VARCHAR(100) NULL,
    Fecha_Apertura DATE NULL,
    F_Cobro DATE NULL,
    Id_Convenio INT NULL,
    Convenio VARCHAR(100) NULL,
    Id_Sucursal INT NULL,
    Sucursal VARCHAR(100) NULL,
    Capital DECIMAL(15,2) NULL,
    Primer_Pago DATE NULL,
    Comision DECIMAL(15,2) NULL,
    IVA DECIMAL(15,2) NULL,
    Cobertura DECIMAL(15,2) NULL,
    IVA_Cobertura DECIMAL(15,2) NULL,
    Disposicion DECIMAL(15,2) NULL,
    Monto_Retenido DECIMAL(15,2) NULL,
    Pago_de_Deuda DECIMAL(15,2) NULL,
    Comision_Financiada DECIMAL(15,2) NULL,
    IVA_Comision_Financiada DECIMAL(15,2) NULL,
    Solicitud INT NULL,
    Vendedor INT NULL,
    Nombre_Vendedor VARCHAR(100) NULL,
    TipoVendedor VARCHAR(50) NULL,
    vSupervisorId INT NULL,
    vSupName VARCHAR(100) NULL,
    Producto VARCHAR(100) NULL,
    Descripcion_Tasa VARCHAR(100) NULL,
    Persona DECIMAL(15,2) NULL,
    Plazo INT NULL,
    Id_Producto VARCHAR(100) NULL,
    vCampaign VARCHAR(100) NULL,
    Tipo_de_Financiamiento VARCHAR(100) NULL,
    vFinancingTypeId VARCHAR(100) NULL,
    vAliado VARCHAR(100) NULL,
    vComisionable VARCHAR(25),
	vSolActivas VARCHAR(25)
);

-- *****************************
-- TABLA: D5_Gestiones
-- *****************************
CREATE TABLE D5_Gestiones (
	D5_Key INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    Agencia_Registro VARCHAR(255),
    Causa_No_Pago VARCHAR(255),
    Causa_No_Domiciliacion VARCHAR(255),
    Codigo_Accion VARCHAR(255),
    Codigo_Resultado VARCHAR(255),
    Comentarios TEXT,
    Contacto_Generado VARCHAR(255),
    Coordenadas VARCHAR(255),
    Id_Credito INT,
    Estatus_Promesa VARCHAR(50),
    Fecha_Actividad DATETIME,
    Fecha_Promesa DATE,
    Monto_Promesa DECIMAL(10,2) NULL,
    Origen VARCHAR(50),
    Producto VARCHAR(50),
    Resultado VARCHAR(255),
    Telefono VARCHAR(20),
    Tipo_Pago VARCHAR(50),
    Usuario_Registro VARCHAR(50)
);

-- *****************************
-- TABLA: D6_Quebrantos
-- *****************************
CREATE TABLE D6_Quebrantos (
	D6_Key INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    Operacion INT,
    Referencia INT,
    Nombre VARCHAR(255),
    Convenio VARCHAR(255),
    vFinancingtypeid VARCHAR(50),
    KVigente DECIMAL(10,2),
    KVencido DECIMAL(10,2),
    IntVencido DECIMAL(10,2),
    IVAIntVencido DECIMAL(10,2),
    IntVencidoCO DECIMAL(10,2),
    IVAIntVencidoCO DECIMAL(10,2),
    TotalQuebranto DECIMAL(10,2),
    PagosRealizados DECIMAL(10,2),
    SdoPendiente DECIMAL(10,2),
    IntXDevengar DECIMAL(10,2),
    SdoTotalXPagar DECIMAL(10,2),
    FechaQuebranto VARCHAR(10),
    UltPagoTeorico VARCHAR(10),
    UltimoPago VARCHAR(10),
    UltPagoApl VARCHAR(10),
    Gestor VARCHAR(255),
    nCommission DECIMAL(10,2),
    nCommTax DECIMAL(10,2),
    vMotive VARCHAR(255)
);

-- *****************************
-- TABLA: D7_Juicios
-- *****************************
CREATE TABLE D7_Juicios (
	D7_Key INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    Credito_MC INT,
    Decla VARCHAR(255),
    Descripcion_Cierre TEXT,
    Dias_Activo INT,
    Dias_Caducar INT,
    Estatus VARCHAR(50),
    Etapa_Procesal VARCHAR(255),
    Expediente BIGINT,
    Fecha_Actualizacion DATETIME,
    Fecha_Carga_Inicial DATETIME,
    Fecha_Cierre DATE,
    Fecha_Ultima_Act DATE,
    Id_Juicio INT,
    Juzgado VARCHAR(255),
    Motivo_Cierre VARCHAR(255),
    Producto_MC VARCHAR(50),
    Tipo_Juicio VARCHAR(50),
    Validar_Cierre VARCHAR(50)
);

-- *****************************
-- TABLA: D8_Sistema
-- *****************************
CREATE TABLE D8_Sistema (
	D8_Key INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    Id_Credito INT,
    Referencia VARCHAR(50) NOT NULL,
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
    Diferencia DECIMAL(10,2) NULL,
    Interes_Devengado DECIMAL(10,2) NULL,
    IVA_Interes_Devengado DECIMAL(10,2) NULL,
    Interes_No_DevengadoB DECIMAL(10,2) NULL,
    Fecha_Cartera_Vencida DATE NULL,
    Saldo_Contable DECIMAL(10,2) NULL,
    Saldo_Insoluto DECIMAL(10,2) NULL,
    Porc_Provision DECIMAL(10,2) NULL,
    Reserva DECIMAL(10,2) NULL,
    nCAT DECIMAL(10,2) NULL,
    vOpTable VARCHAR(50) NULL
);

-- *****************************
-- TABLA: D9_Gestores_Area
-- *****************************
CREATE TABLE D9_Gestores_Area (
	D9_Key INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    UsuarioMC VARCHAR(50) NULL,
    Area VARCHAR(50) NULL,
    Estatus INT NULL,
    Perfil VARCHAR(50) NULL
);

-- *****************************
-- TABLAS DE CATÁLOGOS
-- *****************************
CREATE TABLE C1_Dependencia (
	C1_Key INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    Dependencia VARCHAR(100) NULL,
    Abreviatura VARCHAR(100) NULL,
	Abreviatura2 VARCHAR(100) NULL,
    Estatus VARCHAR(100) NULL,
    Clasificacion VARCHAR(100) NULL
);

CREATE TABLE C2_Financiamiento (
	C2_Key INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    Tipo_Financiamiento VARCHAR(50) NULL,
    Financiamiento VARCHAR(50) NULL,
    Producto VARCHAR(50) NULL
);

CREATE TABLE C3_Motivo (
	C3_Key INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    Motivo VARCHAR(100) NULL,
    BanderaPago VARCHAR(50) NULL,
    BanderaDependendia VARCHAR(50) NULL,
    Abreviatura VARCHAR(50) NULL
);

CREATE TABLE C4_Bancos (
	C4_Key INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
	Clabe VARCHAR(100) NULL,
	NombreCorto VARCHAR(100) NULL,
	RazonSocial VARCHAR(100) NULL
);

CREATE TABLE C5_Referencias (
	C5_Key INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    Id_Persona INT NOT NULL,
    Referencia1 VARCHAR(255),
    RELACION1 VARCHAR(255),
    vphonenumber1 VARCHAR(20),
    Referencia2 VARCHAR(255),
    RELACION2 VARCHAR(255),
    vphonenumber2 VARCHAR(20),
    Referencia3 VARCHAR(255),
    RELACION3 VARCHAR(255),
    vphonenumber3 VARCHAR(20),
    Referencia4 VARCHAR(255),
    RELACION4 VARCHAR(255),
    vphonenumber4 VARCHAR(20),
    Referencia5 VARCHAR(255),
    RELACION5 VARCHAR(255),
    vphonenumber5 VARCHAR(20),
    Referencia6 VARCHAR(255),
    RELACION6 VARCHAR(255),
    vphonenumber6 VARCHAR(20),
    Referencia7 VARCHAR(255),
    RELACION7 VARCHAR(255),
    vphonenumber7 VARCHAR(20),
    Referencia8 VARCHAR(255),
    RELACION8 VARCHAR(255),
    vphonenumber8 VARCHAR(20),
    Referencia9 VARCHAR(255),
    RELACION9 VARCHAR(255),
    vphonenumber9 VARCHAR(20),
    Referencia10 VARCHAR(255),
    RELACION10 VARCHAR(255),
    vphonenumber10 VARCHAR(20),
    Referencia11 VARCHAR(255),
    RELACION11 VARCHAR(255),
    vphonenumber11 VARCHAR(20),
    Referencia12 VARCHAR(255),
    RELACION12 VARCHAR(255),
    vphonenumber12 VARCHAR(20),
    Referencia13 VARCHAR(255),
    RELACION13 VARCHAR(255),
    vphonenumber13 VARCHAR(20),
    Referencia14 VARCHAR(255),
    RELACION14 VARCHAR(255),
    vphonenumber14 VARCHAR(20),
    Referencia15 VARCHAR(255),
    RELACION15 VARCHAR(255),
    vphonenumber15 VARCHAR(20),
    Referencia16 VARCHAR(255),
    RELACION16 VARCHAR(255),
    vphonenumber16 VARCHAR(20),
    Referencia17 VARCHAR(255),
    RELACION17 VARCHAR(255),
    vphonenumber17 VARCHAR(20),
    Referencia18 VARCHAR(255),
    RELACION18 VARCHAR(255),
    vphonenumber18 VARCHAR(20),
    Referencia19 VARCHAR(255),
    RELACION19 VARCHAR(255),
    vphonenumber19 VARCHAR(20),
    Referencia20 VARCHAR(255),
    RELACION20 VARCHAR(255),
    vphonenumber20 VARCHAR(20),
    Referencia21 VARCHAR(255),
    RELACION21 VARCHAR(255),
    vphonenumber21 VARCHAR(20),
    Referencia22 VARCHAR(255),
    RELACION22 VARCHAR(255),
    vphonenumber22 VARCHAR(20)
);

-- *****************************
-- TABLAS DE INT (STAGING E INT FINAL)
-- *****************************
CREATE TABLE INT1_STAGE_MDC_TR (
  No_Referencia INT,
  Fecha_de_consulta_MDC VARCHAR(255),
  Indicador_TL INT,
  Fecha_de_integracion DATE,
  ID_Buro INT,
  Clave_de_usuario VARCHAR(50),
  Otorgante VARCHAR(100),
  Telefono_Otorgante VARCHAR(50),
  No_Cuenta VARCHAR(50),
  Responsabilidad CHAR(1),
  Tipo_de_Cuenta CHAR(1),
  Tipo_de_Contrato VARCHAR(2),
  Moneda CHAR(2),
  Importe_Avaluo INT,
  No_de_pagos INT,
  Frecuencia_de_pagos CHAR(1),
  Monto_a_pagar INT,
  Fecha_de_apertura DATE,
  Fecha_de_ultimo_pago DATE,
  Fecha_de_ultima_compra DATE,
  Fecha_de_cierre DATE,
  Fecha_de_actualizacion DATE,
  Ultima_vez_saldo_0 DATE,
  Fecha_primer_historico DATE,
  Fecha_ultimo_historico DATE,
  Fecha_monto_max_morosidad DATE,
  Fecha_reestructura DATE,
  Modo_de_Informe CHAR(1),
  Garantia VARCHAR(100),
  Credito_maximo INT,
  Saldo_actual INT,
  Limite_de_credito INT,
  Saldo_vencido INT,
  No_Pagos_vencidos INT,
  Mop_actual VARCHAR(2),
  Historico_de_pagos VARCHAR(50),
  Claves_de_observacion VARCHAR(50),
  No_Total_de_pagos_revisados INT,
  No_Total_de_pagos_calificados_MOP_02 INT,
  No_Total_de_pagos_calificados_MOP_03 INT,
  No_Total_de_pagos_calificados_MOP_04 INT,
  No_Total_de_pagos_calificados_MOP_05 INT,
  Monto_max_morosidad INT,
  Max_morosidad VARCHAR(2)
);

CREATE TABLE INT2_STAGE_MDC_SC (
    CuentaReferencia INT NOT NULL,
    FechaConsulta VARCHAR(255) NOT NULL,
    ScoreIndicator INT NOT NULL,
    ScoreName VARCHAR(255) NOT NULL,
    ScoreCode VARCHAR(10) NOT NULL,
    ScoreValue VARCHAR(10) NOT NULL,
    ReasonCode1 INT NULL,
    ReasonCode2 INT NULL,
    ReasonCode3 INT NULL,
    ReasonCode4 INT NULL,
    ErrorCode VARCHAR(50) NULL
);

CREATE TABLE INT1_MDC (
	INT1_Key INT AUTO_INCREMENT PRIMARY KEY,
    ID_PERSONA INT,
    ID_CREDITO INT,
    Fecha_de_apertura DATE,
    Estatus VARCHAR(50),
    Fecha_de_consulta_MDC DATE,
    Indicador_TL INT,
    Fecha_de_integracion DATE,
    ID_Buro INT,
    Clave_de_usuario VARCHAR(50),
    Otorgante VARCHAR(100),
    Otorgante_Real VARCHAR(100),
    Propiedad VARCHAR(100),
    Telefono_Otorgante VARCHAR(50),
    No_Cuenta VARCHAR(50),
    Responsabilidad CHAR(1),
    Responsabilidad_Real VARCHAR(100),
    Tipo_de_Cuenta CHAR(1),
    Tipo_de_cuenta_Real VARCHAR(100),
    Tipo_de_Contrato VARCHAR(2),
    Tipo_de_contrato_Real VARCHAR(100),
    Moneda CHAR(2),
    Importe_Avaluo INT,
    No_de_pagos INT,
    Frecuencia_de_pagos CHAR(1),
    Frecuencia_Real VARCHAR(100),
    Monto_a_pagar INT,
    Fecha_de_ultimo_pago DATE,
    Fecha_de_ultima_compra DATE,
    Fecha_de_cierre DATE,
    Fecha_de_actualizacion DATE,
    Ultima_vez_saldo_0 DATE,
    Fecha_primer_historico DATE,
    Fecha_ultimo_historico DATE,
    Fecha_monto_max_morosidad DATE,
    Fecha_reestructura DATE,
    Garantia VARCHAR(100),
    Credito_maximo INT,
    Saldo_actual INT,
    Limite_de_credito INT,
    Saldo_vencido INT,
    No_Pagos_vencidos INT,
    Mop_actual VARCHAR(2),
    Rango_MOP VARCHAR(50),
    Historico_de_pagos VARCHAR(50),
    Claves_de_observacion VARCHAR(50),
    No_Total_de_pagos_revisados INT,
    No_Total_de_pagos_calificados_MOP_02 INT,
    No_Total_de_pagos_calificados_MOP_03 INT,
    No_Total_de_pagos_calificados_MOP_04 INT,
    No_Total_de_pagos_calificados_MOP_05 INT,
    Monto_max_morosidad INT,
    Max_morosidad VARCHAR(2),
    BC_Score INT
);

-- *****************************
-- ÍNDICES
-- *****************************

-- Para INT1_STAGE_MDC_TR
ALTER TABLE INT1_STAGE_MDC_TR
  ADD INDEX idx_int1_stage_tr_no_referencia (No_Referencia),
  ADD INDEX idx_int1_stage_tr_fecha_consulta (Fecha_de_consulta_MDC);

-- Para INT2_STAGE_MDC_SC
ALTER TABLE INT2_STAGE_MDC_SC
  ADD INDEX idx_int2_stage_sc_cuenta_score (CuentaReferencia, ScoreName);

-- Para INT1_MDC
ALTER TABLE INT1_MDC
  ADD INDEX idx_int1_mdc_id_credito (ID_CREDITO),
  ADD INDEX idx_int1_mdc_fecha_consulta (Fecha_de_consulta_MDC),
  ADD INDEX idx_int1_mdc_mop_actual (Mop_actual);


-- *****************************
-- TABLAS DE CATÁLOGOS (INTCAT)
-- *****************************
CREATE TABLE INTCAT1_Responsabilidad (
    INTCAT1_Key INT AUTO_INCREMENT PRIMARY KEY,
    Codigo VARCHAR(2),
    Descripcion VARCHAR(255) NOT NULL
);

CREATE TABLE INTCAT2_TipoContrato (
    INTCAT2_Key INT AUTO_INCREMENT PRIMARY KEY,
	Codigo VARCHAR(2),
    Descripcion VARCHAR(255) NOT NULL
);

CREATE TABLE INTCAT3_TipoCuenta (
    INTCAT3_Key INT AUTO_INCREMENT PRIMARY KEY,
	Codigo VARCHAR(2),
    Descripcion VARCHAR(255) NOT NULL
);

CREATE TABLE INTCAT4_FrecuenciaPago (
    INTCAT4_Key INT AUTO_INCREMENT PRIMARY KEY,
	Codigo VARCHAR(2),
    Descripcion VARCHAR(255) NOT NULL
);

ALTER TABLE INTCAT1_Responsabilidad
  ADD INDEX idx_intcat1_codigo (Codigo);

ALTER TABLE INTCAT2_TipoContrato
  ADD INDEX idx_intcat2_codigo (Codigo);

ALTER TABLE INTCAT3_TipoCuenta
  ADD INDEX idx_intcat3_codigo (Codigo);

ALTER TABLE INTCAT4_FrecuenciaPago
  ADD INDEX idx_intcat4_codigo (Codigo);

-- *****************************
-- FIN DE LA DEFINICIÓN
-- *****************************
