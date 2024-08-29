#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_cotiz_enc.sh
## Autor      : Silva
## Finalidad  : Crear las tablas externas de Data Raw y Data Lake
## 		
## ParÃ¡metros : No Hay
## Retorno    : 0 - OK
##              9 - NOK - Error de ejecuciÃ³n
## Historia   : Fecha     | Descripcion
##              ----------|-----------------------------------------------------------------
##              10/07/2018| Código inicial
###########################################################################################
#set -x
## Carga las variables de entorno
. ${HOME}/ETL/.parameters

source="gnx"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.cotiz_enc_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.cotiz_enc;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.cotiz_enc_flatfile(
ID_TRANSACCION VARCHAR(50),
CENTRO_CD VARCHAR(4),
FECHA_INICIO_TRANS VARCHAR(10),
HORA_INICIO_TRANS VARCHAR(8),
FECHA_FIN_TRANS VARCHAR(10),
HORA_FIN_TRANS VARCHAR(8),
ID_ESTADO Varchar(10),
ORIGEN_VENTA_CD VARCHAR(5),
ORIGEN_PEDIDO_CD VARCHAR(5),
SUB_TIPO_OPERACION_CD VARCHAR(50),
OBSERVACIONES VARCHAR(255),
RANGO_ENTREGA_CD VARCHAR(13),
ID_TRANSACCION_ANTERIOR double precision,
FECHA_FACTURACION VARCHAR(10),
HORA_FACTURACION VARCHAR(8),
ID_TRN double precision,
POS_NUMERO_CAJA double precision,
POS_NUMERO_TICKET varchar(20),
TIPO_OPERADOR_ID VARCHAR(10),
OPERADOR_ID VARCHAR(50),
NOMBRE_OPERADOR VARCHAR(50),
POS_LEGAJO_CAJERO double precision,
FLAG_VENTA_EMPRESA VARCHAR(3),
NRO_SENIA VARCHAR(60),
TELEFONO_CONTACTO Varchar(30),
FECHA_VENCIMIENTO_ACOPIO VARCHAR(10),
TIPO_DOCUMENTO_IDENT VARCHAR(10),
DOC_IDENT_NRO VARCHAR(20),
NOM_AUTORIZO_MP VARCHAR(50),
FL_APROBADO VARCHAR(3),
IMPORTE_TOTAL double precision,
IMPORTE_FLETE double precision,
IMPORTE_LINEA_CAJA double precision,
VENDEDOR_CD VARCHAR(10),
GIRO_CD VARCHAR(10),
GIRO_DESC VARCHAR(255),
DIAS_VALIDEZ_CNT VARCHAR(10),
CONDICION_PAGO_CD VARCHAR(10),
MARGEN_TOTAL_PCT VARCHAR(10),
PROYECTO_CLIENTE_CD VARCHAR(30),
PROYECTO_CLIENTE_DESC VARCHAR(255),
FECHA_ESTIM_ENTREGA VARCHAR(10),
MARCA_PERSONA_PAGO VARCHAR(255),
ID_TRANS_VISITA VARCHAR(20))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/cotiz_enc';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.cotiz_enc(
ID_TRANSACCION VARCHAR(50),
CENTRO_CD VARCHAR(4),
FECHA_INICIO_TRANS VARCHAR(10),
HORA_INICIO_TRANS VARCHAR(8),
FECHA_FIN_TRANS VARCHAR(10),
HORA_FIN_TRANS VARCHAR(8),
ID_ESTADO Varchar(10),
ORIGEN_VENTA_CD VARCHAR(5),
ORIGEN_PEDIDO_CD VARCHAR(5),
SUB_TIPO_OPERACION_CD VARCHAR(50),
OBSERVACIONES VARCHAR(255),
RANGO_ENTREGA_CD VARCHAR(13),
ID_TRANSACCION_ANTERIOR double precision,
FECHA_FACTURACION VARCHAR(10),
HORA_FACTURACION VARCHAR(8),
ID_TRN double precision,
POS_NUMERO_CAJA double precision,
POS_NUMERO_TICKET varchar(20),
TIPO_OPERADOR_ID VARCHAR(10),
OPERADOR_ID VARCHAR(50),
NOMBRE_OPERADOR VARCHAR(50),
POS_LEGAJO_CAJERO double precision,
FLAG_VENTA_EMPRESA VARCHAR(3),
NRO_SENIA VARCHAR(60),
TELEFONO_CONTACTO Varchar(30),
FECHA_VENCIMIENTO_ACOPIO VARCHAR(10),
TIPO_DOCUMENTO_IDENT VARCHAR(10),
DOC_IDENT_NRO VARCHAR(20),
NOM_AUTORIZO_MP VARCHAR(50),
FL_APROBADO VARCHAR(3),
IMPORTE_TOTAL double precision,
IMPORTE_FLETE double precision,
IMPORTE_LINEA_CAJA double precision,
VENDEDOR_CD VARCHAR(10),
GIRO_CD VARCHAR(10),
GIRO_DESC VARCHAR(255),
DIAS_VALIDEZ_CNT VARCHAR(10),
CONDICION_PAGO_CD VARCHAR(10),
MARGEN_TOTAL_PCT VARCHAR(10),
PROYECTO_CLIENTE_CD VARCHAR(30),
PROYECTO_CLIENTE_DESC VARCHAR(255),
FECHA_ESTIM_ENTREGA VARCHAR(10),
MARCA_PERSONA_PAGO VARCHAR(255),
ID_TRANS_VISITA VARCHAR(20))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/cotiz_enc' 
\p \echo \echo 
    
EOF
