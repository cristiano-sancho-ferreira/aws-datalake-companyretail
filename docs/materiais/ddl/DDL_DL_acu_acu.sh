#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_acu_acu.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.acu_acu_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.acu_acu;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.acu_acu_flatfile(
operacion_cd VARCHAR(3),
acuerdo_cd VARCHAR(10),
acuerdo_clase VARCHAR(10),
fecha_creacion VARCHAR(10),
hora VARCHAR(8),
fecha_modif VARCHAR(10),
hora_modif VARCHAR(8),
mone_cd VARCHAR(10),
nombre_ext_acu VARCHAR(255),
orga_compras VARCHAR(10),
proveedor_cd VARCHAR(10),
status_acuerdo_cd VARCHAR(10),
fecha_ini_validez VARCHAR(10),
fecha_fin_validez VARCHAR(10),
clase_grupo VARCHAR(10),
nombre_acuerdo VARCHAR(255),
grupo_compras VARCHAR(10),
sociedad_cd VARCHAR(10),
acuerdo_anterior VARCHAR(10),
prov_division_cd  VARCHAR(10),
clase_grupo_comercial VARCHAR(10),
Tipo_rapel VARCHAR(3),
Tipo_facturable VARCHAR(3),
Tipo_recurrente VARCHAR(3),
Tipo_Automatico VARCHAR(3),
centro_cd VARCHAR(10),
tipo_aplicacion_cd VARCHAR(3),
num_condicion VARCHAR(20),
porcentaje_descu double precision,
monto_descu double precision,
seccion double precision)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/acu_acu';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.acu_acu(
operacion_cd VARCHAR(3),
acuerdo_cd VARCHAR(10),
acuerdo_clase VARCHAR(10),
fecha_creacion VARCHAR(10),
hora VARCHAR(8),
fecha_modif VARCHAR(10),
hora_modif VARCHAR(8),
mone_cd VARCHAR(10),
nombre_ext_acu VARCHAR(255),
orga_compras VARCHAR(10),
proveedor_cd VARCHAR(10),
status_acuerdo_cd VARCHAR(10),
fecha_ini_validez VARCHAR(10),
fecha_fin_validez VARCHAR(10),
clase_grupo VARCHAR(10),
nombre_acuerdo VARCHAR(255),
grupo_compras VARCHAR(10),
sociedad_cd VARCHAR(10),
acuerdo_anterior VARCHAR(10),
prov_division_cd  VARCHAR(10),
clase_grupo_comercial VARCHAR(10),
Tipo_rapel VARCHAR(3),
Tipo_facturable VARCHAR(3),
Tipo_recurrente VARCHAR(3),
Tipo_Automatico VARCHAR(3),
centro_cd VARCHAR(10),
tipo_aplicacion_cd VARCHAR(3),
num_condicion VARCHAR(20),
porcentaje_descu double precision,
monto_descu double precision,
seccion double precision)
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/acu_acu' 
    \p \echo \echo 
    
EOF
