#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_prov_surtp.sh
## Autor      : Jackson.Silva(ST IT Consulting)
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.prov_surtp_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.prov_surtp;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.prov_surtp_flatfile (
operacion_cd VARCHAR(1),
proveedor_cd VARCHAR(20),
prov_division_cd VARCHAR(10),
fecha_creac DATE,
nombre_div_prov VARCHAR(100),
regionsgc_cd VARCHAR(18),
clave_pais VARCHAR(3),
region  VARCHAR(3),
ciudad VARCHAR(100),
codigo_postal VARCHAR(16),
direccion VARCHAR(255),
direccion_cd VARCHAR(10),
nombre_contacto VARCHAR(100),
email VARCHAR(255),
nro_celular VARCHAR(16),
nro_tel_1 VARCHAR(16),
nro_tel_2 VARCHAR(16),
fax VARCHAR(31),
bloqueo_logico VARCHAR(10),
nro_sucursal_cliente double precision,
plazo_pago VARCHAR(10),
cod_pla double precision,
cod_sur_sap VARCHAR(6),
cla_geo VARCHAR(1),
seccion VARCHAR(2))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/prov_surtp';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.prov_surtp (
operacion_cd VARCHAR(1),
proveedor_cd VARCHAR(20),
prov_division_cd VARCHAR(10),
fecha_creac VARCHAR(10),
nombre_div_prov VARCHAR(100),
regionsgc_cd VARCHAR(18),
clave_pais VARCHAR(3),
region  VARCHAR(3),
ciudad VARCHAR(100),
codigo_postal VARCHAR(16),
direccion VARCHAR(255),
direccion_cd VARCHAR(10),
nombre_contacto VARCHAR(100),
email VARCHAR(255),
nro_celular VARCHAR(16),
nro_tel_1 VARCHAR(16),
nro_tel_2 VARCHAR(16),
fax VARCHAR(31),
bloqueo_logico VARCHAR(10),
nro_sucursal_cliente double precision,
plazo_pago VARCHAR(10),
cod_pla double precision,
cod_sur_sap VARCHAR(6),
cla_geo VARCHAR(1),
seccion VARCHAR(2))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/prov_surtp';
    \p \echo \echo 
    
EOF
