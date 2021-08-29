#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_prov_prov.sh
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

DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.prov_prov_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.prov_prov;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.prov_prov_flatfile (
operacion_cd VARCHAR(1),
proveedor_cd VARCHAR(20),
clave_pais VARCHAR(3),
nombre_prov VARCHAR(100),
nombre_2_prov VARCHAR(100),
nombre_contacto VARCHAR(100),
email VARCHAR(255),
ciudad VARCHAR(100),
codigo_postal VARCHAR(16),
region_cd VARCHAR(3),
direccion VARCHAR(255),
direccion_cd VARCHAR(10),
tratamiento VARCHAR(15),
clave_ramo_ind VARCHAR(4),
fecha_creacion DATE,
nro_cliente VARCHAR(10),
borrado_logico VARCHAR(1),
nro_ident_fiscal VARCHAR(16),
nro_celular VARCHAR(16),
nro_tel_1 VARCHAR(16),
nro_tel_2 VARCHAR(16),
fax VARCHAR(31),
tipo_nro_ident_fiscal VARCHAR(10),
nro_ant_prov VARCHAR(10),
categoria_fiscal VARCHAR(10),
sociedad_cd VARCHAR(4),
Codigo_sap double precision,
Regimen_iva VARCHAR(1))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/prov_prov';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.prov_prov (
operacion_cd VARCHAR(1),
proveedor_cd VARCHAR(20),
clave_pais VARCHAR(3),
nombre_prov VARCHAR(100),
nombre_2_prov VARCHAR(100),
nombre_contacto VARCHAR(100),
email VARCHAR(255),
ciudad VARCHAR(100),
codigo_postal VARCHAR(16),
region_cd VARCHAR(3),
direccion VARCHAR(255),
direccion_cd VARCHAR(10),
tratamiento VARCHAR(15),
clave_ramo_ind VARCHAR(4),
fecha_creacion VARCHAR(10),
nro_cliente VARCHAR(10),
borrado_logico VARCHAR(1),
nro_ident_fiscal VARCHAR(16),
nro_celular VARCHAR(16),
nro_tel_1 VARCHAR(16),
nro_tel_2 VARCHAR(16),
fax VARCHAR(31),
tipo_nro_ident_fiscal VARCHAR(10),
nro_ant_prov VARCHAR(10),
categoria_fiscal VARCHAR(10),
sociedad_cd VARCHAR(4),
Codigo_sap double precision,
Regimen_iva VARCHAR(1))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/prov_prov';
    \p \echo \echo 
    
EOF
