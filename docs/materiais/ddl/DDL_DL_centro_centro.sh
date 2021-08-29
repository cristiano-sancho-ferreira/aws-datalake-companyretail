#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_centro_centro.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.centro_centro_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.centro_centro;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.centro_centro_flatfile (
operacion_cd VARCHAR(1),
centro_cd VARCHAR(10),
centro_nombre VARCHAR(100),
nro_cliente VARCHAR(10),
direccion  VARCHAR(255),
codigo_postal  VARCHAR(16),
ciudad VARCHAR(255),
orga_compras VARCHAR(4),
orga_ventas VARCHAR(4),
clave_pais VARCHAR(3),
region_cd VARCHAR(3),
cadena_cd VARCHAR(3),
codigo_municipal VARCHAR(4),
direccion_local VARCHAR(10),
canal_distribucion VARCHAR(2),
tipo_centro VARCHAR(10),
proveedor_regular_ind VARCHAR(1),
zona_costo VARCHAR(10),
id_ant_centro VARCHAR(10),
regionsgc_cd VARCHAR(4),
formato_local varchar(10),
zona_comercial varchar(20),
estado_local varchar(10),
fecha_inc_actividad DATE,
fecha_fin_actividad DATE,
superf_salon VARCHAR(10),
sociedad_cd VARCHAR(4),
virtual varchar(1), 
descripcion_corta varchar(255))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/centro_centro';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.centro_centro (
operacion_cd VARCHAR(1),
centro_cd VARCHAR(10),
centro_nombre VARCHAR(100),
nro_cliente VARCHAR(10),
direccion  VARCHAR(255),
codigo_postal  VARCHAR(16),
ciudad VARCHAR(255),
orga_compras VARCHAR(4),
orga_ventas VARCHAR(4),
clave_pais VARCHAR(3),
region_cd VARCHAR(3),
cadena_cd VARCHAR(3),
codigo_municipal VARCHAR(4),
direccion_local VARCHAR(10),
canal_distribucion VARCHAR(2),
tipo_centro VARCHAR(10),
proveedor_regular_ind VARCHAR(1),
zona_costo VARCHAR(10),
id_ant_centro VARCHAR(10),
regionsgc_cd VARCHAR(4),
formato_local varchar(10),
zona_comercial varchar(20),
estado_local varchar(10),
fecha_inc_actividad VARCHAR(10),
fecha_fin_actividad VARCHAR(10),
superf_salon VARCHAR(10),
sociedad_cd VARCHAR(4),
virtual varchar(1), 
descripcion_corta varchar(255))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/centro_centro';
    \p \echo \echo 
    
EOF
