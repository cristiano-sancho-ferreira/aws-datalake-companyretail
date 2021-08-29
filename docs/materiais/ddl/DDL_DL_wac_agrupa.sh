#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_wac_agrupa.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.wac_agrupa_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.wac_agrupa;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.wac_agrupa_flatfile(
codigo_de_barras VARCHAR(50),
agrupacion int,
nombre_agrupacion VARCHAR(50),
CodigoDescuento VARCHAR(50),
fecha_inicio VARCHAR(10),
Fecha_fin VARCHAR(10),
CdNegocio VARCHAR(5))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/wac_agrupa';
    \p \echo \echo 
    

CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.wac_agrupa(
codigo_de_barras VARCHAR(50),
agrupacion int,
nombre_agrupacion VARCHAR(50),
CodigoDescuento VARCHAR(50),
fecha_inicio VARCHAR(10),
Fecha_fin VARCHAR(10),
CdNegocio VARCHAR(5))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/wac_agrupa';
    \p \echo \echo 
    
EOF
