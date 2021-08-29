#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_inv_ubiarti.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.inv_ubiarti_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.inv_ubiarti;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.inv_ubiarti_flatfile(
centro_cd VARCHAR(10),
Ubicación_cd VARCHAR(50),
articulo_cd VARCHAR(18),
codigo_barra_cd VARCHAR(18),
Fecha_ubicacion VARCHAR(10),
Fecha_ult_Inventario VARCHAR(10),
Estado_ult_Inventario VARCHAR(5),
Programa double precision)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/inv_ubiarti';
  
CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.inv_ubiarti(
centro_cd VARCHAR(10),
Ubicación_cd VARCHAR(50),
articulo_cd VARCHAR(18),
codigo_barra_cd VARCHAR(18),
Fecha_ubicacion VARCHAR(10),
Fecha_ult_Inventario VARCHAR(10),
Estado_ult_Inventario VARCHAR(5),
Programa double precision)
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/inv_ubiarti';
    \p \echo \echo 
    
EOF
