#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_inv_mubi.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.inv_mubi_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.inv_mubi;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.inv_mubi_flatfile(
Ubicación_cd VARCHAR(50),
Ubicación_Desc VARCHAR(250),
Tipo_exhibicion VARCHAR(18),
Mobiliario_cd VARCHAR(18),
Centro_cd VARCHAR(10),
Fecha_Generacion VARCHAR(10))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/inv_mubi';
  
CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.inv_mubi(
Ubicacion_cd VARCHAR(50),
Ubicacion_Desc VARCHAR(250),
Tipo_exhibicion VARCHAR(18),
Mobiliario_cd VARCHAR(18),
Centro_cd VARCHAR(10),
Fecha_Generacion VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/inv_mubi';
    \p \echo \echo 
    
EOF
