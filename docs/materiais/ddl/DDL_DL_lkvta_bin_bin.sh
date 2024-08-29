#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_lkvta_bin_bin.sh
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

source="jan"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.lkvta_bin_bin_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.lkvta_bin_bin;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.lkvta_bin_bin_flatfile(
CodigoBINId VARCHAR (10),
EntidadFinancieraId VARCHAR (4),
FranquiciaId VARCHAR (4),
TipoTarjetaId VARCHAR (4),
ProductoAsociadoTarjetaId VARCHAR (4),
SubTipo_TarjetaId VARCHAR (4))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/lkvta_bin_bin';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.lkvta_bin_bin(
CodigoBINId VARCHAR (10),
EntidadFinancieraId VARCHAR (4),
FranquiciaId VARCHAR (4),
TipoTarjetaId VARCHAR (4),
ProductoAsociadoTarjetaId VARCHAR (4),
SubTipo_TarjetaId VARCHAR (4))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/lkvta_bin_bin' 
    \p \echo \echo 
    
EOF
