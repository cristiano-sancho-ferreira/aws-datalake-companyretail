#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_pricing_artiason.sh
## Autor      : Rafael Melo(4Strategies)
## Finalidad  : Crear las tablas externas de Data Raw y Data Lake
## 		
## ParÃ¡metros : No Hay
## Retorno    : 0 - OK
##              9 - NOK - Error de ejecuciÃ³n
## Historia   : Fecha     | Descripcion
##              ----------|-----------------------------------------------------------------
##              28/03/2019| Código inicial
###########################################################################################
#set -x
## Carga las variables de entorno
. ${HOME}/ETL/.parameters

source="gnx"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.pricing_artiaso_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.pricing_artiaso;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.pricing_artiaso_flatfile(
articulo_cd VARCHAR(20),
codigo_barras_cd VARCHAR(18),
articulo_cd_agrupado VARCHAR(20),
codigo_barras_cd_agrupado VARCHAR(18),
porcentaje_relacion double precision,
tipo_asociacion VARCHAR(18),
estado VARCHAR(4),
fecha_mov VARCHAR(10))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/pricing_artiaso';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.pricing_artiaso(
articulo_cd VARCHAR(20),
codigo_barras_cd VARCHAR(18),
articulo_cd_agrupado VARCHAR(20),
codigo_barras_cd_agrupado VARCHAR(18),
porcentaje_relacion double precision,
tipo_asociacion VARCHAR(18),
estado VARCHAR(4),
fecha_mov VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/pricing_artiaso' 
    \p \echo \echo 
    
EOF
