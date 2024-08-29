#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_pricing_artigrup.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.pricing_artigrup_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.pricing_artigrup;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.pricing_artigrup_flatfile(
articulo_cd VARCHAR(20),
codigo_barras_cd VARCHAR(18),
articulo_cd_agrupado VARCHAR(20),
codigo_barras_cd_agrupado VARCHAR(18),
venta_directa VARCHAR(18),
unidades_relacionadas VARCHAR(18),
codigo_Agrupado VARCHAR(255),
tipo_agrupado_stock VARCHAR(4),
desc_codigo_Agrupado VARCHAR(255))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/pricing_artigrup';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.pricing_artigrup(
articulo_cd VARCHAR(20),
codigo_barras_cd VARCHAR(18),
articulo_cd_agrupado VARCHAR(20),
codigo_barras_cd_agrupado VARCHAR(18),
venta_directa VARCHAR(18),
unidades_relacionadas VARCHAR(18),
codigo_Agrupado VARCHAR(255),
tipo_agrupado_stock VARCHAR(4)),
desc_codigo_Agrupado VARCHAR(255))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/pricing_artigrup' 
    \p \echo \echo 
    
EOF

