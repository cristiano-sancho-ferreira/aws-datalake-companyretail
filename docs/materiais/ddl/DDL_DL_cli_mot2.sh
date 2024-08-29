#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_cli_mot2.sh
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

source="mot"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.cli_mot2_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.cli_mot2;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.cli_mot2_flatfile (
codigo_programa VARCHAR(50),
Tipo_registro VARCHAR(10),
tipo_identificador_cliente VARCHAR(20),
identificador_cliente_nro VARCHAR(50),
subprograma_nro VARCHAR(10),
carne_nro VARCHAR(50),
nombre_cliente VARCHAR(200),
direccion_cliente VARCHAR(200),
telefono_cliente VARCHAR(50),
cupo_cliente VARCHAR(50))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
location 's3://${bucketDataRaw}.${source}/cli_mot2';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.cli_mot2 (
codigo_programa VARCHAR(50),
Tipo_registro VARCHAR(10),
tipo_identificador_cliente VARCHAR(20),
identificador_cliente_nro VARCHAR(50),
subprograma_nro VARCHAR(10),
carne_nro VARCHAR(50),
nombre_cliente VARCHAR(200),
direccion_cliente VARCHAR(200),
telefono_cliente VARCHAR(50),
cupo_cliente VARCHAR(50))
STORED AS PARQUET
location 's3://${bucketDataLake}/cli_mot2';
    \p \echo \echo 
    
EOF
