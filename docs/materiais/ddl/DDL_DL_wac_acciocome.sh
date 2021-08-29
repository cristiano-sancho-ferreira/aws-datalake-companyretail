#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_wac_accicome.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.wac_accicome_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.wac_accicome;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.wac_accicome_flatfile(
CodigoAccionComercial VARCHAR(20),
NombreAccionComercial VARCHAR(100),
CostoMarketing double precision,
CodigoTipoAccionComer VARCHAR(3),
NumeroPaginasTotales int,
NumeroArticulos int,
FechaInicio DATE,
FechaFin DATE,
CodigoSistemaFuente VARCHAR(3))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/wac_accicome';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.wac_accicome(
CodigoAccionComercial VARCHAR(20),
NombreAccionComercial VARCHAR(100),
CostoMarketing double precision,
CodigoTipoAccionComer VARCHAR(3),
NumeroPaginasTotales int,
NumeroArticulos int,
FechaInicio varchar(10),
FechaFin varchar(10),
CodigoSistemaFuente VARCHAR(3))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/wac_accicome';
    \p \echo \echo 
    
EOF
