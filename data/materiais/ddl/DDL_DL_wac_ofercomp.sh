#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_wac_ofercomp.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.wac_ofercomp_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.wac_ofercomp;
EOF

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.wac_ofercomp_flatfile (
CodigoDescuento double precision,
CodigoAccionComercial VARCHAR(20),
DescripcionDescuento VARCHAR(50),
CodigoTipoPrecio int,
FechaInicioDescuento VARCHAR(10),
FechaPlaneadaFinDescuento VARCHAR(10),
FechaRealFinDescuento VARCHAR(10),
CodigoSistemaFuente VARCHAR(3),
CodigoMedioDifusion VARCHAR(3),
NegocioId VARCHAR(3),
CodigoTipoPromocion VARCHAR(20),
CodigoClasePromocion VARCHAR(20))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/wac_ofercomp';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.wac_ofercomp (
CodigoDescuento double precision,
CodigoAccionComercial VARCHAR(20),
DescripcionDescuento VARCHAR(50),
CodigoTipoPrecio int,
FechaInicioDescuento varchar(10),
FechaPlaneadaFinDescuento varchar(10),
FechaRealFinDescuento varchar(10),
CodigoSistemaFuente VARCHAR(3),
CodigoMedioDifusion VARCHAR(3),
NegocioId VARCHAR(3),
CodigoTipoPromocion VARCHAR(20),
CodigoClasePromocion VARCHAR(20))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/wac_ofercomp';
    \p \echo \echo 
    
EOF
