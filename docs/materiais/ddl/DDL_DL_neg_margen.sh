#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_neg_margen.sh
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

source="user"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.neg_margen_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.neg_margen;
EOF

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.neg_margen_flatfile (
Ano_Mes varchar(6),
Semana varchar(3),
Tienda varchar(10),
Seccion	varchar(3),
Proveedor varchar(3),
Tipo_Concepto varchar(10),
ID_Buscar varchar(10),
Id_Concepto	varchar(10),
Valor double precision)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/neg_margen';
    \p \echo \echo

CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.neg_margen (
Ano_Mes varchar(6),
Semana varchar(3),
Tienda varchar(10),
Seccion	varchar(3),
Proveedor varchar(3),
Tipo_Concepto varchar(10),
ID_Buscar varchar(10),
Id_Concepto	varchar(10),
Valor double precision)
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/neg_margen';
    \p \echo \echo 
    
EOF
