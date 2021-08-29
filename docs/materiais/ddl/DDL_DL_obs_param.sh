#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_obs_param.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.obs_param_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.obs_param;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.obs_param_flatfile(
merc_nivel0_cd  VARCHAR(4) ,
merc_nivel1_cd  VARCHAR(4),
merc_nivel2_cd VARCHAR(4) ,
merc_nivel3_cd VARCHAR(4),
merc_nivel4_cd VARCHAR(20) ,
nacional        VARCHAR(3),
dias_ini        double precision,
dias_fin        double precision,
dias_envio      double precision,
porc_prov       double precision,
usuario         VARCHAR(8) ,
estado          VARCHAR(3),
fecha_actualiza VARCHAR(10))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/obs_param';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.obs_param(
merc_nivel0_cd  VARCHAR(4) ,
merc_nivel1_cd  VARCHAR(4),
merc_nivel2_cd VARCHAR(4) ,
merc_nivel3_cd VARCHAR(4),
merc_nivel4_cd VARCHAR(20) ,
nacional        VARCHAR(3),
dias_ini        double precision,
dias_fin        double precision,
dias_envio      double precision,
porc_prov       double precision,
usuario         VARCHAR(8) ,
estado          VARCHAR(3),
fecha_actualiza VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/obs_param' 
    \p \echo \echo 
    
EOF
