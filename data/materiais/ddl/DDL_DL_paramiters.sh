#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_acu_acu.sh
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

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

DROP TABLE IF EXISTS ${SpectrumDatabaseScripts}.parameters;

CREATE EXTERNAL TABLE ${SpectrumDatabaseScripts}.parameters (
InterfaceGroup varchar(255),
TableDataLake varchar(255),
TableFlatFile varchar(255),
Source varchar(10),
Mode varchar(10),
IsPartition varchar(1),
PartitionColumn varchar(255))
ROW FORMAT  serde 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://${bucketScripts}/parameters';
    \p \echo \echo
    
EOF