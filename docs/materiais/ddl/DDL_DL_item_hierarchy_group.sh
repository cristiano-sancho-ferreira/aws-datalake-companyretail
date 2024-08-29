#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_item_hierarchy_group.sh
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

source="user"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.item_hierarchy_group_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.item_hierarchy_group;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.item_hierarchy_group_flatfile(
Item_Hierarchy_Group_Cd varchar(50),
Item_Hierarchy_Group_Desc varchar(255))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/item_hierarchy_group';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.item_hierarchy_group(
Item_Hierarchy_Group_Cd varchar(50),
Item_Hierarchy_Group_Desc varchar(255))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/item_hierarchy_group' 
    \p \echo \echo 
    
EOF
