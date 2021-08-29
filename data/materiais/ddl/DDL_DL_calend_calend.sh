#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_calend_calend.sh
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
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.calend_calend_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.calend_calend;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.calend_calend_flatfile(
Calendar_Dt	varchar(10),
Day_Of_week_Num	int,
Day_Of_Year_Num	int,
Reporting_Week_Id	int,
Reporting_Month_Id	int,
Reporting_Quarter_Id	int,
Reporting_Semester_Id	int,
Reporting_Year_Id	int,
Reporting_Year_Week_Id	int,
Report_Last_Year_Dt	varchar(10),
Report_Last_Year_Id	int,
Report_Holiday_id	int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/calend_calend';
    \p \echo \echo 


CREATE EXTERNAL TABLE  ${SpectrumDatabaseDataLake}.calend_calend(
Calendar_Dt	varchar(10),
Day_Of_week_Num	int,
Day_Of_Year_Num	int,
Reporting_Week_Id	int,
Reporting_Month_Id	int,
Reporting_Quarter_Id	int,
Reporting_Semester_Id	int,
Reporting_Year_Id	int,
Reporting_Year_Week_Id	int,
Report_Last_Year_Dt	varchar(10),
Report_Last_Year_Id	int,
Report_Holiday_id	int)
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/calend_calend';
    \p \echo \echo 
    
EOF
