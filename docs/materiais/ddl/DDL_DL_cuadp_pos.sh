#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_cuadp_pos.sh
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

source_gnx="gnx"
source_jan="jan"
S3_PATH_APPLICATION="s3://${bucketDataLake}/tmp"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.cuadp_pos_${source_gnx}_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.cuadp_pos_${source_gnx};
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.cuadp_pos_${source_jan}_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.cuadp_pos_${source_jan};
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.cuadp_pos_${source_gnx}_flatfile (
Country_CD VARCHAR(50),
Business_Unit_Id VARCHAR(50),
Kpi_Id INT,
Calendar_Dt VARCHAR(10),
Primary_Dim_Obj_Level_2_Id VARCHAR(50),
Primary_Dim_Level_2_id VARCHAR(50),
Secondary_Dim_Obj_Level_2_Id VARCHAR(50),
Secondary_Dim_Level_2_id VARCHAR(50),
Source_ID varchar(3),
Version_Id INT,
Kpi_Analysis_Amt double precision)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source_gnx}/cuadp_pos_${source_gnx}';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.cuadp_pos_${source_gnx} (
Country_CD VARCHAR(50),
Business_Unit_Id VARCHAR(50),
Kpi_Id INT,
Primary_Dim_Obj_Level_2_Id VARCHAR(50),
Secondary_Dim_Obj_Level_2_Id VARCHAR(50),
Secondary_Dim_Level_2_id VARCHAR(50),
Source_ID varchar(3),
Version_Id INT,
Kpi_Analysis_Amt double precision)
PARTITIONED BY(Calendar_Dt VARCHAR(10), Primary_Dim_Level_2_id VARCHAR(50))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/cuadp_pos_${source_gnx}';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.cuadp_pos_${source_jan}_flatfile (
Country_CD VARCHAR(50),
Business_Unit_Id VARCHAR(50),
Kpi_Id INT,
Calendar_Dt VARCHAR(10),
Primary_Dim_Obj_Level_2_Id VARCHAR(50),
Primary_Dim_Level_2_id VARCHAR(50),
Secondary_Dim_Obj_Level_2_Id VARCHAR(50),
Secondary_Dim_Level_2_id VARCHAR(50),
Source_ID varchar(3),
Version_Id INT,
Kpi_Analysis_Amt double precision)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source_jan}/cuadp_pos_${source_jan}';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.cuadp_pos_${source_jan} (
Country_CD VARCHAR(50),
Business_Unit_Id VARCHAR(50),
Kpi_Id INT,
Primary_Dim_Obj_Level_2_Id VARCHAR(50),
Secondary_Dim_Obj_Level_2_Id VARCHAR(50),
Secondary_Dim_Level_2_id VARCHAR(50),
Source_ID varchar(3),
Version_Id INT,
Kpi_Analysis_Amt double precision)
PARTITIONED BY(Calendar_Dt VARCHAR(10), Primary_Dim_Level_2_id VARCHAR(50))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/cuadp_pos_${source_jan}';
    \p \echo \echo 

EOF

## ejecuta comando en el athena
echo creando particiones de la tabla cuadp_pos_${source_gnx}
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.cuadp_pos_${source_gnx}" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

## espera athena terminar de crear particiones
vQUERY_ID=$(echo $QUERY_ID| cut -d'"' -f 4)
vLoop=1
echo -query_id: ${vQUERY_ID}
while [[ 1 -eq $vLoop ]]
do
QUERY_STATE=$(aws athena get-query-execution --region us-east-1 --query-execution-id ${vQUERY_ID})
vQUERY_STATE=$(echo $QUERY_STATE| cut -d'"' -f 10)
echo -status: ${vQUERY_STATE}
case $vQUERY_STATE in
  FAILED|CANCELLED)
   echo #### ERROR #### al crear las particiones
   vLoop=2;;
  SUCCEEDED)
   RC=0
   vLoop=0;;
  *)
   sleep 5
   ;;
esac
done

## ejecuta comando en el athena
echo creando particiones de la tabla cuadp_pos_${source_jan}
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.cuadp_pos_${source_jan}" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

## espera athena terminar de crear particiones
vQUERY_ID=$(echo $QUERY_ID| cut -d'"' -f 4)
vLoop=1
echo -query_id: ${vQUERY_ID}
while [[ 1 -eq $vLoop ]]
do
QUERY_STATE=$(aws athena get-query-execution --region us-east-1 --query-execution-id ${vQUERY_ID})
vQUERY_STATE=$(echo $QUERY_STATE| cut -d'"' -f 10)
echo -status: ${vQUERY_STATE}
case $vQUERY_STATE in
  FAILED|CANCELLED)
   echo #### ERROR #### al crear las particiones
   vLoop=2;;
  SUCCEEDED)
   RC=0
   vLoop=0;;
  *)
   sleep 5
   ;;
esac
done

## apaga carpeta tmp
aws s3 rm ${S3_PATH_APPLICATION} --recursive

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando view

CREATE OR REPLACE VIEW ${schema_vwdatalake}cuadp_pos AS
select  
		country_cd, 
		business_unit_id, 
		kpi_id, 
		calendar_dt, 
		primary_dim_obj_level_2_id, 
		primary_dim_level_2_id, 
		secondary_dim_obj_level_2_id, 
		secondary_dim_level_2_id, 
		source_id, 
		version_id, 
		kpi_analysis_amt
		from
			(
				SELECT * FROM ${SpectrumDatabaseDataLake}.cuadp_pos_${source_gnx}
				UNION ALL
				SELECT * FROM ${SpectrumDatabaseDataLake}.cuadp_pos_${source_jan}
			)
WITH NO SCHEMA binding;
    \p \echo \echo 

EOF
