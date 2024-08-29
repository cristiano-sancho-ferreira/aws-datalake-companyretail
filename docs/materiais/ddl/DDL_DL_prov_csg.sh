#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_prov_csg.sh
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
S3_PATH_APPLICATION="s3://${bucketDataLake}/tmp"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.prov_csg_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.prov_csg;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.prov_csg_flatfile(
prov_divison_cd varchar(10),
centro_cd VARCHAR(10),
fecha_alta varchar(10),
proveedor_xd_ind VARCHAR(3),
proveedor_domicilio_ind VARCHAR(3),
proveedor_cambio_costo VARCHAR(3),
proveedor_inventariar VARCHAR(3),
periocidad_liquidacion VARCHAR(255),
proveedor_cd VARCHAR(10))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/prov_csg';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.prov_csg(
prov_divison_cd varchar(10),
centro_cd VARCHAR(10),
proveedor_xd_ind VARCHAR(3),
proveedor_domicilio_ind VARCHAR(3),
proveedor_cambio_costo VARCHAR(3),
proveedor_inventariar VARCHAR(3),
periocidad_liquidacion VARCHAR(255),
proveedor_cd VARCHAR(10))
PARTITIONED BY (fecha_alta varchar(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/prov_csg' 
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla prov_csg
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.prov_csg" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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
