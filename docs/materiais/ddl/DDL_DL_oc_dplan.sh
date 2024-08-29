#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_oc_dplan.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.oc_dplan_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.oc_dplan;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.oc_dplan_flatfile(
tipo_comprobante Varchar(10),
doc_compras_nro VARCHAR(10),
posic_doc_compras_nro Varchar(10),
reparto_nro Varchar(10),
fecha_entrega Varchar(10),
cantidad double precision,
solicitud_pedido_nro varchar(20),
Issuing_Location_Id varchar(10),
nro_oc_compra_sgc varchar(20),
fecha_generacion varchar(10)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/oc_dplan';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.oc_dplan(
tipo_comprobante Varchar(10),
doc_compras_nro VARCHAR(10),
posic_doc_compras_nro Varchar(10),
cantidad double precision,
solicitud_pedido_nro varchar(20),
Issuing_Location_Id varchar(10),
nro_oc_compra_sgc varchar(20),
fecha_generacion VARCHAR(10))
PARTITIONED BY(fecha_entrega Varchar(10), reparto_nro Varchar(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/oc_dplan' 
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla oc_dplan
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.oc_dplan" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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
