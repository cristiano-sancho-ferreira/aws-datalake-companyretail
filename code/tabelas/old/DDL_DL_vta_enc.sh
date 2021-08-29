#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_vta_enc.sh
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

source="jan"
S3_PATH_APPLICATION="s3://${bucketDataLake}/tmp"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.vta_enc_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.vta_enc;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.vta_enc_flatfile (
centro_cd VARCHAR(10),
local_gestion VARCHAR(10),
caja_nro VARCHAR(10),
transaccion_nro VARCHAR(20),
fecha DATE,
hora VARCHAR(8),
tipo_transaccion VARCHAR(10),
tipo_identificador_operador VARCHAR(10),
identificador_operador_nro VARCHAR(20),
total_financiamiento_mnt double precision,
total_mnt double precision,
tiempo_escaneo double precision,
tiempo_cobro double precision,
tiempo_desconexion double precision,
tiempo_inactivo double precision,
transaccion_ind VARCHAR(32),
tipo_comprobante VARCHAR(10),
documento_nro VARCHAR(20),
tipo_documento_ref VARCHAR(10),
documento_ref_nro VARCHAR(20),
canal_venta_cd VARCHAR(10),
canal_pedido_nro VARCHAR(18),
tipo_identif_cliente VARCHAR(10),
cliente_identif_nro VARCHAR(20),
tipo_identif_comprador VARCHAR(10),
comprador_identif_nro VARCHAR(20),
tipo_identif_empleado VARCHAR(10),
empleado_identif_nro VARCHAR(20),
tipo_programa_especial VARCHAR(10),
programa_especial_nro VARCHAR(20),
fecha_contable DATE,
merc_nivel1_cd VARCHAR(10),
tipo_identif_vendedor VARCHAR(10),
nro_identif_vendedor VARCHAR(20),
tipo_comision_vendedor VARCHAR(10),
tipo_venta_ind VARCHAR(1),
caja_fiscal_nro VARCHAR(20),
codigo_postal_cliente VARCHAR(10),
version_software_pos VARCHAR(50),
anulacion_ind VARCHAR(1),
modo_transaccion_cd VARCHAR(1),
autoriza_modificacion_ind VARCHAR(1),
lote_sec_nro  varchar(10),
categoria_ib_cliente_cd VARCHAR(10),
zeta_nro VARCHAR(10),
Fecha_fin_transaccion DATE,
hora_fin_transaccion VARCHAR(20))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/vta_enc';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.vta_enc (
local_gestion VARCHAR(10),
caja_nro VARCHAR(10),
transaccion_nro VARCHAR(20),
fecha VARCHAR(10),
hora VARCHAR(8),
tipo_transaccion VARCHAR(10),
tipo_identificador_operador VARCHAR(10),
identificador_operador_nro VARCHAR(20),
total_financiamiento_mnt double precision,
total_mnt double precision,
tiempo_escaneo double precision,
tiempo_cobro double precision,
tiempo_desconexion double precision,
tiempo_inactivo double precision,
transaccion_ind VARCHAR(32),
tipo_comprobante VARCHAR(10),
documento_nro VARCHAR(20),
tipo_documento_ref VARCHAR(10),
documento_ref_nro VARCHAR(20),
canal_venta_cd VARCHAR(10),
canal_pedido_nro VARCHAR(18),
tipo_identif_cliente VARCHAR(10),
cliente_identif_nro VARCHAR(20),
tipo_identif_comprador VARCHAR(10),
comprador_identif_nro VARCHAR(20),
tipo_identif_empleado VARCHAR(10),
empleado_identif_nro VARCHAR(20),
tipo_programa_especial VARCHAR(10),
programa_especial_nro VARCHAR(20),
merc_nivel1_cd VARCHAR(10),
tipo_identif_vendedor VARCHAR(10),
nro_identif_vendedor VARCHAR(20),
tipo_comision_vendedor VARCHAR(10),
tipo_venta_ind VARCHAR(1),
caja_fiscal_nro VARCHAR(20),
codigo_postal_cliente VARCHAR(10),
version_software_pos VARCHAR(50),
anulacion_ind VARCHAR(1),
modo_transaccion_cd VARCHAR(1),
autoriza_modificacion_ind VARCHAR(1),
lote_sec_nro  varchar(10),
categoria_ib_cliente_cd VARCHAR(10),
zeta_nro VARCHAR(10),
Fecha_fin_transaccion VARCHAR(10),
hora_fin_transaccion VARCHAR(20))
PARTITIONED BY (fecha_contable VARCHAR(10), centro_cd VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/vta_enc';
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla vta_enc
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.vta_enc" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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