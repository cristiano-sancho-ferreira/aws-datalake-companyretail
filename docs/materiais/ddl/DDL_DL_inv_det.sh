#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_inv_det.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.inv_det_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.inv_det;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.inv_det_flatfile(
Numero_inventario VARCHAR(50),
Marca_linea VARCHAR(3),
Marca_escaneo VARCHAR(3),
centro_cd VARCHAR(10),
articulo_cd VARCHAR( 18 ),
codigo_barra_cd VARCHAR( 18 ),
umedida_cd VARCHAR(10),
numerador_conv_umb double precision,
unidades_iniciales_qty double precision,
Valor_stock_inicial_amt double precision,
Costo_cpp_unitario double precision,
unidades_reales_qty double precision,
valor_stock_real_amt double precision,
consecutivo_linea INT,
unidades_tienda_qty double precision,
valor_stock_tienda_amt double precision,
unidades_bodega_qty double precision,
valor_stock_bodega_amt double precision,
Unidades_Stock_inicial_qty double precision,
usuario VARCHAR(15),
Hora_inicio VARCHAR(8),
Hora_Fin VARCHAR(8))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/inv_det';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.inv_det(
Numero_inventario VARCHAR(50),
Marca_linea VARCHAR(3),
Marca_escaneo VARCHAR(3),
centro_cd VARCHAR(10),
articulo_cd VARCHAR( 18 ),
codigo_barra_cd VARCHAR( 18 ),
umedida_cd VARCHAR(10),
numerador_conv_umb double precision,
unidades_iniciales_qty double precision,
Valor_stock_inicial_amt double precision,
Costo_cpp_unitario double precision,
unidades_reales_qty double precision,
valor_stock_real_amt double precision,
consecutivo_linea INT,
unidades_tienda_qty double precision,
valor_stock_tienda_amt double precision,
unidades_bodega_qty double precision,
valor_stock_bodega_amt double precision,
Unidades_Stock_inicial_qty double precision,
usuario VARCHAR(15),
Hora_inicio VARCHAR(8),
Hora_Fin VARCHAR(8))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/inv_det' 
    \p \echo \echo 
    
EOF
