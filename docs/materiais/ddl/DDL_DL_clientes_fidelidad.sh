#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_clientes_fidelidad.sh
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

source="fid"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.clientes_fidelidad_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.clientes_fidelidad;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.clientes_fidelidad_flatfile(
Tipo_novedad					VARCHAR(3),
tipo_identificador_cliente		VARCHAR(20),
identificador_cliente_nro		VARCHAR(50),
digito_verificacion			VARCHAR(10),
primer_nombre					VARCHAR(50),
segundo_nombre					VARCHAR(50),
primer_apellido				VARCHAR(50),
segundo_apellido				VARCHAR(50),
nombre_completo				VARCHAR(200),
codigo_responsabilidad			VARCHAR(3),
plazo_comercial				VARCHAR(3),
genero							VARCHAR(3),
fecha_nacimiento				VARCHAR(10),
direccion_residencia			VARCHAR(100),
codigo_ciudad_residencia		VARCHAR(50),
codigo_pais_residencia			VARCHAR(50),
telefono_residencia			VARCHAR(50),
direccion_comercial			VARCHAR(100),
codigo_ciudad_comercial		VARCHAR(50),
codigo_pais_comercial			VARCHAR(50),
telefono_comercial				VARCHAR(50),
nombre_representante_legal		VARCHAR(200),
telefono_celular				VARCHAR(50),
correo_electronico1			VARCHAR(100),
correo_electronico2			VARCHAR(100),
id_sap							VARCHAR(50),
fecha_creacion					VARCHAR(10),
codigo_servicio				VARCHAR(10),
nombre_servicio				VARCHAR(50))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/clientes_fidelidad';
\p \echo \echo 

CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.clientes_fidelidad(
Tipo_novedad					VARCHAR(3),
tipo_identificador_cliente		VARCHAR(20),
identificador_cliente_nro		VARCHAR(50),
digito_verificacion			VARCHAR(10),
primer_nombre					VARCHAR(50),
segundo_nombre					VARCHAR(50),
primer_apellido				VARCHAR(50),
segundo_apellido				VARCHAR(50),
nombre_completo				VARCHAR(200),
codigo_responsabilidad			VARCHAR(3),
plazo_comercial				VARCHAR(3),
genero							VARCHAR(3),
fecha_nacimiento				VARCHAR(10),
direccion_residencia			VARCHAR(100),
codigo_ciudad_residencia		VARCHAR(50),
codigo_pais_residencia			VARCHAR(50),
telefono_residencia			VARCHAR(50),
direccion_comercial			VARCHAR(100),
codigo_ciudad_comercial		VARCHAR(50),
codigo_pais_comercial			VARCHAR(50),
telefono_comercial				VARCHAR(50),
nombre_representante_legal		VARCHAR(200),
telefono_celular				VARCHAR(50),
correo_electronico1			VARCHAR(100),
correo_electronico2			VARCHAR(100),
id_sap							VARCHAR(50),
fecha_creacion					VARCHAR(10),
codigo_servicio				VARCHAR(10),
nombre_servicio				VARCHAR(50))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/clientes_fidelidad';
\p \echo \echo 

EOF
