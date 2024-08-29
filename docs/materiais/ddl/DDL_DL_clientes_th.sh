#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_clientes_th.sh
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

source="bnk"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.clientes_th_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.clientes_th;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.clientes_th_flatfile(
TipoIdentificacion	VARCHAR(20),
NumeroIdentificacion	VARCHAR(50),
Primer_Nombre	VARCHAR(100),
Segundo_Nombre	VARCHAR(100),
Primer_apellido	VARCHAR(100),
Segundo_apellido	VARCHAR(100),
Fecha_de_Nacimiento	VARCHAR(10),
Genero	VARCHAR(2),
Estado_Civil	VARCHAR(2),
PROFESION	VARCHAR(100),
NivelEducativo	VARCHAR(2),
TipoResidencia	VARCHAR(2),
Estrato	VARCHAR(2),
Codigo_Actividad_Economica	VARCHAR(4),
Direccion_CasaFact_Visa	VARCHAR(100),
Codigo_Ciudad_CasaFact_Visa	VARCHAR(6),
NOMBRE_DEPARTAMENTO	VARCHAR(100),
Telefono_Casa_Fact_VISA	VARCHAR(20),
Telefono_Movil_Fact_VISA	VARCHAR(20),
Email	VARCHAR(100),
Tipo_de_Producto	VARCHAR(2),
Estado_del_Producto_Colpatria	VARCHAR(20),
Fecha_Emision	VARCHAR(10),
Marca_de_la_Tarjeta	VARCHAR(2),
ultimo_Cupo_Global	VARCHAR(14),
Saldo_de_cartera	VARCHAR(10),
Dias_de_mora	VARCHAR(100),
Tipo_Mercado_Solicitud	VARCHAR(100),
Tipo_Mercado_H	VARCHAR(100),
NUMERO_DE_PRODUCTO	VARCHAR(100),
DESCRIPCION_ESTADO_TARJETA	VARCHAR(100),
MARCA_TARJETA_EXTENDIDA	VARCHAR(100))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/clientes_th';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.clientes_th(
TipoIdentificacion	VARCHAR(20),
NumeroIdentificacion	VARCHAR(50),
Primer_Nombre	VARCHAR(100),
Segundo_Nombre	VARCHAR(100),
Primer_apellido	VARCHAR(100),
Segundo_apellido	VARCHAR(100),
Fecha_de_Nacimiento	VARCHAR(10),
Genero	VARCHAR(2),
Estado_Civil	VARCHAR(2),
PROFESION	VARCHAR(100),
NivelEducativo	VARCHAR(2),
TipoResidencia	VARCHAR(2),
Estrato	VARCHAR(2),
Codigo_Actividad_Economica	VARCHAR(4),
Direccion_CasaFact_Visa	VARCHAR(100),
Codigo_Ciudad_CasaFact_Visa	VARCHAR(6),
NOMBRE_DEPARTAMENTO	VARCHAR(100),
Telefono_Casa_Fact_VISA	VARCHAR(20),
Telefono_Movil_Fact_VISA	VARCHAR(20),
Email	VARCHAR(100),
Tipo_de_Producto	VARCHAR(2),
Estado_del_Producto_Colpatria	VARCHAR(20),
Fecha_Emision	VARCHAR(10),
Marca_de_la_Tarjeta	VARCHAR(2),
ultimo_Cupo_Global	VARCHAR(14),
Saldo_de_cartera	VARCHAR(10),
Dias_de_mora	VARCHAR(100),
Tipo_Mercado_Solicitud	VARCHAR(100),
Tipo_Mercado_H	VARCHAR(100),
NUMERO_DE_PRODUCTO	VARCHAR(100),
DESCRIPCION_ESTADO_TARJETA	VARCHAR(100),
MARCA_TARJETA_EXTENDIDA	VARCHAR(100))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/clientes_th';
    \p \echo \echo 
    
EOF
