###########################################################################################
## SHELL      : VTA_CLIF_MIGRACION.sh
## Autor      : Silva
## Finalidad  : Script que hace inserción de la en migración
## Parámetros : $1 - BEGIN_DATE
##              $2 - END_DATE
## Retorno    : 0 - OK
##              9 - NOK
## Historia   : Fecha     | Descripción
##              ----------|-----------------------------------------------------------------
##              11/10/2018| Código inicial
###########################################################################################
## set -e

BEGIN_DATE=$1
END_DATE=$2

presto-cli --catalog hive  <<EOF
   
insert into cencosud_desa1_datalake_sm_col_migracion.vta_clif (
nro_caja 
,transaccion_nro 
,fecha 
,hora 
,linea_nro 
,programa_cd 
,monto_total  
,factor_conversion 
,tipo_identificador_clifre 
,identificador_clifre_nro 
,lote_sec_nro 
,fecha_contable 
,centro_cd
)
select Cast(sales_transaction.pos_register_host_cd as varchar(10)) as nro_caja
		,Cast(sales_transaction.sales_transaction_num as varchar(20)) as transaccion_nro
        ,Cast(sales_transaction.tran_start_dt as varchar(10)) as fecha
		,Cast(sales_transaction.tran_start_tm as varchar(08)) as  hora
		,'1' as linea_nro
		,'77' as programa_cd
		,Cast(Case
			   When  sales_transaction.total_tax_amt in('',null) then '0'
				else  sales_transaction.total_tax_amt
			  end as double) as monto_total 
		,Cast(1 as double) as factor_conversion 
		,Cast(sales_transaction.party_identification_type_cd as varchar(10)) as tipo_identificador_clifre 
		,Cast(sales_transaction.party_identification_num as varchar(10)) as identificador_clifre_nro
		,Cast(Date_Format(CURRENT_DATE,'%y%m%d%h%m%i') as varchar(10)) as lote_sec_nro
        ,Cast(sales_transaction.business_dt as varchar(10)) as fecha_contable  
		,Cast(sales_transaction.location_host_cd as varchar(10)) as centro_cd
FROM  cencosud_desa1_datalake_sm_col_teradata.sales_transaction
where sales_transaction.tran_loyalty_ind = 'S'  
and CAST(sales_transaction.business_dt as varchar(10)) between '${BEGIN_DATE}' and '${END_DATE}';

exit;
EOF

## log
echo "${BEGIN_DATE}" >> VTA_CLIF.log

exit $?
