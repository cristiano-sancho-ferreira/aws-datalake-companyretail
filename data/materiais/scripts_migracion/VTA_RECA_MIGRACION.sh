###########################################################################################
## SHELL      : VTA_RECA_migracion.sh
## Autor      : jackson.silva(4strategies)
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
   
Insert  into cencosud_desa1_datalake_sm_col_migracion.vta_reca(
caja_nro			
,transaccion_nro	
,fecha		
,hora			
,linea_nro			
,concepto_cobro_cd			
,tipo_operacion		
,tipo_pago			
,total_pago_mnt				
,tipo_reacudacion_cd		
,beneficiario_cd			
,cuenta_nro			
,tipo_identifi_titular			
,nro_identifi_titular			
,medio_pago			
,autorizacion_cd		
,doc_referencia_cd					
,lote_sec_nro					
,fecha_contable	
,centro_cd
)				
SELECT CAST(collect_transaction.pos_register_host_cd  as varchar(10)) as caja_nro
,CAST(collect_transaction.sales_transaction_num as varchar(20)) as transaccion_nro
,CAST(sales_transaction.tran_start_dt as varchar(10)) as fecha
,CAST(sales_transaction.tran_start_tm as varchar(08)) as hora
,CAST(collect_transaction.collect_tran_line_num as varchar(10)) as linea_nro
,CAST(collect_transaction_type.collect_tran_type_host_cd as varchar(10)) as concepto_cobro_cd 
,CAST(null as varchar(10)) as tipo_operacion
,CAST(null as varchar(10)) as tipo_pago
,CAST(CASE 
     WHEN collect_transaction.collect_tran_total_amt IN('',NULL) THEN '0'
     ELSE collect_transaction.collect_tran_total_amt 	 
     END as double) as total_pago_mnt
,CAST(collect_transaction_type.collect_tran_subtype_cd as varchar(18)) as tipo_reacudacion_cd
,CAST(null as varchar(10)) as beneficiario_cd  
,CAST(null as varchar(25)) as cuenta_nro  
,CAST(sales_transaction.party_identification_type_cd as varchar(10)) as tipo_identifi_titular	
,CAST(sales_transaction.party_identification_num as varchar(20)) as nro_identifi_titular	
,CAST(null as varchar(10)) as medio_pago
,CAST(null as varchar(10)) as autorizacion_cd 
,CAST(collect_transaction.collect_tran_txt2 as varchar(50)) as doc_referencia_cd	
,CAST(Date_Format(CURRENT_DATE,'%y%m%d%h%m%i') as varchar(10)) as lote_sec_nro
,CAST(collect_transaction.business_dt as varchar(10)) as fecha_contable
,CAST(collect_transaction.location_host_cd as varchar(10)) as centro_cd
FROM cencosud_desa1_datalake_sm_col_teradata.collect_transaction 
LEFT JOIN cencosud_desa1_datalake_sm_col_teradata.sales_transaction on(collect_transaction.sales_tran_id = sales_transaction.sales_tran_id )  
LEFT JOIN cencosud_desa1_datalake_sm_col_teradata.collect_transaction_type on(collect_transaction.Collect_Tran_Type_Id =  
collect_transaction_type.Collect_Tran_Type_Id)
WHERE CAST(collect_transaction.business_dt as varchar(10)) BETWEEN  '${BEGIN_DATE}' AND '${END_DATE}'
AND CAST(sales_transaction.business_dt as varchar(10)) BETWEEN  '${BEGIN_DATE}' AND '${END_DATE}';

exit;
EOF

## log
echo "${BEGIN_DATE}" >> VTA_RECA.log

exit $?
