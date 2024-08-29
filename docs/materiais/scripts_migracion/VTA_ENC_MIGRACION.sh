###########################################################################################
## SHELL      : VTA_ENC_MIGRACION.sh
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
##set -e

BEGIN_DATE=$1
END_DATE=$2

presto-cli --catalog hive  <<EOF
   
insert into cencosud_desa1_datalake_sm_col_migracion.vta_enc(
local_gestion				
,caja_nro			
,transaccion_nro	
,fecha		
,hora			
,tipo_transaccion			
,tipo_identificador_operador				
,identificador_operador_nro				
,total_financiamiento_mnt		
,total_mnt				
,tiempo_escaneo				
,tiempo_cobro				
,tiempo_desconexion			
,tiempo_inactivo				
,transaccion_ind				
,tipo_comprobante			
,documento_nro			
,tipo_documento_ref				
,documento_ref_nro				
,canal_venta_cd			
,canal_pedido_nro				
,tipo_identif_cliente				
,cliente_identif_nro				
,tipo_identif_comprador				
,comprador_identif_nro				
,tipo_identif_empleado				
,empleado_identif_nro			
,tipo_programa_especial			
,programa_especial_nro		
,merc_nivel1_cd				
,tipo_identif_vendedor			
,nro_identif_vendedor			
,tipo_comision_vendedor			
,tipo_venta_ind				
,caja_fiscal_nro			
,codigo_postal_cliente				
,version_software_pos			
,anulacion_ind				
,modo_transaccion_cd				
,autoriza_modificacion_ind				
,lote_sec_nro			
,categoria_ib_cliente_cd				
,zeta_nro			
,fecha_fin_transaccion			
,hora_fin_transaccion		
,fecha_contable				
,centro_cd
)
SELECT CAST(sales_transaction.Location_Host_CD as varchar(10)) as local_gestion
       ,CAST(sales_transaction.Pos_Register_Host_CD as varchar(10))  as caja_nro
       ,CAST(sales_transaction.sales_transaction_Num as varchar(20)) as transaccion_nro
       ,CAST(sales_transaction.Tran_Start_Dt as varchar(10)) as fecha
       ,CAST(sales_transaction.Tran_Start_Tm as varchar(08)) as hora
	   ,CAST(sales_transaction.Tipotrans_Ind as varchar(10)) as tipo_transaccion
       ,CAST(associate.Party_Identification_Type_Cd as varchar(10)) as tipo_identificador_operador
       ,CAST(associate.Party_Identification_Num as varchar(20)) as identificador_operador_nro
	   ,CAST(CASE    
	     WHEN sales_transaction.Total_Tax_Amt in('',NULL) then '0'
	     ELSE sales_transaction.Total_Tax_Amt  
	    END as double precision) total_financiamiento_mnt 
	   ,CAST(CASE
	     WHEN sales_transaction.Total_Tax_Amt in('',NULL) then '0'
	     ELSE sales_transaction.Total_Tax_Amt  
	    END as double precision) total_mnt 
	   ,CAST(NULL as double) as tiempo_escaneo
       ,CAST(NULL as double) as tiempo_cobro
       ,CAST(NULL as double) as tiempo_desconexion
       ,CAST(NULL as double) as tiempo_inactivo
       ,CAST(NULL as varchar(32))   as transaccion_ind
       ,CAST('T'  as varchar(10)) as tipo_comprobante
       ,CAST(sales_transaction.sales_transaction_Num as varchar(20)) as documento_nro
       ,CAST(NULL as varchar(10)) as tipo_documento_ref 
	   ,CAST(NULL as varchar(20)) as documento_ref_nro
       ,CAST(sales_transaction.Channel_CD as varchar(10)) as canal_venta_cd
       ,CAST(NULL as varchar(18)) as canal_pedido_nro
       ,CAST(sales_transaction.Party_Identification_Type_Cd as varchar(10)) as tipo_identif_cliente
       ,CAST(sales_transaction.Party_Identification_Num  as varchar(20)) as cliente_identif_nro
       ,CAST(NULL as varchar(10)) as tipo_identif_comprador
       ,CAST(NULL as varchar(20)) as comprador_identif_nro
       ,CAST(associate.Party_Identification_Type_Cd as varchar(10)) as tipo_identif_empleado
	   ,CAST(associate.Party_Identification_Num as varchar(20)) as empleado_identif_nro
	   ,CAST(NULL as varchar(10)) as tipo_programa_especial
       ,CAST(NULL as varchar(20)) as programa_especial_nro
       ,CAST(NULL as varchar(10)) as merc_nivel1_cd
       ,CAST(associate.Party_Identification_Type_Cd as varchar(10)) as tipo_identif_vendedor 
	   ,CAST(associate.Party_Identification_Num as varchar(20)) as nro_identif_vendedor  
	   ,CAST(NULL as varchar(10)) as tipo_comision_vendedor
       ,CAST('N' as  varchar(01))   tipo_venta_ind
       ,CAST(sales_transaction.Pos_Register_Host_CD as varchar(20)) as caja_fiscal_nro
       ,CAST(NULL as varchar(10))  as codigo_postal_cliente
       ,CAST(NULL as varchar(50)) as version_software_pos
       ,CAST(CASE
         WHEN sales_transaction.Tipotrans_Ind IN ('A', 'E') THEN 'S'
         ELSE 'N'
		 END as varchar(01)) as anulacion_ind
       ,CAST(NULL as varchar(01)) as modo_transaccion_cd
       ,CAST(NULL as varchar(01)) as autoriza_modificacion_ind
       ,CAST(Date_Format(CURRENT_DATE,'%y%m%d%h%m%i') as varchar(10)) as lote_sec_nro
	   ,CAST(NULL as varchar(10)) as categoria_ib_cliente_cd
       ,CAST(NULL as varchar(10)) as zeta_nro
       ,CAST(sales_transaction.Tran_End_Dt as varchar(10)) as fecha_fin_transaccion
       ,CAST(sales_transaction.Tran_End_Tm as varchar(20)) hora_fin_transaccion
       ,CAST(sales_transaction.Business_dt as varchar(10)) as fecha_contable
       ,CAST(sales_transaction.Location_Host_CD as varchar(10)) as centro_cd
   from cencosud_desa1_datalake_sm_col_teradata.sales_transaction 
   left Join cencosud_desa1_datalake_sm_col_teradata.associate on (sales_transaction.Sales_Associate_Id = associate.Party_Id)
   Where CAST(sales_transaction.business_dt as varchar(10)) between  '${BEGIN_DATE}' and '${END_DATE}'; 

exit;
EOF

## log
echo "${BEGIN_DATE}" >> VTA_ENC.log

exit $?
