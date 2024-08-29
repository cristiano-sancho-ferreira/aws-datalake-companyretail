###########################################################################################
## SHELL      : VTA_PROD_migracion.sh
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
   
INSERT INTO cencosud_desa1_datalake_sm_col_migracion.vta_prod
(nro_caja		
,transaccion_nro	
,fecha	
,hora		
,linea_nro			
,codigo_barra_venta		
,plu			
,cantidad			
,total_pago_mnt			
,iva_mnt			
,otros_imp_mnt			
,venta_margen_mnt			
,precio_digitado_ind			
,precio_requerido_ind			
,ingreso_cantidad_ind			
,tipo_linea				
,tipo_ingreso			
,precio_unitario			
,tipo_prog_especial		
,prog_especial_nro			
,tipo_despacho		
,tipo_sena		
,tipo_movi_sena		
,senia_nro		
,modifica_inventario_ind			
,tipo_identifi_supervisor			
,nro_identif_supervisor			
,motivo_devolucion_cd			
,codigo_barra_rebaja		
,precio_original_rebaja	
,motivo_rebaja_cd		
,motivo_rebaja_desc		
,anios_garantia						
,lote_sec_nro			
,tipo_movi_rebaja_cd			
,venta_bruta		
,fecha_contable			
,centro_cd)
SELECT Distinct 
Tab.nro_caja
,Tab.transaccion_nro
,Tab.fecha 
,Tab.hora
,Tab.linea_nro 
,Tab.codigo_barra_venta 
,Tab.plu
,Tab.cantidad
,Tab.total_pago_mnt 
,Tab.iva_mnt
,Tab.otros_imp_mnt 
,(Tab.Unit_Cost_Amt + Tab.Consumption_Tax_Amt) AS venta_margen_mnt
,Tab.precio_digitado_ind
,Tab.precio_requerido_ind
,Tab.ingreso_cantidad_ind
,Tab.tipo_linea
,tipo_ingreso 
,(Tab.Unit_List_Price_Amt / Tab.Item_Qty) AS precio_unitario  
,Tab.tipo_prog_especial 
,Tab.prog_especial_nro
,Tab.tipo_despacho
,Tab.tipo_sena
,Tab.tipo_movi_sena
,Tab.senia_nro
,Tab.modifica_inventario_ind
,Tab.tipo_identifi_supervisor	
,Tab.nro_identif_supervisor	
,Tab.motivo_devolucion_cd	
,Tab.codigo_barra_rebaja
,Tab.precio_original_rebaja
,Tab.motivo_rebaja_cd
,Tab.motivo_rebaja_desc 
,Tab.anios_garantia
,Tab.lote_sec_nro
,Tab.tipo_movi_rebaja_cd 
,Tab.venta_bruta   
,Tab.fecha_contable        
,Tab.centro_cd  
From 
(Select CAST(sales_transaction_line.Pos_Register_Host_CD AS VARCHAR(10)) AS nro_caja
,CAST(sales_transaction_line.Sales_Transaction_Num  AS VARCHAR(20)) AS transaccion_nro
,CAST(sales_transaction.Tran_Start_Tm AS varchar(8)) AS hora
,CAST(sales_transaction_line.Sales_Tran_Line_Num AS VARCHAR(10)) AS linea_nro 
,CAST(sales_transaction_line.Item_Ean_Num AS VARCHAR(18)) AS codigo_barra_venta 
,CAST(SUBSTRING(sales_transaction_line.Item_Class_CD,1,2) AS VARCHAR(18)) AS plu
,CAST(CASE
      WHEN sales_transaction_line.Item_Qty IN('',NULL) THEN '0'
      ELSE sales_transaction_line.Item_Qty  
     END AS DOUBLE ) cantidad
,CAST(CASE 
	  WHEN sales_transaction_line.Unit_List_Price_Amt IN('',NULL) THEN '0'
	  ELSE sales_transaction_line.Unit_List_Price_Amt
	 END AS DOUBLE ) AS total_pago_mnt 
,CAST(CASE
      WHEN sales_transaction_line.Tax_Amt IN('',NULL) THEN '0'
      ELSE sales_transaction_line.Tax_Amt
	 END AS DOUBLE ) AS iva_mnt
,CAST(CASE
      WHEN sales_transaction_line.Consumption_Tax_Amt IN('',NULL) THEN '0'	 
      ELSE sales_transaction_line.Consumption_Tax_Amt 
     END AS DOUBLE ) AS otros_imp_mnt 
 ,CAST(CASE 
	  WHEN sales_transaction_line.Unit_Cost_Amt IN('',NULL) THEN '0'
	  ELSE sales_transaction_line.Unit_Cost_Amt 
	  END AS DOUBLE ) AS Unit_Cost_Amt  
 ,CAST(CASE
       WHEN sales_transaction_line.Consumption_Tax_Amt IN('',NULL) THEN '0' 
	   ELSE sales_transaction_line.Consumption_Tax_Amt 
	   END AS DOUBLE) AS Consumption_Tax_Amt  
,CAST('N' AS VARCHAR(01)) AS precio_digitado_ind
,CAST('N' AS VARCHAR(01)) AS precio_requerido_ind
,CAST('N' AS VARCHAR(01)) AS ingreso_cantidad_ind
,CAST(sales_transaction_line.Sales_Trans_Line_Type_Cd AS VARCHAR(10)) AS tipo_linea

,CAST(sales_transaction_line.Input_Method_Type_Ind  AS VARCHAR(01)) AS tipo_ingreso 

,CAST(CASE 
	  WHEN sales_transaction_line.Unit_List_Price_Amt IN('',NULL) THEN '0'
	  ELSE sales_transaction_line.Unit_List_Price_Amt
	  END AS DOUBLE ) AS Unit_List_Price_Amt 
,CAST(CASE	  
	  WHEN sales_transaction_line.Item_Qty IN('',NULL) THEN '1'
       ELSE sales_transaction_line.Item_Qty
       END  AS DOUBLE ) AS Item_Qty
,CAST(NULL AS VARCHAR(10)) AS tipo_prog_especial 
,CAST(NULL AS VARCHAR(20)) AS prog_especial_nro
,CAST(NULL AS VARCHAR(10)) AS tipo_despacho
,CAST(NULL AS VARCHAR(10)) AS tipo_sena
,CAST(NULL AS VARCHAR(10)) AS tipo_movi_sena
,CAST(NULL AS VARCHAR(20)) AS senia_nro
,CAST(NULL AS varchar(1)) AS modifica_inventario_ind
,CAST(associate.Party_Identification_Type_Cd AS VARCHAR(10)) AS tipo_identifi_supervisor	
,CAST(associate.Party_Identification_Num AS VARCHAR(20)) AS  nro_identif_supervisor	
,CAST(sales_transaction_line.Devolution_Reason_cd AS VARCHAR(10)) AS motivo_devolucion_cd	
,CAST(NULL AS VARCHAR(18)) AS codigo_barra_rebaja
,CAST('0' AS DOUBLE ) AS precio_original_rebaja
,CAST(NULL AS VARCHAR(10)) AS motivo_rebaja_cd
,CAST(NULL AS VARCHAR(100)) AS motivo_rebaja_desc 
,CAST('0' AS DOUBLE ) AS anios_garantia
,CAST(sales_transaction_line.Business_Dt AS VARCHAR(10)) AS fecha_contable
,CAST(Date_Format(CURRENT_DATE,'%y%m%d%h%m%i') AS VARCHAR(10)) AS lote_sec_nro
,CAST(NULL AS VARCHAR(01)) AS tipo_movi_rebaja_cd 
,CAST(CASE
      WHEN sales_transaction_line.Sales_Tran_Line_Amt IN('',NULL) THEN '0'
      ELSE sales_transaction_line.Sales_Tran_Line_Amt
      END AS DOUBLE) AS venta_bruta           
,CAST(sales_transaction_line.Tran_Start_Dt AS VARCHAR(10)) AS fecha 
,CAST(sales_transaction_line.Location_Host_CD AS VARCHAR(10)) AS centro_cd  
from cencosud_desa1_datalake_sm_col_teradata.sales_transaction_line 
inner join cencosud_desa1_datalake_sm_col_teradata.associate on(sales_transaction_line.Manager_Associate_Id = associate.Party_Id)
inner join cencosud_desa1_datalake_sm_col_teradata.sales_transaction on(sales_transaction_line.Sales_Tran_Id = sales_transaction.Sales_tran_Id)
Where CAST(sales_transaction.business_dt as varchar(10))    between '${BEGIN_DATE}' and '${END_DATE}'
and CAST(sales_transaction_line.business_dt as varchar(10)) between '${BEGIN_DATE}' and '${END_DATE}') tab;

exit;
EOF

## log
echo "${BEGIN_DATE}" >> VTA_PROD.log

exit $?
