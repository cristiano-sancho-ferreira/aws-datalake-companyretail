###########################################################################################
## SHELL      : VTA_PAG_migracion.sh
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
   
INSERT INTO cencosud_desa1_datalake_sm_col_migracion.vta_pag(
nro_caja				
,transaccion_nro	
,fecha				
,hora				
,linea_nro			
,tipo_operacion				
,forma_pago_cd			
,variedad_pago_cd				
,total_mnt				
,monto_donacion			
,tipo_instit_donacion				
,institu_donacion_nro				
,moneda_pos_cd				
,tarifa_cambio				
,monto_moneda_orginal				
,ch_tipo_operacion				
,ch_fecha_emision				
,ch_fecha_cobro				
,ch_banco_nro				
,ch_sucursal_nro				
,ch_ctacte_nro				
,ch_cheque_nro				
,ch_tipo_identifi_emisor				
,ch_identifi_emisor_nro				
,cta_cte_nro			
,cta_cte_exten_nro				
,tipo_identifi_cliente_ctacte				
,identifi_cliente_ctacte_nro			
,tj_marca_cd				
,tj_tipo_operacion				
,tj_cuotas			
,tj_plan_cuotas				
,codigo_autoriz				
,lote_trans_nro				
,ticket_nro			
,log_nro				
,tj_hora				
,tj_tiempo				
,tj_tarjeta_nro			
,tj_fecha_expiracion				
,tj_nro_comercio				
,tj_modo_ingreso			
,tj_moneda_transaccion_cd				
,tj_tasa_recargo_pct			
,tj_recargo_mnt				
,nro_identificador_supervisor						
,lote_sec_nro						
,fecha_contable	
,centro_cd
)				
Select Distinct 
  	 Tab.nro_caja
	,Tab.transaccion_nro
    ,Tab.fecha
	,Tab.hora
	,Tab.linea_nro
	,Tab.tipo_operacion
	,Tab.forma_pago_cd
	,Tab.variedad_pago_cd
	,Tab.total_mnt
	,Tab.monto_donacion
	,Tab.tipo_instit_donacion
	,Tab.institu_donacion_nro
	,Tab.moneda_pos_cd
	,Tab.tarifa_cambio
	,Tab.monto_moneda_orginal
	,Tab.ch_tipo_operacion
	,Tab.ch_fecha_emision
	,Tab.ch_fecha_cobro
	,Tab.ch_banco_nro
	,Tab.ch_sucursal_nro
	,Tab.ch_ctacte_nro
	,Tab.ch_cheque_nro
	,Tab.ch_tipo_identifi_emisor
	,Tab.ch_identifi_emisor_nro
	,Tab.cta_cte_nro
	,Tab.cta_cte_exten_nro
	,Tab.tipo_identifi_cliente_ctacte
	,Tab.identifi_cliente_ctacte_nro
	,Tab.tj_marca_cd
	,Tab.tj_tipo_operacion
	,Tab.tj_cuotas
	,Tab.tj_plan_cuotas
	,Tab.codigo_autoriz
	,Tab.lote_trans_nro
	,Tab.ticket_nro
	,Tab.log_nro
	,Tab.tj_hora
	,Tab.tj_tiempo
	,Tab.tj_tarjeta_nro
	,Tab.tj_fecha_expiracion
	,Tab.tj_nro_comercio
	,Tab.tj_modo_ingreso
	,Tab.tj_moneda_transaccion_cd
	,Tab.tj_tasa_recargo_pct
	,Tab.tj_recargo_mnt
	,Tab.nro_identificador_supervisor
	,Tab.lote_sec_nro
    ,Tab.fecha_contable
	,Tab.centro_cd
FROM (SELECT CAST(PAYMENT_LINE.Pos_Register_Host_CD AS VARCHAR(10)) AS nro_caja
	,CAST(PAYMENT_LINE.Sales_Transaction_Num AS VARCHAR(20)) AS transaccion_nro
    ,CAST(PAYMENT_LINE.Tran_Start_Dt AS VARCHAR(10)) AS fecha
	,CAST(Substr(PAYMENT_LINE.tran_start_dttm,12,8) AS VARCHAR(8)) AS hora 
	,CAST(CASE
	      WHEN PAYMENT_LINE.Payment_Tran_Id in('',null) Then '10000' 
		  ELSE LPAD(PAYMENT_LINE.Payment_Tran_Id,5,'10000')
	      End as Varchar(10)) AS linea_nro
	,CAST('V' AS  VARCHAR(20)) AS tipo_operacion 
	,CAST('1' AS  VARCHAR(10)) AS forma_pago_cd
	,CAST(BANK_IDENTIFICATION_NUMBER.Card_Association_Type_Cd AS VARCHAR(10)) AS variedad_pago_cd
	,(CAST(CASE
          WHEN SALES_TRANSACTION.Change_Amt IN('',null) Then '0'
          ELSE SALES_TRANSACTION.Change_Amt
          END AS DOUBLE)*-1) AS total_mnt
	,CAST(NULL AS DOUBLE) AS monto_donacion
	,CAST(NULL AS VARCHAR(10)) AS tipo_instit_donacion
	,CAST(NULL AS VARCHAR(20)) AS institu_donacion_nro
	,CAST('COP' AS VARCHAR(10)) AS moneda_pos_cd
	,CAST(NULL AS DOUBLE) AS  tarifa_cambio
	,(CAST(CASE
           WHEN SALES_TRANSACTION.Change_Amt in('',null) Then '0'
           ELSE SALES_TRANSACTION.Change_Amt
           END AS DOUBLE)*-1)  AS monto_moneda_orginal
	,CAST(NULL AS VARCHAR(10)) AS ch_tipo_operacion
	,CAST(NULL AS VARCHAR(10)) AS ch_fecha_emision
	,CAST(NULL AS VARCHAR(10)) AS ch_fecha_cobro
	,CAST(NULL AS VARCHAR(10)) AS ch_banco_nro
	,CAST(NULL AS VARCHAR(10)) AS ch_sucursal_nro
	,CAST(NULL AS VARCHAR(20)) AS ch_ctacte_nro
	,CAST(NULL AS VARCHAR(10)) AS ch_cheque_nro
	,CAST(NULL AS VARCHAR(10)) AS ch_tipo_identifi_emisor
	,CAST(NULL AS VARCHAR(20)) AS ch_identifi_emisor_nro
	,CAST(NULL AS VARCHAR(10)) AS cta_cte_nro
	,CAST(NULL AS VARCHAR(10)) AS cta_cte_exten_nro
    ,CAST(NULL AS VARCHAR(10)) AS tipo_identifi_cliente_ctacte
	,CAST(NULL AS VARCHAR(20)) AS identifi_cliente_ctacte_nro
	,CAST(BANK_IDENTIFICATION_NUMBER.Card_Association_Type_Cd AS VARCHAR(20)) AS tj_marca_cd
	,CAST(SALES_TRANSACTION.TipoTrans_Ind AS VARCHAR(10)) AS tj_tipo_operacion
	,CAST(PAYMENT_LINE.Card_Quotas_Qty AS VARCHAR(10)) AS tj_cuotas
	,CAST(NULL AS  VARCHAR(10)) AS tj_plan_cuotas
	,CAST(PAYMENT_LINE.Card_Authorization_Num AS VARCHAR(10)) AS codigo_autoriz
	,CAST(NULL AS VARCHAR(10)) AS lote_trans_nro
	,CAST(PAYMENT_LINE.Sales_Transaction_Num AS VARCHAR(10)) AS ticket_nro
	,CAST(NULL AS  VARCHAR(10)) AS log_nro
	,CAST(NULL AS VARCHAR(08)) AS tj_hora
	,CAST(NULL AS DOUBLE) AS  tj_tiempo
	,CAST(CASE WHEN SUBSTR(PAYMENT_LINE.Card_Num, 1, 4) = '****' 
		THEN (PAYMENT_LINE.Payment_Line_BIN_Cd || PAYMENT_LINE.Card_num)
		ELSE PAYMENT_LINE.Card_Num
	    END AS VARCHAR(50)) AS tj_tarjeta_nro
	,CAST(NULL AS VARCHAR(10)) AS tj_fecha_expiracion
	,CAST(NULL AS VARCHAR(20)) AS tj_nro_comercio
	,CAST('A'  AS VARCHAR(01)) AS tj_modo_ingreso
	,CAST('COP' AS VARCHAR(10)) AS tj_moneda_transaccion_cd
	,CAST(NULL AS DOUBLE) AS tj_tasa_recargo_pct
	,CAST(NULL AS DOUBLE) AS tj_recargo_mnt
	,CAST(ASSOCIATE.Party_Identification_Num AS VARCHAR(20)) AS nro_identificador_supervisor
	,CAST(date_format(localtimestamp, '%y%m%d%H%i') AS VARCHAR(10)) AS lote_sec_nro
    ,CAST(PAYMENT_LINE.Business_Dt AS VARCHAR(10)) AS fecha_contable
    ,CAST(PAYMENT_LINE.Location_Host_CD AS VARCHAR(10)) AS centro_cd
FROM cencosud_desa1_datalake_sm_col_teradata.PAYMENT_LINE
LEFT JOIN 
     (SELECT  Sales_Tran_ID,
              Manager_Associate_ID,
              Change_Amt,
              TipoTrans_Ind,
	          business_dt
	     FROM cencosud_desa1_datalake_sm_col_teradata.SALES_TRANSACTION
	     where SALES_TRANSACTION.Change_Amt NOT IN('0','')
		   and (CAST(SALES_TRANSACTION.business_dt AS VARCHAR(10)) BETWEEN '${BEGIN_DATE}' AND '${END_DATE}')
		 ) SALES_TRANSACTION 
	    ON(PAYMENT_LINE.Sales_Tran_ID = SALES_TRANSACTION.Sales_Tran_ID)
     INNER JOIN cencosud_desa1_datalake_sm_col_teradata.PAYMENT_TYPE USING(Payment_Type_Id)
     INNER JOIN cencosud_desa1_datalake_sm_col_teradata.ASSOCIATE ON(SALES_TRANSACTION.Manager_Associate_ID = ASSOCIATE.Party_ID)
     INNER JOIN cencosud_desa1_datalake_sm_col_teradata.BANK_IDENTIFICATION_NUMBER 	ON(PAYMENT_LINE.Payment_Line_BIN_Cd = BANK_IDENTIFICATION_NUMBER.BIN_Cd)
WHERE (CAST(PAYMENT_LINE.business_dt    AS VARCHAR(10)) BETWEEN '${BEGIN_DATE}' AND '${END_DATE}')
UNION
SELECT CAST(PAYMENT_LINE.Pos_Register_Host_CD AS VARCHAR(10)) AS nro_caja
	,CAST(PAYMENT_LINE.Sales_Transaction_Num AS VARCHAR(20)) AS transaccion_nro
    ,CAST(PAYMENT_LINE.Tran_Start_Dt AS VARCHAR(10)) AS fecha
	,CAST(Substr(PAYMENT_LINE.tran_start_dttm,12,8) AS VARCHAR(8)) AS hora 
    ,CAST(PAYMENT_LINE.Payment_Tran_Id AS VARCHAR(10)) AS linea_nro
	,'P' AS tipo_operacion
	,CAST(PAYMENT_TYPE.Payment_Type_Host_Cd AS VARCHAR(10)) AS forma_pago_cd
	,CAST(BANK_IDENTIFICATION_NUMBER.Card_Association_Type_Cd AS VARCHAR(10)) AS variedad_pago_cd
    ,CAST(CASE
          WHEN PAYMENT_LINE.Payment_Amt in('',null) then '0'  
          ELSE PAYMENT_LINE.Payment_Amt 
          END AS DOUBLE) AS total_mnt
	,NULL AS monto_donacion
	,NULL AS tipo_instit_donacion
	,NULL AS institu_donacion_nro
	,'COP' AS moneda_pos_cd
	,NULL AS tarifa_cambio
	,CAST(CASE
          WHEN PAYMENT_LINE.Payment_Amt in('',null) then '0'  
          ELSE PAYMENT_LINE.Payment_Amt 
          END AS DOUBLE)  AS monto_moneda_orginal
  	,NULL AS ch_tipo_operacion
	,NULL AS ch_fecha_emision
	,NULL AS ch_fecha_cobro
	,NULL AS ch_banco_nro
	,NULL AS ch_sucursal_nro
	,NULL AS ch_ctacte_nro
	,NULL AS ch_cheque_nro
	,NULL AS ch_tipo_identifi_emisor
	,NULL AS ch_identifi_emisor_nro
	,NULL AS cta_cte_nro
	,NULL AS cta_cte_exten_nro
	,NULL AS tipo_identifi_cliente_ctacte
	,NULL AS identifi_cliente_ctacte_nro
	,CAST(BANK_IDENTIFICATION_NUMBER.Card_Association_Type_Cd AS VARCHAR(20)) AS tj_marca_cd
	,CAST(SALES_TRANSACTION.TipoTrans_Ind AS VARCHAR(10)) AS tj_tipo_operacion
	,CAST(PAYMENT_LINE.Card_Quotas_Qty AS VARCHAR(10)) AS tj_cuotas
	,NULL AS tj_plan_cuotas
	,CAST(PAYMENT_LINE.Card_Authorization_Num AS VARCHAR(10)) AS codigo_autoriz
	,NULL AS lote_trans_nro
	,CAST(PAYMENT_LINE.Sales_Transaction_Num AS VARCHAR(10)) AS ticket_nro
	,NULL AS log_nro
	,NULL AS tj_hora
	,NULL AS tj_tiempo
	,CAST(CASE WHEN SUBSTR(PAYMENT_LINE.Card_Num, 1, 4) = '****' 
		THEN (PAYMENT_LINE.Payment_Line_BIN_Cd || PAYMENT_LINE.Card_num)
		ELSE PAYMENT_LINE.Card_Num
	 END AS VARCHAR(50)) AS tj_tarjeta_nro
	,NULL AS tj_fecha_expiracion
	,NULL AS tj_nro_comercio
	,'A'  AS tj_modo_ingreso
	,'COP' AS tj_moneda_transaccion_cd
	,NULL AS tj_tasa_recargo_pct
	,NULL AS tj_recargo_mnt
	,CAST(ASSOCIATE.Party_Identification_Num AS VARCHAR(20)) AS nro_identificador_supervisor
	,CAST(date_format(localtimestamp, '%y%m%d%H%i') AS VARCHAR(10)) AS lote_sec_nro
    ,CAST(PAYMENT_LINE.Business_Dt AS VARCHAR(10)) AS fecha_contable
    ,CAST(PAYMENT_LINE.Location_Host_CD AS VARCHAR(10)) AS centro_cd
FROM cencosud_desa1_datalake_sm_col_teradata.PAYMENT_LINE
LEFT JOIN 
     (SELECT  distinct Sales_Tran_ID,
              Manager_Associate_ID,
              Change_Amt,
              TipoTrans_Ind,
	          business_dt
	     FROM cencosud_desa1_datalake_sm_col_teradata.SALES_TRANSACTION
	     where (CAST(SALES_TRANSACTION.business_dt AS VARCHAR(10)) BETWEEN '${BEGIN_DATE}' AND '${END_DATE}')
		 ) SALES_TRANSACTION 
	    ON(PAYMENT_LINE.Sales_Tran_ID = SALES_TRANSACTION.Sales_Tran_ID)
     INNER JOIN cencosud_desa1_datalake_sm_col_teradata.PAYMENT_TYPE USING(Payment_Type_Id)
     INNER JOIN cencosud_desa1_datalake_sm_col_teradata.ASSOCIATE ON(SALES_TRANSACTION.Manager_Associate_ID = ASSOCIATE.Party_ID)
     INNER JOIN cencosud_desa1_datalake_sm_col_teradata.BANK_IDENTIFICATION_NUMBER 	ON(PAYMENT_LINE.Payment_Line_BIN_Cd = BANK_IDENTIFICATION_NUMBER.BIN_Cd)
WHERE (CAST(PAYMENT_LINE.business_dt AS VARCHAR(10)) BETWEEN '${BEGIN_DATE}' AND '${END_DATE}')) Tab;

exit;
EOF

## log
echo "${BEGIN_DATE}" >> VTA_PAG.log

exit $?
