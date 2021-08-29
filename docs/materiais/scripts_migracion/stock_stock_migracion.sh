###########################################################################################
## SHELL      : stock_stock_migracion.sh
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

BEGIN_DATE=$1
END_DATE=$2

presto-cli --catalog hive  <<EOF
   
Insert into cencosud_desa1_datalake_sm_col_migracion.stock_stock(
 articulo_cd 
,almacen_cd 
,clave_moneda_cd 
,mborrado_articulo_cd 
,control_precio_ind 
,p_variable_interno_cd 
,precio_estandar_cd 
,stock_total_valorado_qty 
,stock_total_valorado_mnt 
,hora_stock 
,codigo_barras_cd 
,estado_de_stock 
,estado_articulo_tienda 
,fecha_ultima_compra 
,fecha_ultima_venta 
,fecha_ultima_modificacion 
,unidades_pendientes 
,proveedor_cd 
,prov_division_cd 
,proveedor_ultima_compra_ext 
,fecha_ultima_compra_externo 
,estado_arti_cd 
,concesion_ind 
,consignacion_ind 
,importado_ind 
,marca_propia 
,precio_vta_unit_vig_mnt 
,venta_media 
,fecha_stock 
,centro_cd 
)
Select Distinct  
  Tab.articulo_cd 
,Tab.almacen_cd 
,Tab.clave_moneda_cd
,Tab.mborrado_articulo_cd 
,Tab.control_precio_ind 
,Tab.p_variable_interno_cd 
,Tab.precio_estandar_cd 
,Tab.stock_total_valorado_qty 
,Tab.stock_total_valorado_mnt 
,Tab.hora_stock 
,Tab.codigo_barras_cd 
,Tab.estado_de_stock 
,Tab.estado_articulo_tienda 
,Tab.fecha_ultima_compra 
,Tab.fecha_ultima_venta 
,Tab.fecha_ultima_modificacion 
,Tab.unidades_pendientes 
,Tab.proveedor_cd 
,Tab.prov_division_cd 
,Tab.proveedor_ultima_compra_ext 
,Tab.fecha_ultima_compra_externo 
,Tab.estado_arti_cd 
,Tab.concesion_ind 
,Tab.consignacion_ind 
,Tab.importado_ind 
,Tab.marca_propia 
,Tab.precio_vta_unit_vig_mnt 
,Tab.venta_media 
,Tab.fecha_stock 
,Tab.centro_cd 
From( 
Select  Cast(ITEM.Item_SKU_num as varchar(18)) as articulo_cd
		,Cast('-1' as varchar(4)) as almacen_cd
		,Cast('COP' as varchar(5)) as clave_moneda_cd
		,'N' as mborrado_articulo_cd
		,'N' as control_precio_ind
		,Cast(0 as double) as p_variable_interno_cd
		,CASE WHEN ITEM_INVENTORY.On_Hand_Unit_Qty IN ('', null) THEN 0
				WHEN ITEM_INVENTORY.On_Hand_Value_Amt_DD IN ('') THEN 0
				ELSE CAST(ROUND(CAST(ITEM_INVENTORY.On_Hand_Value_Amt_DD AS DOUBLE)/CAST(ITEM_INVENTORY.On_Hand_Unit_Qty AS DOUBLE), 4) * 10000 AS double) / 10000
			END AS precio_estandar_cd
		,Cast(case when ITEM_INVENTORY.On_Hand_Unit_Qty in ('', null) then '0' 
				   else ITEM_INVENTORY.On_Hand_Unit_Qty 
			   end as double) as stock_total_valorado_qty
		,Cast(case when ITEM_INVENTORY.On_Hand_Value_Amt_DD in ('', null) then '0' 
				   else ITEM_INVENTORY.On_Hand_Value_Amt_DD 
			   end as double) as stock_total_valorado_mnt
		,'00:00:00' as hora_stock
		,Cast(ITEM.Item_EAN_num as varchar(18)) as codigo_barras_cd
		,'D' as estado_de_stock
		,Cast(ITEM_INVENTORY.Item_Status_Cd as varchar(50)) as estado_articulo_tienda
		,Cast(ITEM_LOCATOR_MOVEMENT.Last_Purchase_Dt as varchar(10)) as fecha_ultima_compra
		,Cast(ITEM_LOCATOR_MOVEMENT.Last_Sale_Dt as varchar(10)) as fecha_ultima_venta
		,Cast(ITEM_LOCATOR_MOVEMENT.Last_Movement_Dt as varchar(10)) as fecha_ultima_modificacion
		,Cast(case when ITEM_INVENTORY.On_Hold_Unit_Qty in ('', null) then '0' 
				   else ITEM_INVENTORY.On_Hold_Unit_Qty 
			   end as double) as unidades_pendientes
		,Cast(VENDOR.vendor_party_Org_Host_cd as varchar(10)) as proveedor_cd
		,Cast(VENDOR.vendor_party_host_cd as varchar(20)) as prov_division_cd
		,Cast(VENDOR.vendor_party_host_cd as varchar(20)) as proveedor_ultima_compra_ext
		,Cast(ITEM_LOCATOR_MOVEMENT.Last_Purchase_Dt as varchar(10)) as fecha_ultima_compra_externo
		,Cast('-1' as varchar(4)) as estado_arti_cd
		,Cast('-1' as varchar(3)) as concesion_ind
		,Cast('-1' as varchar(3)) as consignacion_ind
		,Cast('-1' as varchar(3)) as importado_ind
		,'N' as marca_propia
		,Cast(-1 as double) as precio_vta_unit_vig_mnt
		,Cast(-1 as double) as venta_media
		,Cast(ITEM_INVENTORY.Item_Inv_Dt as varchar(10)) as fecha_stock
		,Cast(LOCATION.Location_host_cd as varchar(10)) as centro_cd
   from cencosud_desa1_datalake_sm_col_teradata.ITEM_INVENTORY
  inner Join cencosud_desa1_datalake_sm_col_teradata.ITEM_LOCATOR_MOVEMENT 
		  on (ITEM_LOCATOR_MOVEMENT.Location_Id      = ITEM_INVENTORY.Location_id
		  and ITEM_LOCATOR_MOVEMENT.Item_id          = ITEM_INVENTORY.item_id
		  and ITEM_LOCATOR_MOVEMENT.item_movement_dt = ITEM_INVENTORY.item_inv_dt)
  inner join cencosud_desa1_datalake_sm_col_teradata.ITEM on (ITEM_INVENTORY.Item_Id = ITEM.Item_id)
  inner join cencosud_desa1_datalake_sm_col_teradata.LOCATION on (ITEM_INVENTORY.Location_Id = LOCATION.Location_id)
  inner Join cencosud_desa1_datalake_sm_col_teradata.VENDOR On(ITEM_INVENTORY.vendor_party_id = VENDOR.vendor_party_id)  
  Where Cast(ITEM_INVENTORY.item_inv_dt as varchar(10)) Between  '${BEGIN_DATE}' And '${END_DATE}'
    And Cast(ITEM_LOCATOR_MOVEMENT.item_movement_dt as varchar(10)) Between  '${BEGIN_DATE}' and '${END_DATE}') Tab;

exit;
EOF

## log
echo "${BEGIN_DATE}" >> stock_stock.log

exit $?
