###########################################################################################
## SHELL      : cpp_cpp_migracion.sh
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
   
Insert into cencosud_desa1_datalake_sm_col_migracion.cpp_cpp (
articulo_cd
,costo_cpp
,codigo_barras_cd
,fecha_inc_validez
,centro_cd
)
Select Tab.articulo_cd
       ,Tab.costo_cpp
       ,Tab.codigo_barras_cd 
       ,Tab.fecha_inc_validez
       ,Tab.centro_cd
From (Select Distinct 
            Cast(ITEM.Item_SKU_num as varchar(20)) as articulo_cd 
            ,Cast(Case
                when ITEM_COST_DETAIL.Allocated_cost_amt in('',null) Then '0' 
                Else ITEM_COST_DETAIL.Allocated_cost_amt
                End as double) as costo_cpp
            ,Cast(ITEM.Item_EAN_num as varchar(18)) as codigo_barras_cd
            ,Cast(ITEM_COST_DETAIL.item_cost_start_dt as varchar(10)) as fecha_inc_validez
            ,Cast(LOCATION.Location_Host_cd as varchar(10)) as centro_cd
        From cencosud_desa1_datalake_sm_col_teradata.ITEM_COST_DETAIL
        Join cencosud_desa1_datalake_sm_col_teradata.ITEM on (ITEM_COST_DETAIL.Item_Id = ITEM.Item_id)
        Join cencosud_desa1_datalake_sm_col_teradata.LOCATION on (ITEM_COST_DETAIL.Location_Id = LOCATION.Location_id)
		Where Cast(ITEM_COST_DETAIL.item_cost_start_dt as varchar(10)) BETWEEN '${BEGIN_DATE}' and '${END_DATE}') Tab;

exit;
EOF

## log
echo "${BEGIN_DATE}" >> cpp_cpp.log

exit $?
