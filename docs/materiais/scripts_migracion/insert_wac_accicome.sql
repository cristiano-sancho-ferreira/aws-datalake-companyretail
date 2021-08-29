Insert into cencosud_desa1_datalake_sm_col_migracion.wac_accicome
(codigoaccioncomercial 
,nombreaccioncomercial 
,costomarketing 
,codigotipoaccioncomer 
,numeropaginastotales 
,numeroarticulos 
,fechainicio 
,fechafin 
,codigosistemafuente) 

Select Distinct
Tab.codigoaccioncomercial 
,Tab.nombreaccioncomercial 
,Tab.costomarketing 
,Tab.codigotipoaccioncomer 
,Tab.numeropaginastotales 
,Tab.numeroarticulos 
,Tab.fechainicio 
,Tab.fechafin 
,Tab.codigosistemafuente
From(Select  
Cast(COMMERCIAL_ACTION.Commercial_Action_Host_Cd as varchar(20)) as codigoaccioncomercial
,Cast(Substr(COMMERCIAL_ACTION.Commercial_Action_Desc,1,100) as varchar(100)) as nombreaccioncomercial
,Cast(Case 
     When COMMERCIAL_ACTION.Cost_Amt in('',null) Then '0'
     Else COMMERCIAL_ACTION.Cost_Amt   
     End as Double) as costomarketing 
,Cast(COMMERCIAL_ACTION.Commercial_Action_Type_Cd as Varchar(3)) as codigotipoaccioncomer
,Cast(COMMERCIAL_ACTION.Page_Cnt as integer) as numeropaginastotales
,Cast(COMMERCIAL_ACTION.Item_Cnt as Integer) as numeroarticulos
,Cast(COMMERCIAL_ACTION.Commercial_Action_Start_Dt as varchar(10)) as fechainicio 
,Cast(COMMERCIAL_ACTION.Commercial_Action_End_Dt as varchar(10)) as fechafin
,Cast(COMMERCIAL_ACTION.Boss_Application_Cd as varchar(3)) as codigosistemafuente
From cencosud_desa1_datalake_sm_col_teradata.COMMERCIAL_ACTION ) Tab
