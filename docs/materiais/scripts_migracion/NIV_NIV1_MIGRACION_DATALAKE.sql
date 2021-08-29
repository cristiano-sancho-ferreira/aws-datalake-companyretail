Insert into cencosud_desa1_datalake_sm_col_migracion.niv_niv1
		(operacion_cd 
		,secuencial 
		,merc_nivel1_cd 
		,fecha_novedad 
		,merc_nivel1_desc 
		,merc_nivel0_cd) 	

Select Distinct Tab.operacion_cd 
	  ,Tab.secuencial 
	  ,Tab.merc_nivel1_cd 
	  ,Tab.fecha_novedad 
	  ,Tab.merc_nivel1_desc 
      ,Tab.merc_nivel0_cd 		
		
From(select 'M' as operacion_cd 
	   ,'0' as secuencial
       ,cast(division.division_cd as varchar(16)) as merc_nivel1_cd
       ,'1900-01-01' as fecha_novedad
       ,Cast(division.division_name as varchar(100)) as merc_nivel1_desc
       ,Cast(division.group_cd as varchar(16)) as merc_nivel0_cd 
from  cencosud_desa1_datalake_sm_col_teradata.division) Tab
Where Not Exists ( Select 1 from cencosud_desa1_datalake_sm_col.niv_niv1 B   
                      Where B.operacion_cd     = Tab.operacion_cd   
						And B.secuencial       = Tab.secuencial 
						And B.merc_nivel1_cd   = Tab.merc_nivel1_cd
						And	B.merc_nivel1_desc = Tab.merc_nivel1_desc
						And B.merc_nivel0_cd   = Tab.merc_nivel0_cd)
                        

 
  
  
 