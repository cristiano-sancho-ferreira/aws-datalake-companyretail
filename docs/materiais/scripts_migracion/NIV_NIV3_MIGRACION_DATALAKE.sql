Insert into cencosud_desa1_datalake_sm_col_migracion.niv_niv3
		(operacion_cd 
		,secuencial 
		,merc_nivel3_cd 
		,fecha_novedad 
		,merc_nivel3_desc 
		,merc_nivel2_cd) 

Select	Distinct Tab.operacion_cd 
		,Tab.secuencial 
		,Tab.merc_nivel3_cd 
		,Tab.fecha_novedad 
		,Tab.merc_nivel3_desc 
		,Tab.merc_nivel2_cd		
From( Select  'M' as operacion_cd
              ,'0' as secuencial
              ,Cast(Substr(item_class.item_class_cd,1,16) as varchar(16)) as merc_nivel3_cd 
              ,'1900-01-01' as fecha_novedad  
              ,Cast(item_class.item_class_name as varchar(100)) as merc_nivel3_desc 
              ,Cast(item_class.department_cd as varchar(16)) as merc_nivel2_cd  
        From  cencosud_desa1_datalake_sm_col_teradata.item_class) Tab
Where Not Exists (Select 1 From cencosud_desa1_datalake_sm_col.niv_niv3 B     
                  Where B.operacion_cd	    = Tab.operacion_cd 
					And B.secuencial	    = Tab.secuencial 
					And B.merc_nivel3_cd	= Tab.merc_nivel3_cd 
					And B.merc_nivel3_desc	= Tab.merc_nivel3_desc 
					And B.merc_nivel2_cd	= Tab.merc_nivel2_cd)								  
						
						

