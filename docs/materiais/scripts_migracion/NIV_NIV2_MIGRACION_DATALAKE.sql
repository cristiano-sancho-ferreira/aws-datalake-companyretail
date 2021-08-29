insert  into cencosud_desa1_datalake_sm_col_migracion.niv_niv2
		(operacion_cd 
		,secuencial 
		,merc_nivel2_cd 
		,fecha_novedad 
		,merc_nivel2_desc
		,merc_nivel1_cd) 		

Select  Distinct Tab.operacion_cd 
		,Tab.secuencial 
		,Tab.merc_nivel2_cd 
		,Tab.fecha_novedad 
		,Tab.merc_nivel2_desc
		,Tab.merc_nivel1_cd
From(select 'M'  as operacion_cd
	   ,'0' as secuencial 
	   ,Cast(Substr(department.department_cd,1,10) as varchar(10)) as  merc_nivel2_cd
	   ,'1900-01-01' as fecha_novedad
	   ,Cast(department.department_name as varchar(100)) as merc_nivel2_desc
	   ,Cast(Division.division_cd as varchar(16)) as merc_nivel1_cd
     From  cencosud_desa1_datalake_sm_col_Teradata.Department
Inner Join cencosud_desa1_datalake_sm_col_Teradata.Division On(Division.division_cd = Department.division_cd)) Tab 
Where Not Exists ( Select 1 from cencosud_desa1_datalake_sm_col.niv_niv2 B   
                      Where B.operacion_cd     = Tab.operacion_cd   
						And B.secuencial       = Tab.secuencial 
						And B.merc_nivel2_cd   = Tab.merc_nivel2_cd
						And	B.merc_nivel2_desc = Tab.merc_nivel2_desc
						And B.merc_nivel1_cd   = Tab.merc_nivel1_cd)
                        
                        


 
 
 
  
  
 