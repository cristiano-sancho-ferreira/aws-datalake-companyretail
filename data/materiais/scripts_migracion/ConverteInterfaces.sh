###########################################################################################
## SHELL      : ConverteInterfaces.sh
## Autor      : Alam(ST IT Consulting)
## Finalidad  : Script genérico de carga historica para formato Parquet e las interfaces de origene.
## Parâmetros : Não Há
## Retorno    : 0 - OK
##              9 - NOK
## Historia   : Fecha     | Descripción
##              ----------|-----------------------------------------------------------------
##              15/08/2018| Código inicial
###########################################################################################
NBARG=$#   
## Tratamiento de los parâmetros 
rm -f finish.log
echo "Inicio de ejecución $(date)" > start.log
##
FILE_PARAMETERS=./parameters_extract.ctl ## Archivo con la lista de extracciones que se ejecutan
##
## Archivo de parâmetros->FILE_PARAMETERS
##
## Parâmetros : EXTRACTION_SHELL_FILE - Nombre Script Shell con Query de la tabla Data Lake.
##              BEGIN_DATE - La fecha Inicio de processo
##              END_DATE - La fecha Fim de processo
## Ejemplo de un registro de archivo de parâmetros:
##
## EXTRACTION_SHELL_FILE=VTA_PROD_MIGRACION.sh;BEGIN_DATE=2016-05-01;END_DATE=2016-05-31;
##
## |                                        |                              |                     |  
##Parametro1-EXTRACTION_SHELL_FILE ;Parametro2 BEGIN_DATE;Parametro3 END_DATE
##
FILE_COMPLETED=./completed_extract.ctl ## Archivo con la lista de extracciones ya ejecutadas y finalizadas
##
## Completed Extracion->FILE_COMPLETED
## Archivo con la lista de extracciones ya ejecutadas y finalizadas, 
##
if [ ! -f ${FILE_COMPLETED} ] 
then
	echo > ${FILE_COMPLETED}
fi

## Variable returne code global
declare -i RC_GLOBAL=0

## 
## Loop con la lista de las ejecuciones que no finalizaron. El siguiente comando lee en el archivo con la lista de parâmetro que se deben ejecutar 
## y elimina la lista de parâmetros que ya se han finalizado
##
diff -n ${FILE_COMPLETED} ${FILE_PARAMETERS} |grep EXTRACTION_SHELL_FILE| while read V_Loop
do
echo $V_Loop |awk '{ a=split( $0, 'G' , ";"); for ( i in G ) if (i == 2) {print G[1]; print G[i];} else { if ( i !=1 ) {print G[i];}}}'| while read V_PAR 
do
    ##
    ## Recupera los valores de los parâmetro a ejecutar y almacena en las variables de trabajo
	##
    v_ParName=`echo $V_PAR | awk -F "=" '{ print $1 }'`
    v_ParVal=`echo $V_PAR | awk -F "=" '{ print $2 }'`
    case "$v_ParName" in 
       "EXTRACTION_SHELL_FILE")
	  export EXTRACTION_SHELL_FILE=${v_ParVal};;
       "BEGIN_DATE")
	  export BEGIN_DATE=${v_ParVal};;
       "END_DATE")
	  export END_DATE=${v_ParVal};;
    esac
done
		
    ## Ejecuta la exportacíon del archivo utilizando shell de presto
	
	echo "-${EXTRACTION_SHELL_FILE} -${RC_GLOBAL} -${BEGIN_DATE} -${END_DATE}"
	
	./${EXTRACTION_SHELL_FILE} ${BEGIN_DATE} ${END_DATE} > file_output.out;
	RC=$?
    if [ $RC -ne 0 ]
    then
	   echo ${V_Loop} >> ./e_extract.ctl
       vMessage="Erro en ejecuta la carga del archivo utilizando presto EMR"
       RC_GLOBAL=9
    fi
	##
	echo ${V_Loop} >> ${FILE_COMPLETED}
	
done
echo "Fin de ejecuciones $(date)" > finish.log
echo ${vMessage} >> finish.log
echo "RC_GLOBAL=${RC_GLOBAL}" >> finish.log
echo $RC_GLOBAL