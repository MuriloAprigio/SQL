# Databricks notebook source
# MAGIC %run 
# MAGIC /Repos/lakehouse/ProjetoLakehouse/0_feature/nb_globais_definicoes

# COMMAND ----------

# MAGIC %run 
# MAGIC
# MAGIC /Repos/lakehouse/ProjetoLakehouse/0_feature/nb_globais_funcoes

# COMMAND ----------

# DBTITLE 1,CRIAÇÃO DDL
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS `3_prd_sandbox`.`engenharia_dados`.dim_produto
# MAGIC (
# MAGIC   	SK_PRODUTO                      	  string, 
# MAGIC 	PK_PRODUTO                      	  string,                    	
# MAGIC 	COD_PRODUTO                     	  string,
# MAGIC 	DSC_PRODUTO					    	          string,
# MAGIC 	DSC_TIPO_PRODUTO                  	string,
# MAGIC 	DSC_UNIDADE_MEDIDA_ESTOQUE         	STRING,
# MAGIC 	COD_MATERIAL_GENERICO               string,
# MAGIC 	COD_MATERIAL_ESTOQUE				        STRING,             				                             
# MAGIC 	COD_PROTHEUS                        string,
# MAGIC 	COD_SISTEMA_ORIGEM                 	string,
# MAGIC 	NOM_SANEADO                        	string,
# MAGIC 	DSC_PRINCIIO_ATIVO                 	string,
# MAGIC   DSC_SISTEMA_ORIGEM 								  string, --NOME ORIGEM
# MAGIC 	DAT_CARGA                           timestamp
# MAGIC
# MAGIC );

# COMMAND ----------

# DBTITLE 1,PRODUTO
sql_produto_final = f"""
WITH PRODUTO_FINAL AS 
(
SELECT 
    A.PK_PRODUTO AS SK_PRODUTO,
    A.PK_PRODUTO AS PK_PRODUTO,			
    A.COD_PRODUTO AS COD_PRODUTO,				
    A.DSC_PRODUTO AS DSC_PRODUTO,				
    A.DSC_TIPO_PRODUTO AS DSC_TIPO_PRODUTO,
    CASE 
        WHEN A.DSC_UNIDADE_MEDIDA_ESTOQUE IS NULL OR A.DSC_UNIDADE_MEDIDA_ESTOQUE = '' THEN '-1'
        ELSE A.DSC_UNIDADE_MEDIDA_ESTOQUE 
    END  AS DSC_UNIDADE_MEDIDA_ESTOQUE,
    CASE 
        WHEN A.COD_MATERIAL_GENERICO IS NULL OR A.COD_MATERIAL_GENERICO = '' THEN '-1'
        ELSE A.COD_MATERIAL_GENERICO 
    END AS COD_MATERIAL_GENERICO,
    CASE 
        WHEN A.COD_MATERIAL_ESTOQUE IS NULL OR A.COD_MATERIAL_ESTOQUE = '' THEN '-1'
        ELSE A.COD_MATERIAL_ESTOQUE 
    END AS COD_MATERIAL_ESTOQUE,
    CAST(CASE LTRIM(RTRIM(A.COD_PROTHEUS))
        WHEN '-1' THEN 'NAO IDENTIFICADO'
        WHEN '-2' THEN 'NAO SE APLICA'
        WHEN '-3' THEN 'INDETERMINADO'
        WHEN '-4' THEN 'NAO EXISTE'
        ELSE LTRIM(RTRIM(A.COD_PROTHEUS))
    END AS STRING) AS COD_PROTHEUS,
    CAST(LTRIM(RTRIM(A.COD_SISTEMA_ORIGEM)) AS STRING) AS COD_SISTEMA_ORIGEM,
    CAST(CASE LTRIM(RTRIM(COALESCE(VIA.PRODUTO, A.NOM_SANEADO)))
        WHEN '-1' THEN 'NAO IDENTIFICADO'
        WHEN '-2' THEN 'NAO SE APLICA'
        WHEN '-3' THEN 'INDETERMINADO'
        WHEN '-4' THEN 'NAO EXISTE'
        ELSE LTRIM(RTRIM(COALESCE(VIA.PRODUTO, A.NOM_SANEADO)))
    END AS STRING) AS NOM_SANEADO,
    CAST(CASE LTRIM(RTRIM(A.DSC_PRINCIPIO_ATIVO))
        WHEN '-1' THEN 'NAO IDENTIFICADO'
        WHEN '-2' THEN 'NAO SE APLICA'
        WHEN '-3' THEN 'INDETERMINADO'
        WHEN '-4' THEN 'NAO EXISTE'
        ELSE LTRIM(RTRIM(A.DSC_PRINCIPIO_ATIVO))
    END AS STRING) AS DSC_PRINCIPIO_ATIVO,
    A.ORIGEM AS DSC_SISTEMA_ORIGEM,
    DATEADD(HOUR, -3, current_timestamp()) AS DAT_CARGA			
FROM `3_prd_sandbox`.engenharia_dados.tbl_tru_pe_produto A
LEFT JOIN (
    SELECT
        CAST(RIGHT('000000' + COD_PROTHEUS, 6) AS STRING) AS COD_PROTHEUS,
        MAX(TP_VIA_ADMINISTRACAO) AS TP_VIA_ADMINISTRACAO,
        MAX(TIPO_PRODUTO) AS TIPO_PRODUTO,
        MAX(CLASSIFICACAO_INTERNA) AS CLASSIFICACAO_INTERNA,
        MAX(GRP_CLASSE_TERAPEUTICA) AS GRP_CLASSE_TERAPEUTICA,
        MAX(SUBSTANCIA) AS SUBSTANCIA,
        MAX(PRODUTO) AS PRODUTO
    FROM `{uc_ingestao}`.`1_raw_sharepoint`.lista_medicamentos
    WHERE COD_PROTHEUS IS NOT NULL
    GROUP BY COD_PROTHEUS
) AS VIA
ON A.COD_PROTHEUS = VIA.COD_PROTHEUS
)
"""
df_produto_final = spark.sql(sql_produto_final)
df_produto_final.createOrReplaceTempView("vw_produto_final")


# COMMAND ----------

# DBTITLE 1,DELETE
sql_delete = f"""
DELETE FROM `3_prd_sandbox`.`engenharia_dados`.dim_produto
"""
df_insert = spark.sql(sql_insert)

# COMMAND ----------

# DBTITLE 1,INSERT
sql_insert = f"""
INSERT INTO `3_prd_sandbox`.`engenharia_dados`.dim_produto
SELECT 			
 SK_PRODUTO		
,PK_PRODUTO		
,COD_PRODUTO	
,DSC_PRODUTO				
,DSC_TIPO_PRODUTO
,DSC_UNIDADE_MEDIDA_ESTOQUE
,COD_MATERIAL_GENERICO
,COD_MATERIAL_ESTOQUE
,COD_PROTHEUS
,COD_SISTEMA_ORIGEM
,NOM_SANEADO
,DSC_PRINCIPIO_ATIVO				
,DSC_SISTEMA_ORIGEM
,DAT_CARGA				
FROM PRODUTO_FINAL
    """

df_insert = spark.sql(sql_insert)