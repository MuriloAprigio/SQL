# Databricks notebook source
# MAGIC %run 
# MAGIC /Repos/lakehouse/ProjetoLakehouse/0_feature/nb_globais_definicoes

# COMMAND ----------

# MAGIC %run 
# MAGIC /Repos/lakehouse/ProjetoLakehouse/0_feature/nb_globais_funcoes

# COMMAND ----------

sistema_origem = 'PAEEON_NEO'

#uc_lakehouse = '/Repos/lakehouse/ProjetoLakehouse/0_feature/nb_globais_definicoes'

#uc_ingestao = '/Repos/lakehouse/ProjetoLakehouse/0_feature/nb_globais_definicoes'

# COMMAND ----------

# DBTITLE 1,CRIAÇÃO DDL
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS `3_prd_sandbox`.engenharia_dados.tbl_tru_pe_produto
# MAGIC (
# MAGIC   
# MAGIC 	PK_PRODUTO                      	string,                    	
# MAGIC 	COD_PRODUTO                     	string,
# MAGIC 	DSC_PRODUTO					    	string,
# MAGIC 	DSC_TIPO_PRODUTO                  	string,
# MAGIC 	DSC_UNIDADE_MEDIDA_ESTOQUE         	STRING,
# MAGIC 	COD_MATERIAL_GENERICO               string,
# MAGIC 	COD_MATERIAL_ESTOQUE				STRING,             				                             
# MAGIC 	COD_PROTHEUS                        string,
# MAGIC 	COD_SISTEMA_ORIGEM                 	string,
# MAGIC 	NOM_SANEADO                        	string,
# MAGIC 	DSC_PRINCIIO_ATIVO                 	string,
# MAGIC 	ORIGEM 								string,
# MAGIC 	DAT_CARGA                           timestamp
# MAGIC
# MAGIC );

# COMMAND ----------

# DBTITLE 1,TMP_MAT_MED
sql_tmp_tbl_matmed = f"""
SELECT
CASE
    WHEN MATERIAL.PD_COD  IS NULL OR MATERIAL.ORIGEM IS NULL THEN NULL
    ELSE CAST(MATERIAL.PD_COD AS STRING) ||CAST(MATERIAL.ORIGEM AS STRING)
  END AS PK_PRODUTO,
TRIM(CAST(MATERIAL.PD_COD AS VARCHAR(255))) AS COD_PRODUTO,
UPPER(TRIM(COALESCE(CAST(MATERIAL.PROD_DESCRICAO AS VARCHAR(4000)), '-1'))) AS DSC_PRODUTO,
'MATMED' AS DSC_TIPO_PRODUTO,
CASE 
    WHEN MATERIAL.PROD_DESC LIKE '%ML%' AND MATERIAL.PROD_DESC LIKE '%MG%' THEN 'ML/MG'
    WHEN MATERIAL.PROD_DESC LIKE '%ML%' THEN 'ML'
    WHEN MATERIAL.PROD_DESC LIKE '%MG%' THEN 'MG'
    ELSE '-1'
END AS DSC_UNIDADE_MEDIDA_ESTOQUE,
'-1' AS COD_MATERIAL_GENERICO,
'-1' AS COD_MATERIAL_ESTOQUE,
CAST(COALESCE(TRIM(TOTVS.b1_cod),'-1') AS VARCHAR(255)) AS COD_PROTHEUS,
TRIM(CAST(MATERIAL.PD_COD AS VARCHAR(255))) AS COD_SISTEMA_ORIGEM,
'-1' AS NOM_SANEADO,
CAST(UPPER(TRIM(COALESCE(VIA.SUBSTANCIA,'-1'))) AS VARCHAR(255)) AS DSC_PRINCIPIO_ATIVO,
'{sistema_origem}' as ORIGEM,
    DATEADD(HOUR, -3, current_timestamp()) as DAT_CARGA
FROM
    `4_cubo`.`99_sincronismo`.paeeon_neo_produto_detalhe_final MATERIAL 
    LEFT JOIN `4_cubo`.`1_raw`.tbl_raw_totvs_zu4u68_sb1010 TOTVS 
    ON RIGHT('000000' || CAST(MATERIAL.PD_COD_ONCOCLINICAS AS VARCHAR(255)), 6) = TOTVS.B1_COD
    AND TOTVS.B1_COD BETWEEN '010000' AND '099999'
    AND TOTVS.d_e_l_e_t_ != '*'
    LEFT JOIN  `{uc_ingestao}`.`1_raw_sharepoint`.lista_medicamentos  LISTA 
                    ON TOTVS.b1_cod=LISTA.COD_PROTHEUS
            LEFT  JOIN	(SELECT
                            RIGHT('000000' || CAST(COD_PROTHEUS AS VARCHAR(255)), 6) AS COD_PROTHEUS,
                            MAX(TP_VIA_ADMINISTRACAO) AS TP_VIA_ADMINISTRACAO,
                            MAX(TIPO_PRODUTO) AS TIPO_PRODUTO,
                            MAX(CLASSIFICACAO_INTERNA) AS CLASSIFICACAO_INTERNA,
                            MAX(GRP_CLASSE_TERAPEUTICA) AS GRP_CLASSE_TERAPEUTICA,
                            MAX(SUBSTANCIA) AS SUBSTANCIA
                        FROM
                             `{uc_ingestao}`.`1_raw_sharepoint`.lista_medicamentos  
                        WHERE
                            COD_PROTHEUS IS NOT NULL
                        GROUP BY
                            COD_PROTHEUS) AS VIA
                    ON	TOTVS.b1_cod=VIA.COD_PROTHEUS
"""
df_tmp_tbl_matmed = spark.sql(sql_tmp_tbl_matmed)
df_tmp_tbl_matmed.createOrReplaceTempView("vw_tmp_tbl_matmed")

# COMMAND ----------

# DBTITLE 1,PK_MATMED

sql_matmed = f"""
SELECT
CASE WHEN PK_PRODUTO IS NULL THEN NULL
ELSE oc_cria_sk(PK_PRODUTO) END AS PK_PRODUTO,
COD_PRODUTO,
DSC_PRODUTO,
DSC_TIPO_PRODUTO,
DSC_UNIDADE_MEDIDA_ESTOQUE,
COD_MATERIAL_GENERICO,
COD_MATERIAL_ESTOQUE,
COD_PROTHEUS,
COD_SISTEMA_ORIGEM,
NOM_SANEADO,
DSC_PRINCIPIO_ATIVO,
ORIGEM,
DAT_CARGA
FROM vw_tmp_tbl_matmed
"""
df_matmed = spark.sql(sql_matmed)
df_matmed.createOrReplaceTempView("vw_matmed")


# COMMAND ----------

# DBTITLE 1,TMP_PROCEDIMENTOS
sql_tmp_tbl_procedimentos = f"""
SELECT
  CASE
    WHEN PROCED.PROC_COD IS NULL OR PROCED.ORIGEM IS NULL THEN NULL
    ELSE CAST(PROCED.PROC_COD AS STRING) ||CAST(PROCED.ORIGEM AS STRING)
  END AS PK_PRODUTO,
  TRIM(CONCAT(CAST(PROCED.PROC_COD AS STRING),'-P')) AS COD_PRODUTO,
  UPPER(TRIM(IFNULL(CAST(PROCED.PROC_DESC AS STRING), '-1'))) AS DSC_PRODUTO,
  'PROCEDIMENTO' AS DSC_TIPO_PRODUTO,
  '-1' AS DSC_UNIDADE_MEDIDA_ESTOQUE,
  '-1' AS COD_MATERIAL_GENERICO, 
  '-1' AS COD_MATERIAL_ESTOQUE,
  '-1' AS COD_PROTHEUS,
  '-1' AS COD_SISTEMA_ORIGEM,
  '-1' AS NOM_SANEADO,
  '-1' AS DSC_PRINCIPIO_ATIVO, 
  '{sistema_origem}' as ORIGEM,
    DATEADD(HOUR, -3, current_timestamp()) as DAT_CARGA
FROM 
  `{uc_ingestao}`.`1_raw_paeeon_neo`.procedimentos PROCED
"""

df_tmp_tbl_procedimentos = spark.sql(sql_tmp_tbl_procedimentos)
df_tmp_tbl_procedimentos.createOrReplaceTempView("vw_tmp_tbl_procedimentos")

# COMMAND ----------

# DBTITLE 1,PK_PROCEDIMENTOS
sql_procedimentos = f"""
SELECT
CASE WHEN PK_PRODUTO IS NULL THEN NULL
ELSE oc_cria_sk(PK_PRODUTO) END AS PK_PRODUTO,
COD_PRODUTO,
DSC_PRODUTO,
DSC_TIPO_PRODUTO,
DSC_UNIDADE_MEDIDA_ESTOQUE,
COD_MATERIAL_GENERICO,
COD_MATERIAL_ESTOQUE,
COD_PROTHEUS,
COD_SISTEMA_ORIGEM,
NOM_SANEADO,
DSC_PRINCIPIO_ATIVO,
ORIGEM,
DAT_CARGA
FROM vw_tmp_tbl_procedimentos
"""
df_procedimentos = spark.sql(sql_procedimentos)
df_procedimentos.createOrReplaceTempView("vw_procedimentos")


# COMMAND ----------

# DBTITLE 1,UNION
sql_union_produto = f"""

SELECT * FROM vw_matmed
UNION ALL
SELECT * FROM vw_procedimentos;
"""
df_union_produto = spark.sql(sql_union_produto)
df_union_produto.createOrReplaceTempView("vw_union_produto")

# COMMAND ----------

# DBTITLE 1,DELETE
sql_delete = f"""

DELETE FROM `3_prd_sandbox`.`engenharia_dados`.tbl_tru_pe_produto
WHERE ORIGEM = '{sistema_origem}'
    """
df_delete = spark.sql(sql_delete)

# COMMAND ----------

# DBTITLE 1,INSERT
sql_insert = f"""
INSERT INTO `3_prd_sandbox`.engenharia_dados.tbl_tru_pe_produto
SELECT * FROM vw_union_produto;
    """

df_insert = spark.sql(sql_insert)