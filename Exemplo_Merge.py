# Databricks notebook source
# MAGIC %run /Repos/lakehouse/ProjetoLakehouse/0_feature/nb_globais_definicoes

# COMMAND ----------

# DBTITLE 1,Definição de Variáveis
sistema_origem = 'DATASIGH_IRV'
#uc_lakehouse = valor pré definido no notebook /Workspace/0_feature/nb_globais_definicoes
#uc_ingestao = valor pré definido no notebook /Workspace/0_feature/nb_globais_definicoes 

# COMMAND ----------

# DBTITLE 1,Capturando data carga delta
df_data_delta = spark.sql(f"""
                          SELECT NVL(MAX(DAT_CARGA),'1900-01-01') AS DAT_DELTA
                          FROM `{uc_lakehouse}`.`2_tru_emr`.tbl_tru_produto
                          WHERE ORIGEM = '{sistema_origem}'
                        """)
df_data_delta.createOrReplaceTempView("vw_temp_data_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC             CAST(A.CD_SERVICO AS STRING) AS CD_SERVICO,
# MAGIC             CAST(A.DS_CODIGO AS STRING) AS DS_CODIGO
# MAGIC             FROM(
# MAGIC                 SELECT
# MAGIC                 DISTINCT PROCED.CD_SERVICO,
# MAGIC                 TUS.DS_CODIGO,
# MAGIC                 ROW_NUMBER() OVER (
# MAGIC                     PARTITION BY CONCAT(PROCED.CD_SERVICO, '-', TUS.DS_CODIGO),
# MAGIC                     '{sistema_origem}'
# MAGIC                     ORDER BY
# MAGIC                     CAST(PROCED.DT_CARGA AS DATE) DESC
# MAGIC                 ) AS RN
# MAGIC                 FROM
# MAGIC                 `3_prd_ingestao`.`1_raw_datasigh_irv`.servicos PROCED
# MAGIC                 LEFT JOIN `3_prd_ingestao`.`1_raw_datasigh_irv`.servxlanc TUS ON PROCED.CD_SERVICO = TUS.CD_SERVICO
# MAGIC             ) A
# MAGIC             WHERE
# MAGIC             DS_CODIGO IS NOT NULL
# MAGIC             AND RN = 1;

# COMMAND ----------

# DBTITLE 1,#TMP_TRU_COD_TUSS
df_tmp_tru_cod_tuss = spark.sql(f"""
            SELECT
            CAST(A.CD_SERVICO AS STRING) AS CD_SERVICO,
            CAST(A.DS_CODIGO AS STRING) AS DS_CODIGO
            FROM(
                SELECT
                DISTINCT PROCED.CD_SERVICO,
                TUS.DS_CODIGO,
                ROW_NUMBER() OVER (
                    PARTITION BY CONCAT(PROCED.CD_SERVICO, '-', TUS.DS_CODIGO),
                    '{sistema_origem}'
                    ORDER BY
                    CAST(PROCED.DT_CARGA AS DATE) DESC
                ) AS RN
                FROM
                `{uc_ingestao}`.`1_raw_datasigh_irv`.servicos PROCED
                LEFT JOIN `{uc_ingestao}`.`1_raw_datasigh_irv`.servxlanc TUS ON UPPER(TRIM(PROCED.CD_SERVICO)) = UPPER(TRIM(TUS.CD_SERVICO))
            ) A
            WHERE
            DS_CODIGO IS NOT NULL
            AND RN = 1;
                                """)
df_tmp_tru_cod_tuss.createOrReplaceTempView("vw_tmp_tru_cod_tuss")

# COMMAND ----------

# DBTITLE 1,#TMP_TRU_PRODUTO
df_tmp_tru_produto = spark.sql(f"""
                SELECT
                      COD_SISTEMA_ORIGEM	
                    , COD_PRODUTO	
                    , NOM_PRODUTO		
                    , NOM_SANEADO		
                    , DSC_PRINCIPIO_ATIVO
                    , DSC_CLASSE
                    , DSC_AGRUPAMENTO	
                    , DSC_FAMILIA	
                    , DSC_VIA_ADM	
                    , UND_PRODUTO	
                    , COD_PROTHEUS
                    , DSC_PROTHEUS		
                    , TIP_PROTHEUS		
                    , ESP_PROTHEUS	
                    , FAB_PROTHEUS		
                    , UM_PROTHEUS
                    , COD_TUSS
                    , ORIGEM		
                    , DAT_CARGA			
                    , FLG_APAGA			
                    , NULL AS COD_EAN
                    , NULL AS COD_TUSS_BRASINDICE
                FROM (
                    SELECT
                            TRIM(CAST(PROCED.CD_SERVICO AS STRING))                             AS COD_SISTEMA_ORIGEM,
                            TRIM(CAST(PROCED.CD_SERVICO AS STRING))                             AS COD_PRODUTO,
                            UPPER(TRIM(IFNULL(CAST(PROCED.DS_NOME AS STRING), '-1')))           AS NOM_PRODUTO,
                            '-1'                                                                AS NOM_SANEADO,
                            '-1'                                                                AS DSC_PRINCIPIO_ATIVO,
                            'PROCEDIMENTO'                                                      AS DSC_CLASSE,
                            '-1'                                                                AS DSC_AGRUPAMENTO,
                            '-1'                                                                AS DSC_FAMILIA,
                            '-1'                                                                AS DSC_VIA_ADM,
                            '-1'                                                                AS UND_PRODUTO,
                            '-1'                                                                AS COD_PROTHEUS,
                            '-1'                                                                AS DSC_PROTHEUS,
                            '-1'                                                                AS TIP_PROTHEUS,
                            '-1'                                                                AS ESP_PROTHEUS,
                            '-1'                                                                AS FAB_PROTHEUS,
                            '-1'                                                                AS UM_PROTHEUS,
                            COALESCE(CAST(T.DS_CODIGO	 AS STRING),'-1')                       AS COD_TUSS,
                            '{sistema_origem}'                                                  AS ORIGEM,
                            CAST(PROCED.DT_CARGA AS DATE)                                       AS DAT_CARGA,
                            0                                                                   AS FLG_APAGA,
                            ROW_NUMBER() OVER (
                                PARTITION BY TRIM(CAST(PROCED.CD_SERVICO AS STRING)), '{sistema_origem}' 
                                ORDER BY CAST(PROCED.DT_CARGA AS DATE) DESC)                    AS RN
                    FROM `{uc_ingestao}`.`1_raw_datasigh_irv`.servicos PROCED
                    LEFT JOIN vw_tmp_tru_cod_tuss AS T 
                        ON TRIM(PROCED.CD_SERVICO) = TRIM(T.CD_SERVICO)
            )A
            WHERE A.RN = 1;
	                                """)
df_tmp_tru_produto.createOrReplaceTempView("vw_tmp_tru_produto")


# COMMAND ----------

sql_merge = f"""
            MERGE INTO `{uc_lakehouse}`.`2_tru_emr`.tbl_tru_produto tgt
            USING vw_tmp_tru_produto src
            ON tgt.COD_PRODUTO = src.COD_PRODUTO
            AND tgt.ORIGEM = src.ORIGEM
            AND tgt.COD_SISTEMA_ORIGEM = src.COD_SISTEMA_ORIGEM
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
df_merge = spark.sql(sql_merge)
df_merge.createOrReplaceTempView("vw_merge")
df_merge.show()
