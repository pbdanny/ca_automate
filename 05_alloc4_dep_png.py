# Databricks notebook source
# MAGIC %run /Users/thanakrit.boonquarmdee@lotuss.com/utils/std_import

# COMMAND ----------

project_prefix = "abfss://data@pvtdmdlsazc02.dfs.core.windows.net/tdm_seg.db/thanakrit/media/adhoc/png_beauty"
alloc_prefix = os.path.join(project_prefix, "alloc")
pandas_prefix = "/dbfs/FileStore/media/fb_targeting/png_beauty"
spark_prefix = "dbfs:/FileStore/media/fb_targeting/png_beauty"

# COMMAND ----------

alloc1_exc_gr = spark.read.parquet(os.path.join(alloc_prefix, "alloc1_exclusive_prd_gr.parquet"))
alloc2 = spark.read.parquet(os.path.join(alloc_prefix, "alloc2_gr_4_10.parquet"))
alloc3 = spark.read.parquet(os.path.join(alloc_prefix, "alloc3_gr_10.parquet"))

txn = spark.read.parquet(os.path.join(project_prefix, "txn_all_202201_202226_cc_hde.parquet"))
txn_ex = txn.join(alloc1_exc_gr, "household_id", "leftanti").join(alloc2,  "household_id", "leftanti").join(alloc3, "household_id", "leftanti")

# COMMAND ----------

# MAGIC %md ##Allocate Gr 5

# COMMAND ----------

# DBTITLE 1,Toil not Rejoice, Pantene, HnS & Beau not not Rejoice, Pantene, HnS
all_prd_gr = spark.read.parquet(os.path.join(project_prefix, "prod_gr_nm.parquet"))
beau = all_prd_gr.where(F.col("prd_gr_nm").isin(["dep_beauty"]))
toil = all_prd_gr.where(F.col("prd_gr_nm").isin(["dep_toiletries"]))
rej_pan_hs = all_prd_gr.where(F.col("prd_gr_nm").isin(["1_rejoice", "2_pantene", "3_HnS"]))

# COMMAND ----------

toil_not_hh = \
(txn_ex
 .join(toil, "upc_id")
 .join(rej_pan_hs, "upc_id", "leftanti")
 .select("household_id")
 .drop_duplicates()
)

beau_not_hh = \
(txn_ex
 .join(beau, "upc_id")
 .join(rej_pan_hs, "upc_id", "leftanti")
 .select("household_id")
 .drop_duplicates()
)

alloc4_gr_5 = beau_not_hh.join(toil_not_hh, "household_id")

# COMMAND ----------

alloc4_gr_5.count()

# COMMAND ----------

# DBTITLE 1,Calculate sales rank 
beu_toi_prd_gr = all_prd_gr.where(F.col("prd_gr_nm").isin(["dep_beauty", "dep_toiletries"]))
sales_rank_beu_toi = \
(txn_ex
 .join(beu_toi_prd_gr, "upc_id")
 .withColumn("prd_gr_nm", F.lit("5_toiletries_hba_not_tr1"))
 .groupBy("household_id", "prd_gr_nm")
 .agg(F.sum("net_spend_amt").alias("sales"))
 .withColumn("sales_rank_in_gr", F.row_number().over(Window.partitionBy("prd_gr_nm").orderBy(F.col("sales").desc())))
)

# COMMAND ----------

# DBTITLE 1,Select by sales rank
N_LIMIT = 132000

alloc4_g5_rank = \
(alloc4_gr_5
 .join(sales_rank_beu_toi, "household_id", "inner")
 .orderBy(F.col("sales_rank_in_gr"))
 .limit(N_LIMIT)
)

(alloc4_g5_rank
 .write
 .format("parquet")
 .mode("overwrite")
 .save(os.path.join(alloc_prefix, "alloc4_g5.parquet"))
)

# COMMAND ----------

alloc4_gr5 = spark.read.parquet(os.path.join(alloc_prefix, "alloc4_g5.parquet"))
alloc4_gr5.count()

# COMMAND ----------

# MAGIC %md ##Group 11

# COMMAND ----------

txn_ex5 = txn_ex.join(alloc4_gr5, "household_id", "leftanti")

# COMMAND ----------

# DBTITLE 1,Prod gr 11 : Toiletries not Olay, Safeguard, OralB
all_prd_gr = spark.read.parquet(os.path.join(project_prefix, "prod_gr_nm.parquet"))
toil = all_prd_gr.where(F.col("prd_gr_nm").isin(["dep_toiletries"]))
olay_safeguard_oralb = all_prd_gr.where(F.col("prd_gr_nm").isin(["6_oley", "7_safegard", "8_oralB"]))

# COMMAND ----------

alloc4_gr11 = \
(txn_ex5
 .join(toil, "upc_id")
 .join(olay_safeguard_oralb, "upc_id", "leftanti")
 .select("household_id")
 .drop_duplicates()
)

# COMMAND ----------

alloc4_gr11.count()

# COMMAND ----------

# DBTITLE 1,Calculate sales rank
sales_rank_toi = \
(txn_ex
 .join(toil, "upc_id")
 .withColumn("prd_gr_nm", F.lit("11_toiletries_not_tr2"))
 .groupBy("household_id", "prd_gr_nm")
 .agg(F.sum("net_spend_amt").alias("sales"))
 .withColumn("sales_rank_in_gr", F.row_number().over(Window.partitionBy("prd_gr_nm").orderBy(F.col("sales").desc())))
)

# COMMAND ----------

# DBTITLE 1,Select by sales rank
N_LIMIT = 71500

alloc4_g11_rank = \
(alloc4_gr11
 .join(sales_rank_toi, "household_id", "inner")
 .orderBy(F.col("sales_rank_in_gr"))
 .limit(N_LIMIT)
)

(alloc4_g11_rank
 .write
 .format("parquet")
 .mode("overwrite")
 .save(os.path.join(alloc_prefix, "alloc4_g11.parquet"))
)

# COMMAND ----------

alloc4_gr11 = spark.read.parquet(os.path.join(alloc_prefix, "alloc4_g11.parquet"))
alloc4_gr11.count()
