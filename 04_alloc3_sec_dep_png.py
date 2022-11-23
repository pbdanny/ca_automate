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

txn = spark.read.parquet(os.path.join(project_prefix, "txn_all_202201_202226_cc_hde.parquet"))
txn_ex = txn.join(alloc1_exc_gr, "household_id", "leftanti").join(alloc2,  "household_id", "leftanti")

# COMMAND ----------

# DBTITLE 1,Olay and SkinCare
all_prd_gr = spark.read.parquet(os.path.join(project_prefix, "prod_gr_nm.parquet"))
olay = all_prd_gr.where(F.col("prd_gr_nm").isin(["6_oley"]))
skin = all_prd_gr.where(F.col("prd_gr_nm").isin(["sec_skincare"]))

oley_hh = txn_ex.join(olay, "upc_id").select("household_id").drop_duplicates()
skin_hh = txn_ex.join(skin, "upc_id").select("household_id").drop_duplicates()

oley_skin_hh = oley_hh.join(skin_hh, "household_id")

# COMMAND ----------

(oley_skin_hh
 .withColumn("prd_gr_nm", F.lit("10_oley_skincare"))
 .write
 .format("parquet")
 .mode("overwrite")
 .save(os.path.join(alloc_prefix, "alloc3_gr_10.parquet"))
)

# COMMAND ----------

spark.read.parquet(os.path.join(alloc_prefix, "alloc3_gr_10.parquet")).count()

# COMMAND ----------


