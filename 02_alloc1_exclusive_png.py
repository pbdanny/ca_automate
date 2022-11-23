# Databricks notebook source
# MAGIC %run /Users/thanakrit.boonquarmdee@lotuss.com/utils/std_import

# COMMAND ----------

project_prefix = "abfss://data@pvtdmdlsazc02.dfs.core.windows.net/tdm_seg.db/thanakrit/media/adhoc/png_beauty"
alloc_prefix = os.path.join(project_prefix, "alloc")
pandas_prefix = "/dbfs/FileStore/media/fb_targeting/png_beauty"
spark_prefix = "dbfs:/FileStore/media/fb_targeting/png_beauty"

# COMMAND ----------

# MAGIC %md ##Exclusive shopper - Product group 1-3 / 6-8

# COMMAND ----------

txn = spark.read.parquet(os.path.join(project_prefix, "txn_all_202201_202226_cc_hde.parquet"))
all_prd_gr = spark.read.parquet(os.path.join(project_prefix, "prod_gr_nm.parquet"))
exc_prd_gr = all_prd_gr.where(F.col("prd_gr_nm").isin(["1_rejoice", "2_pantene", "3_HnS", "6_oley", "7_safegard", "8_oralB"]))

exc_prd_gr.select("prd_gr_nm").drop_duplicates().show()

# COMMAND ----------

# DBTITLE 1,Check number of exclusive shopper
(txn
 .join(exc_prd_gr, "upc_id", "inner")
 .groupBy("household_id")
 .agg(F.sort_array(F.collect_set("prd_gr_nm")).alias("prd_gr_set"))
 .withColumn("n_prd_gr_set", F.size("prd_gr_set"))
 .where(F.col("n_prd_gr_set")==1)
 .groupBy("prd_gr_set")
 .count()
 .withColumn("prd_gr_nm", F.explode("prd_gr_set"))
).display()

# COMMAND ----------

# DBTITLE 1,Allocation 1 - Exclusive set
exc_prd_gr_set = \
(txn
 .join(exc_prd_gr, "upc_id", "inner")
 .groupBy("household_id")
 .agg(F.sort_array(F.collect_set("prd_gr_nm")).alias("prd_gr_set"))
 .withColumn("n_prd_gr_set", F.size("prd_gr_set"))
 .where(F.col("n_prd_gr_set")==1)
 .withColumn("prd_gr_nm", F.explode("prd_gr_set"))
)

exc_prd_gr_set.count()

# COMMAND ----------

# DBTITLE 1,Mapping sales rank in each group
sales_rank_by_prd_gr = spark.read.parquet(os.path.join(project_prefix, "sales_rank_by_prd_gr_nm.parquet"))
exc_prd_sales_rank = exc_prd_gr_set.join(sales_rank_by_prd_gr, ["household_id", "prd_gr_nm"], "inner")
exc_prd_sales_rank.count()

# COMMAND ----------

exc_prd_sales_rank.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

alloc_spec = [
    {'prd_gr_nm':'1_rejoice','limit':40000},
    {'prd_gr_nm':'2_pantene','limit':40000},
    {'prd_gr_nm':'3_HnS','limit':71500},
    {'prd_gr_nm':'6_oley','limit':60345},
    {'prd_gr_nm':'7_safegard','limit':43933},
    {'prd_gr_nm':'8_oralB','limit':87000},
]

# COMMAND ----------

def limit_gr_rank(sf, spec, gr_nm_col, rank_cal):
    """
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    limited_sf = spark.createDataFrame([], sf.schema)
    
    for s in spec:
        print(f'{s[gr_nm_col]} limit {s["limit"]:,d}')
        gr_sf = sf.where(F.col(gr_nm_col)==s[gr_nm_col]).orderBy(F.col(rank_cal)).limit(s["limit"])
        # gr_sf = sf.where(F.col(gr_nm_col)==s[gr_nm_col]).where(F.col(rank_cal)<=s["limit"])
        gr_sf = gr_sf.checkpoint()
        limited_sf = limited_sf.unionByName(gr_sf)
        limited_sf = limited_sf.checkpoint()
    
    return limited_sf

# COMMAND ----------

alloc1_exc_gr = limit_gr_rank(exc_prd_sales_rank, alloc_spec, "prd_gr_nm", "sales_rank_in_gr")

# COMMAND ----------

alloc1_exc_gr.groupBy("prd_gr_nm").count().display()

# COMMAND ----------

alloc1_exc_gr.groupBy("prd_gr_nm").agg(F.min("sales_rank_in_gr"), F.max("sales_rank_in_gr"), F.count("*")).display()

# COMMAND ----------

top_rnk = sales_rank_by_prd_gr.where(F.col("prd_gr_nm")=="2_pantene").where(F.col("sales_rank_in_gr")==178).select("household_id")

sales_rank_by_prd_gr.join(top_rnk, "household_id").display()

# COMMAND ----------

(alloc1_exc_gr
 .write
 .format("parquet")
 .mode("overwrite")
 .save(os.path.join(alloc_prefix, "alloc1_exclusive_prd_gr.parquet"))
)

# COMMAND ----------

spark.read.parquet(os.path.join(alloc_prefix, "alloc1_exclusive_prd_gr.parquet")).count()
