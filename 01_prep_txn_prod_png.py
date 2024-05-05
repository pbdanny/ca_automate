# Databricks notebook source
# MAGIC %run /Users/thanakrit.boonquarmdee@lotuss.com/utils/std_import

# COMMAND ----------

# dbutils.fs.mkdirs("abfss://data@pvtdmdlsazc02.dfs.core.windows.net/tdm_seg.db/thanakrit/media/adhoc/png_beauty")

# COMMAND ----------

project_prefix = "abfss://data@pvtdmdlsazc02.dfs.core.windows.net/tdm_seg.db/thanakrit/media/adhoc/png_beauty"
alloc_prefix = os.path.join(project_prefix, "alloc")
pandas_prefix = "/dbfs/FileStore/media/fb_targeting/png_beauty"
spark_prefix = "dbfs:/FileStore/media/fb_targeting/png_beauty"

# COMMAND ----------

txn118wk = spark.read.load(f'abfss://data@pvtdmdlsazc02.dfs.core.windows.net/tdm_seg.db/thanakrit/latest_txn_105wk/txn')
txn118wk.agg(F.min("week_id"), F.max("week_id")).show()

# COMMAND ----------

# DBTITLE 1,Snap Txn and map truprice
period_trprc = spark.table(TBL_DATE).where(F.col("week_id")==202226).select("period_id").collect()[0][0]

trprc_seg = \
(spark.table("tdm_seg.srai_truprice_full_history")
 .where(F.col("period_id")==period_trprc)
 .select("household_id", "truprice_seg_desc")
)

last_6mth_cc_hde = \
(txn118wk
 .where(F.col("store_format_online_subchannel_other")=="HDE")
 .where(F.col("week_id").between(202201, 202226))
 .where(F.col("customer_id").isNotNull())
 .join(trprc_seg, "household_id", "left")
 .fillna(value="No TruPrice segment data", subset=["truprice_seg_desc"])
)

(last_6mth_cc_hde
 .write
 .format('parquet')
 .mode("overwrite")
 .save(os.path.join(project_prefix, "txn_all_202201_202226_cc_hde.parquet"))
)

# COMMAND ----------

# DBTITLE 1,Defined product group
prd_gr = spark.read.csv("dbfs:/FileStore/media/fb_targeting/png_beauty/prd_gr.csv", header=True, inferSchema=True)

# COMMAND ----------

skincare = (spark.table("tdm.v_prod_dim_c")
 .where(F.col("section_name").isin(["SKIN CARE"]))
 .select("upc_id")
 .drop_duplicates()
 .withColumn("prd_gr_nm", F.lit("sec_skincare"))
)

# COMMAND ----------

beauty = (spark.table("tdm.v_prod_dim_c")
 .where(F.col("department_name").isin(["HBA BEAUTY"]))
 .select("upc_id")
 .drop_duplicates()
 .withColumn("prd_gr_nm", F.lit("dep_beauty"))
)

# COMMAND ----------

toiletries = (spark.table("tdm.v_prod_dim_c")
 .where(F.col("department_name").isin(["HBA TOILETRIES"]))
 .select("upc_id")
 .drop_duplicates()
 .withColumn("prd_gr_nm", F.lit("dep_toiletries"))
)

# COMMAND ----------

prod_gr_nm = prd_gr.unionByName(skincare).unionByName(beauty).unionByName(toiletries)

# COMMAND ----------

(prod_gr_nm
 .write
 .format('parquet')
 .mode("overwrite")
 .save(os.path.join(project_prefix, "prod_gr_nm.parquet"))
)

# COMMAND ----------

prod_gr_nm = spark.read.parquet(os.path.join(project_prefix, "prod_gr_nm.parquet"))
prod_gr_nm.groupBy("prd_gr_nm").count().display()

# COMMAND ----------

last_6mth_cc_hde.join(prod_gr_nm, "upc_id").groupBy("prd_gr_nm").agg(F.count_distinct("household_id")).display()

# COMMAND ----------

# DBTITLE 1,Sales rank by prod_gr_nm
sales_rank_by_prd_gr = \
(last_6mth_cc_hde
 .join(prod_gr_nm, "upc_id", "inner")
 .select("household_id", "prd_gr_nm", "net_spend_amt")
 .groupBy("household_id", "prd_gr_nm")
 .agg(F.sum("net_spend_amt").alias("sales"))
 .withColumn("sales_rank_in_gr", F.row_number().over(Window.partitionBy("prd_gr_nm").orderBy(F.col("sales").desc())))
)

(sales_rank_by_prd_gr
 .write
 .format('parquet')
 .mode("overwrite")
 .save(os.path.join(project_prefix, "sales_rank_by_prd_gr_nm.parquet"))
)

# COMMAND ----------

sales_rank_by_prd_gr.display()

# COMMAND ----------

# DBTITLE 1,Allocation spec
# schema = T.StructType([T.StructField("gr_set", T.ArrayType(T.StringType())),
#                        T.StructField("target_samp", T.IntegerType()),
#                        T.StructField("gr_alloc_nm", T.StringType())])

# allc_spc = \
# (spark.read.csv(os.path.join(spark_prefix, "allocation_spec.csv"), header=True, inferSchema=True)
#  .withColumn("gr_set", F.from_json(F.col("gr_set"), "array<string>"))
# )

# allc_spc.display()

# COMMAND ----------

# DBTITLE 1,Shopping Cycle
from pyspark.sql import Window
shp_cyc = \
(last_6mth_cc
 .join(all_prd_gr, "upc_id", "inner")
 .select("household_id", "date_id")
 .drop_duplicates()
 .withColumn("lag_date_id", F.lag("date_id").over(Window.partitionBy("household_id").orderBy("date_id")))
 .withColumn("date_diff", F.datediff("date_id", "lag_date_id"))
 .where(F.col("date_diff").isNotNull()) # remove onetime
 .groupBy("household_id")
 .agg(F.percentile_approx("date_diff", 0.5).alias("med_shp_cyc_day"))
)

shp_cyc.write.format("parquet").mode("overwrite").save(os.path.join(project_prefix, "all_prd_grp_shp_cycl.parquet"))

# COMMAND ----------

# DBTITLE 1,Determine shopping cycle dominant product
# MAGIC %md
# MAGIC Identify dominant shopping cycle
# MAGIC - Calculate each household sales penetration of target product to total lotus sales
# MAGIC - if target product sales contributes > median -> those household have target product dominate shopping cycle
# MAGIC - unless, use total store to calculate those household shopping cycle

# COMMAND ----------

# DBTITLE 1,Tendency to buy
# MAGIC %md
# MAGIC If the household use total product as shopping cycle
# MAGIC A) median method 
# MAGIC - calculate median day as total shopping cycle
# MAGIC - find date last total product shopp
# MAGIC - calculate next tendency day to shop = last day shopped + day shopping cycle
# MAGIC
# MAGIC B) Bayes updating methods
# MAGIC
# MAGIC
# MAGIC
# MAGIC If target product dominate shopping cycle
# MAGIC A) median consumption units / day
# MAGIC - consumption units in each basket, days to next target product shopping
# MAGIC - median of (consumpion units / days)
# MAGIC - find last units shopped, last day shopped
# MAGIC - calculate next tendency day to shop = last day shopped + (last unit shopped / median (consumption units / days))
# MAGIC
# MAGIC B) regression method
# MAGIC
# MAGIC C) Bayes updating methods
# MAGIC

# COMMAND ----------


