# Databricks notebook source
# MAGIC %run /Users/thanakrit.boonquarmdee@lotuss.com/utils/std_import

# COMMAND ----------

project_prefix = "abfss://data@pvtdmdlsazc02.dfs.core.windows.net/tdm_seg.db/thanakrit/media/adhoc/png_beauty"
alloc_prefix = os.path.join(project_prefix, "alloc")
pandas_prefix = "/dbfs/FileStore/media/fb_targeting/png_beauty"
spark_prefix = "dbfs:/FileStore/media/fb_targeting/png_beauty"

# COMMAND ----------

alloc1_exc_gr = spark.read.parquet(os.path.join(alloc_prefix, "alloc1_exclusive_prd_gr.parquet"))

# COMMAND ----------

# MAGIC %md ##Mix shopper - Product group 4 , 9, 11

# COMMAND ----------

txn = spark.read.parquet(os.path.join(project_prefix, "txn_all_202201_202226_cc_hde.parquet"))
all_prd_gr = spark.read.parquet(os.path.join(project_prefix, "prod_gr_nm.parquet"))
alloc2_prd_gr = \
(all_prd_gr
 .where(F.col("prd_gr_nm").isin(["1_rejoice", "2_pantene", "3_HnS", "6_oley", "7_safegard", "8_oralB"]))
 .withColumn("alloc2_prd_gr", 
             F.when(F.col("prd_gr_nm").isin(["1_rejoice", "2_pantene", "3_HnS"]), "4_rejoice_pantene_HnS")
              .when(F.col("prd_gr_nm").isin(["6_oley", "7_safegard", "8_oralB"]), "9_oley_safegard_oralB")
              .otherwise(None)
            )
)

txn_ex_alloc1 = txn.join(alloc1_exc_gr, "household_id", "leftanti")

# COMMAND ----------

# DBTITLE 1,Check number of exclusive shopper
(txn_ex_alloc1
 .join(alloc2_prd_gr, "upc_id", "inner")
 .groupBy("household_id")
 .agg(F.sort_array(F.collect_set("alloc2_prd_gr")).alias("prd_gr_set"))
 .withColumn("n_prd_gr_set", F.size("prd_gr_set"))
 .groupBy("prd_gr_set", "n_prd_gr_set")
 .agg(F.count("*").alias('n_hh'))
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Sales rank by alloc2 product group set
sales_rank_by_alloc2_prd_gr = \
(txn_ex_alloc1
 .join(alloc2_prd_gr, "upc_id", "inner")
 .select("household_id", "alloc2_prd_gr", "net_spend_amt")
 .groupBy("household_id", "alloc2_prd_gr")
 .agg(F.sum("net_spend_amt").alias("sales"))
 .withColumn("sales_rank_in_gr", F.row_number().over(Window.partitionBy("alloc2_prd_gr").orderBy(F.col("sales").desc())))
)

(sales_rank_by_alloc2_prd_gr
 .write
 .format('parquet')
 .mode("overwrite")
 .save(os.path.join(project_prefix, "sales_rank_by_alloc2_prd_gr.parquet"))
)

# COMMAND ----------

schema = T.StructType([
    T.StructField("alloc2_prd_gr", T.ArrayType(T.StringType()), True),
    T.StructField("prd_gr_nm", T.StringType(), True)
])

mapping_alloc2 = spark.createDataFrame([(['4_rejoice_pantene_HnS'], "4_rejoice_pantene_HnS"),
                                        (['9_oley_safegard_oralB'], "9_oley_safegard_oralB"),
                                        (['4_rejoice_pantene_HnS', '9_oley_safegard_oralB'], "9_oley_safegard_oralB")
                                        ], schema=schema)

mapping_alloc2.display()

# COMMAND ----------

# DBTITLE 1,Combine mix group to group 9 , map sales rank
alloc2_rank_by_prd_gr = \
(txn_ex_alloc1
 .join(alloc2_prd_gr, "upc_id", "inner")
 .groupBy("household_id")
 .agg(F.sort_array(F.collect_set("alloc2_prd_gr")).alias("alloc2_prd_gr"))
 .join(mapping_alloc2, "alloc2_prd_gr")
 .join(sales_rank_by_alloc2_prd_gr.withColumnRenamed("alloc2_prd_gr", "prd_gr_nm"), ["household_id", "prd_gr_nm"], "inner")
)

alloc2_rank_by_prd_gr.count()

# COMMAND ----------

alloc2_rank_by_prd_gr.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

alloc_spec = [
    {'prd_gr_nm':'4_rejoice_pantene_HnS','limit':71500},
    {'prd_gr_nm':'9_oley_safegard_oralB','limit':34500},
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

alloc2 = limit_gr_rank(alloc2_rank_by_prd_gr, alloc_spec, "prd_gr_nm", "sales_rank_in_gr")

# COMMAND ----------

alloc2.groupBy("prd_gr_nm").count().display()

# COMMAND ----------

alloc2.groupBy("prd_gr_nm").agg(F.min("sales_rank_in_gr"), F.max("sales_rank_in_gr"), F.count("*")).display()

# COMMAND ----------

(alloc2
 .write
 .format("parquet")
 .mode("overwrite")
 .save(os.path.join(alloc_prefix, "alloc2_gr_4_10.parquet"))
)

# COMMAND ----------

spark.read.parquet(os.path.join(alloc_prefix, "alloc2_gr_4_10.parquet")).count()
