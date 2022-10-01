# Databricks notebook source
# MAGIC %run /Users/thanakrit.boonquarmdee@lotuss.com/utils/std_import

# COMMAND ----------

#---- Initialized widget
dbutils.widgets.removeAll()
dbutils.widgets.text(name="fb_ca_prjct_nm", defaultValue="", label="0_Facebook Custom Audience Project name")
dbutils.widgets.text(name="strt_wk_id", defaultValue="", label="1_Start week_id")
dbutils.widgets.text(name="end_wk_id", defaultValue="", label="2_End week_id")

prjct_nm = dbutils.widgets.get("fb_ca_prjct_nm")
strt_wk_id = dbutils.widgets.get("strt_wk_id")
end_wk_id = dbutils.widgets.get("end_wk_id")

# COMMAND ----------

#---- Define project perfix
abfss_prefix = "abfss://data@pvtdmdlsazc02.dfs.core.windows.net/tdm_seg.db/thanakrit/media/fb_ca"
dbfs_file_prefix = "/dbfs/FileStore/media/fb_ca"
dbfs_spark_prefix = "dbfs:/FileStore/media/fb_ca"

prjct_abfss_prefix = os.path.join(abfss_prefix, prjct_nm)
prjct_dbfs_file_prefix = os.path.join(dbfs_file_prefix, prjct_nm)
prjct_dbfs_spark_prefix = os.path.join(dbfs_spark_prefix, prjct_nm)

# COMMAND ----------

#---- Check snaped transaction range
txn118wk = spark.read.load(f'abfss://data@pvtdmdlsazc02.dfs.core.windows.net/tdm_seg.db/thanakrit/latest_txn_118wk/txn')
txn118wk.agg(F.min("week_id"), F.min("date_id"), F.max("week_id"), F.max("date_id")).show()

# COMMAND ----------

def _get_last_dt_id_txt(wk_id: int) -> str:
    """
    """
    dt_id = spark.table("tdm.v_date_dim").where(F.col("week_id")==wk_id).agg(F.max("date_id")).collect()[0][0]
    dt_id_txt = dt_id.strftime("%Y-%m-%d")
    return dt_id_txt

def _get_truprice_seg(cp_end_date: str):
    """Get truprice seg from campaign end date
    With fallback period_id in case truPrice seg not available
    """
    from datetime import datetime, date, timedelta

    def __get_p_id(date_id: str,
                   bck_days: int = 0)-> str:
        """Get period_id for current or back date
        """
        date_dim = spark.table("tdm.v_date_dim")
        bck_date = (datetime.strptime(date_id, "%Y-%m-%d") - timedelta(days=bck_days)).strftime("%Y-%m-%d")
        bck_date_df = date_dim.where(F.col("date_id")==bck_date)
        bck_p_id = bck_date_df.select("period_id").drop_duplicates().collect()[0][0]

        return bck_p_id

    # Find period id to map Truprice / if the truprice period not publish yet use latest period
    bck_p_id = __get_p_id(cp_end_date, bck_days=180)
    truprice_all = \
        (spark.table("tdm_seg.srai_truprice_full_history")
         .where(F.col("period_id")>=bck_p_id)
         .select("household_id", "truprice_seg_desc", "period_id")
         .drop_duplicates()
        )
    max_trprc_p_id = truprice_all.agg(F.max("period_id")).drop_duplicates().collect()[0][0]

    crrnt_p_id = __get_p_id(cp_end_date, bck_days=0)

    if int(max_trprc_p_id) < int(crrnt_p_id):
        trprc_p_id = max_trprc_p_id
    else:
        trprc_p_id = crrnt_p_id

    trprc_seg = \
        (truprice_all
         .where(F.col("period_id")==trprc_p_id)
         .select("household_id", "truprice_seg_desc")
        )

    return trprc_seg, trprc_p_id

# COMMAND ----------

# DBTITLE 0,Snap Txn and map truprice
"""
Snap txn , map TruPrice, additional filter
----
"""
end_dt_txt = _get_last_dt_id_txt(end_wk_id)

trprc_seg, _  = _get_truprice_seg(cp_end_date=end_dt_txt)

txn = \
(txn118wk
 .where(F.col("week_id").between(strt_wk_id, end_wk_id))
 .where(F.col("customer_id").isNotNull())
 
#  .where(F.col("store_format_online_subchannel_other").isin(["GoFresh", "HDE"]))
 
 .join(trprc_seg, "household_id", "left")
 .fillna(value="unclassified", subset=["truprice_seg_desc"])
)

(txn
 .write
 .format('parquet')
 .mode("overwrite")
 .save(os.path.join(prjct_abfss_prefix, "snap_txn.parquet"))
)

# COMMAND ----------

# DBTITLE 0,Defined product group
"""
Load, check product group
----
"""
prd_gr = spark.read.csv(os.path.join(prjct_dbfs_spark_prefix, "prod_gr_nm.csv"), header=True, inferSchema=True)
prd_gr.printSchema()
print("N upc_id : ", prd_gr.count())
print("N unique upc_id : ", prd_gr.select("upc_id").drop_duplicates().count())
prd_gr.groupBy("gr_nm").count().display()

# COMMAND ----------

"""
Load snap period Txn
----
"""
txn = spark.read.parquet(os.path.join(prjct_abfss_prefix, "snap_txn.parquet"))

# COMMAND ----------

"""
Dev
---
todo
1) Check product class, get all upc in those class
2) Get purchase cycle of those class


"""

# 1) Check product class, get all upc in those class
prd_dim = spark.table(TBL_PROD)
gr_nm_upc = prd_gr.where(F.col("gr_nm").isin(["hygiene"]))
gr_nm_class = prd_dim.join(upc, "upc_id", "inner").select("class_code", "class_name").drop_duplicates()
# gr_nm_class.display()
all_class_upc = prd_dim.join(gr_nm_class_code, "class_code").select("upc_id").drop_duplicates()
# all_class_upc.count()

# 2) get purchase cycle of those class
class_prchs_cycl = \
(txn
 .join(all_class_upc, "upc_id")
 .select("household_id", "date_id", "pkg_weight_unit")
 .groupBy("household_id", "date_id")
 .agg(F.sum("pkg_weight_unit").alias("units"))
 .withColumn("prev_date_id", F.lag("date_id").over(Window.partitionBy("household_id").orderBy(F.col("date_id").asc_nulls_last())))
 .where(F.col("prev_date_id").isNotNull())
 .withColumn("day_diff", F.datediff(F.col("date_id"), F.col("prev_date_id")))
)

class_prchs_cycl.display()

usage = \
(class_prchs_cycl
 .withColumn("day_per_unit", F.col("day_diff")/F.col("units"))
 .groupBy("household_id")
 .agg(F.percentile_approx(F.col("day_per_unit"), percentage=0.5, accuracy=100).alias("med_day_per_unit"))
)

# oth_gr = \
# (txn
#  .join(upc, "upc_id")
#  .groupBy("gr_nm", "household_id")
#  .agg(F.sum("net_spend_amt").alias("sales"))
#  .withColumn("gr_sales_rank", F.row_number().over(Window.partitionBy("gr_nm").orderBy(F.col("sales").desc_nulls_last())))
# )

# oth_gr.display()

# (oth_gr
#  .write
#  .format('parquet')
#  .mode("overwrite")
#  .save(os.path.join(prjct_abfss_prefix, "gr_nm_sales_rank_part2.parquet"))
# )

# COMMAND ----------

"""
Test Poisson distribution
"""


# COMMAND ----------

class_prchs_cycl.count()

# COMMAND ----------

"""
(1) Create table of group_nm and sales rank
----
(C) Special for SalzKing
    Salz toothbrush
"""

salz_brush = prd_gr.where(F.col("gr_nm").isin(["salz_toothbrush"]))
brush_gr = \
(txn
 .join(salz_brush, "upc_id")
 .groupBy("gr_nm", "household_id")
 .agg(F.sum("net_spend_amt").alias("sales"))
 .withColumn("gr_sales_rank", F.row_number().over(Window.partitionBy("gr_nm").orderBy(F.col("sales").desc_nulls_last())))
)

brush_gr.display()

(brush_gr
 .write
 .format('parquet')
 .mode("overwrite")
 .save(os.path.join(prjct_abfss_prefix, "gr_nm_sales_rank_part3.parquet"))
)

# COMMAND ----------

"""
(1) Create table of group_nm and sales rank
----
(D) Special for SalzKing
    Multibrand shopper
"""

multi_brand = prd_gr.where(F.col("gr_nm").isin(["multi_brand"]))
multi_gr = \
(txn
 .join(multi_brand, "upc_id")
 .groupBy("gr_nm", "household_id")
 .agg(F.sum("net_spend_amt").alias("sales"))
 .withColumn("gr_sales_rank", F.row_number().over(Window.partitionBy("gr_nm").orderBy(F.col("sales").desc_nulls_last())))
)

multi_gr.display()

(multi_gr
 .write
 .format('parquet')
 .mode("overwrite")
 .save(os.path.join(prjct_abfss_prefix, "gr_nm_sales_rank_part4.parquet"))
)

# COMMAND ----------

"""
(2) Combine each group nm with rank
-----
"""

from functools import reduce
from pprint import pprint

gr_nm_paths = [f.path for f in dbutils.fs.ls(prjct_abfss_prefix)if f.name.startswith("gr_nm_sales")]
pprint(gr_nm_paths)
gr_nm_sfs = [spark.read.parquet(f) for f in gr_nm_paths]
gr_nm_comb = reduce(SparkDataFrame.unionByName, gr_nm_sfs)
gr_nm_comb.printSchema()

(gr_nm_comb
 .write
 .format('parquet')
 .mode("overwrite")
 .save(os.path.join(prjct_abfss_prefix, "gr_nm_w_rank.parquet"))
)

# COMMAND ----------

"""
(3) Load (Combined) group_nm & rank
    Adjust format for auto allocation
-----

"""
gr_nm_w_rank = spark.read.parquet(os.path.join(prjct_abfss_prefix, "gr_nm_w_rank.parquet"))

# Create hh_id , list of gr_nm and number of group
gr_nm_list_sort = \
(gr_nm_w_rank
 .groupBy("household_id")
 .agg(F.sort_array(F.collect_set("gr_nm")).alias("gr_nm_list"))
 .withColumn("n_gr", F.size("gr_nm_list"))
)
gr_nm_list_sort.display()

# Create hh_id with mapping of gr_nm : rank
gr_nm_w_rank_map = \
(gr_nm_w_rank
 .withColumn("map_rank", F.create_map("gr_nm", "gr_sales_rank"))
 .withColumn("cMap", F.map_entries(F.col("map_rank")))
 .groupBy("household_id")
 .agg(F.map_from_entries(F.flatten(F.collect_set("cMap"))).alias("gr_rank_map"))
)
gr_nm_w_rank_map.display()

# Combine list , map 
gr_nm_list_w_rank_map = gr_nm_list_sort.join(gr_nm_w_rank_map, "household_id", "inner")

(gr_nm_list_w_rank_map 
 .write
 .format('parquet')
 .mode("overwrite")
 .save(os.path.join(prjct_abfss_prefix, "gr_nm_list_w_rank_map.parquet"))
)

gr_nm_list_w_rank_map.display()

# COMMAND ----------

# Check list size all record must equal map size
gr_nm_list_w_rank_map.withColumn("map_size", F.size("gr_rank_map")).where(F.col("n_gr")!=F.col("map_size")).display()

# COMMAND ----------

"""
End
----
"""

# COMMAND ----------

# DBTITLE 0,Shopping Cycle
#---- Shopping Cycle by gr_nm

# from pyspark.sql import Window
# shp_cyc = \
# (last_6mth_cc
#  .join(all_prd_gr, "upc_id", "inner")
#  .select("household_id", "date_id")
#  .drop_duplicates()
#  .withColumn("lag_date_id", F.lag("date_id").over(Window.partitionBy("household_id").orderBy("date_id")))
#  .withColumn("date_diff", F.datediff("date_id", "lag_date_id"))
#  .where(F.col("date_diff").isNotNull()) # remove onetime
#  .groupBy("household_id")
#  .agg(F.percentile_approx("date_diff", 0.5).alias("med_shp_cyc_day"))
# )

# shp_cyc.write.format("parquet").mode("overwrite").save(os.path.join(project_prefix, "all_prd_grp_shp_cycl.parquet"))

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
