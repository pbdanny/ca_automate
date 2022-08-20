# Databricks notebook source
# MAGIC %run /Users/thanakrit.boonquarmdee@lotuss.com/utils/std_import

# COMMAND ----------

from edm_helper import pandas_to_csv_filestore, create_zip_from_dbsf_prefix

# COMMAND ----------

#---- Initialized widget
dbutils.widgets.removeAll()
dbutils.widgets.text(name="fb_ca_prjct_nm", defaultValue="", label="0_Facebook Custom Audience Project name")
prjct_nm = dbutils.widgets.get("fb_ca_prjct_nm")

#---- Define project perfix
abfss_prefix = "abfss://data@pvtdmdlsazc02.dfs.core.windows.net/tdm_seg.db/thanakrit/media/fb_ca"
dbfs_file_prefix = "/dbfs/FileStore/media/fb_ca"
dbfs_spark_prefix = "dbfs:/FileStore/media/fb_ca"

prjct_abfss_prefix = os.path.join(abfss_prefix, prjct_nm)
prjct_dbfs_file_prefix = os.path.join(dbfs_file_prefix, prjct_nm)
prjct_dbfs_spark_prefix = os.path.join(dbfs_spark_prefix, prjct_nm)

# COMMAND ----------

#---- Load Allocatin spec
alloc_spc = spark.read.csv(os.path.join(prjct_dbfs_spark_prefix, "alloc_spec_new.csv"), header=True, inferSchema=True)

alloc_spc.display()
alloc_spc

# COMMAND ----------

#---- Define source table and allocated table
snap_txn_table = "snap_txn.parquet"
rank_map_table = "gr_nm_list_sales_rank_map.parquet"
alloctd_table = "allctd.parquet"
alloctd_truprice = "allctd_truprice.parquet"

# COMMAND ----------

#---- Load allocated table and count check
allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, alloctd_table))
allctd.groupBy("ca_nm").count().orderBy(F.col("ca_nm").asc_nulls_last()).display()

# COMMAND ----------

#---- Map TruPrice Segment
trprc = \
(spark.read.parquet(os.path.join(prjct_abfss_prefix, snap_txn_table))
 .select("household_id", "truprice_seg_desc")
 .drop_duplicates()
)

allctd_trprc = allctd.join(trprc, "household_id", "left")

(allctd_trprc
 .write
 .format("parquet")
 .mode("overwrite")
 .save(os.path.join(prjct_abfss_prefix, "allctd_truprice.parquet"))
)

# COMMAND ----------

allctd_trprc = spark.read.parquet(os.path.join(prjct_abfss_prefix, "allctd_truprice.parquet"))

# COMMAND ----------

allctd_trprc.groupBy("ca_nm").count().display()

# COMMAND ----------

# DBTITLE 0,Stratify sampling 5% by TruPrice
#---- Stratify sampling 5% by TruPrice
fraction = {'Most Price Driven':0.05, 'Most Price Insensitive':0.05, 'Price Driven':0.05, 
            'Price Insensitive':0.05, 'Price Neutral':0.05, 'No TruPrice segment data':0.05}

def statify_2_step(sf, macro_group_col_name, startum_col_name, fraction_dict):
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')

    macro_group_list = sf.select(F.col(macro_group_col_name)).drop_duplicates().toPandas()[macro_group_col_name].to_list()

    test_sf = spark.createDataFrame([], sf.schema)
    control_sf = spark.createDataFrame([], sf.schema)
    for gr in macro_group_list:
        gr_whole = sf.filter(F.col(macro_group_col_name)==gr)
        gr_whole = gr_whole.checkpoint()
        
        gr_ctrl = gr_whole.sampleBy(startum_col_name, fractions=fraction_dict, seed=0)
        gr_test = gr_whole.subtract(gr_ctrl)
                    
        gr_test = gr_test.checkpoint()
        gr_ctrl = gr_ctrl.checkpoint()

        test_sf = test_sf.unionByName(gr_test)
        control_sf = control_sf.unionByName(gr_ctrl)
        
    return test_sf, control_sf

# COMMAND ----------

test, ctrl = statify_2_step(allctd_trprc.select("household_id", "ca_nm", "truprice_seg_desc"), 
                            "ca_nm", "truprice_seg_desc", fraction_dict=fraction)

# COMMAND ----------

(test
 .groupBy("ca_nm").agg(F.count("*").alias("n_test"))
 .join(ctrl
       .groupBy("ca_nm").agg(F.count("*").alias("n_ctrl")), 
       "ca_nm", "inner")
 .orderBy("ca_nm")
 .withColumn("n_total", F.col("n_test")+F.col("n_ctrl"))
).display()

# COMMAND ----------

def map_golden_export_csv(sf):
    """From sparkFrame, map golden record
    """
    cust_golden = spark.table("tdm.v_customer_dim").select('household_id', 'golden_record_external_id_hash').drop_duplicates()
    sf_out = sf.join(cust_golden, "household_id", "left")
    
    return sf_out

def map_group_golden_to_csv(sf, gr_col_nm, dbfs_python_prefix, file_nm_suffix):
    """Get list of group
    Map golden, save to target location as single csv with file name = group_name + suffix
    """
    gr_list = sf.select(gr_col_nm).drop_duplicates().toPandas()[gr_col_nm].to_list()
    sf_gl = map_golden_export_csv(sf)
    
    for gr in gr_list:
        csv_file_nm = gr + "_" + file_nm_suffix + ".csv"
        sf_to_export = sf_gl.where(F.col(gr_col_nm)==gr).select("golden_record_external_id_hash")
        df_to_export = sf_to_export.toPandas()
        print(f"Convert {gr} to csv at location {dbfs_python_prefix} with file name {csv_file_nm}")
        pandas_to_csv_filestore(df_to_export, csv_file_name=csv_file_nm, prefix=dbfs_python_prefix)

# COMMAND ----------

export_prefix = os.path.join(prjct_dbfs_file_prefix, "export")
print(f"Export to DBFS at location : {export_prefix}") 
map_group_golden_to_csv(ctrl, "ca_nm", dbfs_python_prefix=export_prefix, file_nm_suffix="ctrl")

# COMMAND ----------

print(f"Export to DBFS at location : {export_prefix}") 
map_group_golden_to_csv(test, "ca_nm", dbfs_python_prefix=export_prefix, file_nm_suffix="test")

# COMMAND ----------

# Create zip file for download
create_zip_from_dbsf_prefix(export_prefix, "hygiene_fscorefill.zip")

# COMMAND ----------


