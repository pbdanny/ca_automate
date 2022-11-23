# Databricks notebook source
# MAGIC %run /Users/thanakrit.boonquarmdee@lotuss.com/utils/std_import

# COMMAND ----------

from edm_helper import pandas_to_csv_filestore
from edm_helper import create_zip_from_dbsf_prefix

# COMMAND ----------

project_prefix = "abfss://data@pvtdmdlsazc02.dfs.core.windows.net/tdm_seg.db/thanakrit/media/adhoc/png_beauty"
alloc_prefix = os.path.join(project_prefix, "alloc")
pandas_prefix = "/dbfs/FileStore/media/fb_targeting/png_beauty"
spark_prefix = "dbfs:/FileStore/media/fb_targeting/png_beauty"

# COMMAND ----------

f_path = [f.path for f in dbutils.fs.ls(alloc_prefix)]

# COMMAND ----------

combine_alloc = spark.read.parquet(*f_path)

# COMMAND ----------

combine_alloc.groupBy("prd_gr_nm").count().display()

# COMMAND ----------

combine_alloc.count()

# COMMAND ----------

combine_alloc.agg(F.count_distinct("household_id")).show()

# COMMAND ----------

# DBTITLE 1,Map TruPrice
tp = spark.read.parquet(os.path.join(project_prefix, "txn_all_202201_202226_cc_hde.parquet")).select("household_id", "truprice_seg_desc").drop_duplicates()
cmbn_alloc_tp = combine_alloc.join(tp, "household_id", "inner")

# COMMAND ----------

cmbn_alloc_tp.count()

# COMMAND ----------

# DBTITLE 1,Group with no Test
no_test_prd_gr_nm = ["6_oley", "7_safegard", "10_oley_skincare"]

alloc_split = cmbn_alloc_tp.where(~F.col("prd_gr_nm").isin(no_test_prd_gr_nm))
alloc_no_split = cmbn_alloc_tp.where(F.col("prd_gr_nm").isin(no_test_prd_gr_nm))

# COMMAND ----------

# DBTITLE 1,Stratify sampling 5% by TruPrice
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

test, ctrl = statify_2_step(alloc_split, "prd_gr_nm", "truprice_seg_desc", fraction_dict=fraction)

# COMMAND ----------

test.groupBy("prd_gr_nm").count().display()

# COMMAND ----------

ctrl.groupBy("prd_gr_nm").count().display()

# COMMAND ----------

test_comb = test.unionByName(alloc_no_split)

# COMMAND ----------

test_comb.groupBy("prd_gr_nm").count().display()

# COMMAND ----------

ctrl.groupBy("prd_gr_nm").count().display()

# COMMAND ----------

# DBTITLE 1,Map Golden
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

export_path = "/dbfs/FileStore/thanakrit/temp/png_beauty"
map_group_golden_to_csv(ctrl, "prd_gr_nm", dbfs_python_prefix=export_path, file_nm_suffix="ctrl")

# COMMAND ----------

map_group_golden_to_csv(test_comb, "prd_gr_nm", dbfs_python_prefix=export_path, file_nm_suffix="test")

# COMMAND ----------

f_path = [f.path for f in dbutils.fs.ls("dbfs:/FileStore/thanakrit/temp/png_beauty")]
for f in f_path:
    sf = spark.read.csv(f, header=True)
    n = sf.count()
    print(f"{f} : {n:,d}")

# COMMAND ----------

# MAGIC %md ##Create Zip file and export

# COMMAND ----------

create_zip_from_dbsf_prefix(dbfs_prefix_to_zip=export_path, output_zipfile_name="png_beauty_test_ctrl.zip")

# COMMAND ----------


