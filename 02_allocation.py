# Databricks notebook source
# MAGIC %run /Users/thanakrit.boonquarmdee@lotuss.com/utils/std_import

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
# mapping between gr_nm <-> ca_nm (target ca to beallocated)

alloc_spc = spark.read.csv(os.path.join(prjct_dbfs_spark_prefix, "alloc_spec.csv"), header=True, inferSchema=True)
alloc_spc = spark.read.csv(os.path.join(prjct_dbfs_spark_prefix, "alloc_spec_new.csv"), header=True, inferSchema=True)

alloc_spc.display()

# COMMAND ----------

#---- Define source table and allocated table
# -----
# rank_map_table : source table for allocation
# -----
# column
# -----
# household_id
# gr_nm_list (Array) : array of gr_nm to be allocated
# n_gr (int) : count number of gr_nm by each household_id
# gr_rank_map (Map) : mapping of gr_nm with rank (default is sales rank) of each household_id

# -----
# alloctd_table : staging table for allocation
# -----
# ca_nm : flag ca_nm that hh_id was allocated to

rank_map_table = "gr_nm_list_sales_rank_map.parquet"
alloctd_table = "allctd.parquet"

# COMMAND ----------

#---- Check total number of combination
alloc_pool = spark.read.parquet(os.path.join(prjct_abfss_prefix, rank_map_table))
alloc_pool.groupBy("n_gr", "gr_nm_list").count().orderBy(F.col("n_gr"), F.col("count").desc()).display()

# COMMAND ----------

alloc_pool.display()

# COMMAND ----------

def get_to_alloc(alloc_spc,
                 rank_map_table: str,
                 allocated_table: str,
                 reset: bool = False
                ):
    """Calculate n to allocated and order of allocation
    Param
    -----
    alloc_spc : allocation specification
    rank_map_table : original rank map table
    allocated_table : result of allcation, stampped column "ca_nm"
    reset : if True, will start over from rank map table, and re-create allocated_table
    """
    
    if not reset:
        try:
            allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, allocated_table))
        except Exception as e:
            allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, rank_map_table)).withColumn("ca_nm", F.lit(None))
    else:
        print(f"Reset and create new allocated table {rank_map_table}")
        allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, rank_map_table)).withColumn("ca_nm", F.lit(None))
    
    n_allctd = allctd.groupBy("ca_nm").agg(F.count("*").alias("n_allctd"))
    
    to_allctd = \
    (alloc_spc
     .join(n_allctd, "ca_nm", "left")
     .fillna(0, subset="n_allctd")
     .withColumn("to_alloc", F.col("n_ca") - F.col("n_allctd"))
     .withColumn("prop_to_alloc", F.col("to_alloc")/F.col("n_ca"))
     .orderBy(F.col("prop_to_alloc").desc(), F.col("ca_nm"))
    )
    to_allctd.display()
    to_allctd_df = to_allctd.toPandas()

    return to_allctd, to_allctd_df

# COMMAND ----------

def chk_size_combi_allctd(n: int,
                          rank_map_table: str,
                          allocated_table: str,
                          reset: bool = False
                         ):
    """Check if n_gr (size of combination) still available for allocation
    Param
    -----
    n : size of combination 
    rank_map_table : original rank map table
    allocated_table : result of allcation, stampped column "ca_nm"
    reset : if True, will start over from rank map table
    """
    if not reset:
        try:
            allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, allocated_table))
        except Exception as e:
            allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, rank_map_table)).withColumn("ca_nm", F.lit(None))
    else:
        allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, rank_map_table)).withColumn("ca_nm", F.lit(None))
        
    n_avlbl = allctd.where(F.col("ca_nm").isNull()).where(F.col("n_gr")==n).count()
    
    return n_avlbl

# COMMAND ----------

def update_allocation(allocated_sf: SparkDataFrame,
                      allocated_table: SparkDataFrame,
                     ):
    """Update allocation based on result of allocated_sf
    """
    allctd = allocated_table
    with_ca = allctd.where(F.col("ca_nm").isNotNull())
    no_ca = allctd.where(F.col("ca_nm").isNull())
    
    # update allocation 
    fill_ca = \
    (no_ca.alias("a")
     .join(allocated_sf.select("household_id", "alloc_ca_nm").alias("b"), "household_id", "left")
     .withColumn("ca_nm", F.coalesce(F.col("b.alloc_ca_nm"), F.col("a.ca_nm")))
     .drop("alloc_ca_nm")
    )

    # Create new allctd
    new_allctd = with_ca.unionByName(fill_ca)
    
    return new_allctd

# COMMAND ----------

def allocation_ca_nm(n: int,
                     to_alloctd_df: PandasDataFrame,
                     rank_map_table: str,
                     allocated_table: str,
                     reset: bool = False
                    ):
    """
    Param
    -----
    n : size of combination 
    to_alloctd_df : Order of allocation
    rank_map_table : original rank map table
    allocated_table : result of allcation, stampped column "ca_nm"
    reset : if True, will start over from rank map table
    """
    if not reset:
        try:
            allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, allocated_table))
        except Exception as e:
            new_allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, rank_map_table)).withColumn("ca_nm", F.lit(None).cast(T.StringType()))
            new_allctd.write.format('parquet').mode("overwrite").save(os.path.join(prjct_abfss_prefix, allocated_table))
            allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, allocated_table))
    else:
        print(f"Reset allocated table, deleting allocated table at : {os.path.join(prjct_abfss_prefix, allocated_table)}")
        dbutils.fs.rm(os.path.join(prjct_abfss_prefix, allocated_table), True)
        new_allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, rank_map_table)).withColumn("ca_nm", F.lit(None).cast(T.StringType()))
        new_allctd.write.format('parquet').mode("overwrite").save(os.path.join(prjct_abfss_prefix, allocated_table))
        allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, allocated_table))
    
    n_gr = n
    step_allctd_sfs = []

    for r in to_allctd_df.itertuples():
        print(f"Combination size : {n_gr}")
        print(f"'{r.gr_nm}' => '{r.ca_nm}' target to allocate {r.to_alloc:,d}")
        
        # reload allocated from previous loop
        allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, allocated_table))

        filtered_key = \
        (allctd
         .where(F.col("ca_nm").isNull())
         .where(F.col("n_gr")==n_gr)
         # filter map with specific key value
         .withColumn("filter_map", F.map_filter("gr_rank_map", lambda k, v : k == r.gr_nm))
         .where(F.size("filter_map")>0)
        )
        
        print(f"gr_nm '{r.gr_nm}' have {filtered_key.count():,d} records")
        rank = \
        (filtered_key
         # extract value from map pair 
         .withColumn("rank", F.explode(F.map_values(F.col("filter_map"))))
         .orderBy(F.col("rank"))
         .limit(r.to_alloc)
         .withColumn("alloc_ca_nm", F.lit(r.ca_nm))
        )
        
        step_allctd_sfs.append(rank)

        # update allocation to allocated table
        new_allocated = update_allocation(rank, allctd)
        new_allocated.write.format('parquet').mode("overwrite").save(os.path.join(prjct_abfss_prefix, allocated_table))
        
    return step_allctd_sfs

# COMMAND ----------

# MAGIC %md ##---- Main ----

# COMMAND ----------

# MAGIC %md ### Combination 1 : Exclusive Group

# COMMAND ----------

#---- Reset all allocation to n combination =1 
n = 1
print(f"Availble combination size (n_gr) : {n} to allocated {chk_size_combi_allctd(n, rank_map_table=rank_map_table,allocated_table=alloctd_table , reset=True):,d}")
to_allctd, to_allctd_df = get_to_alloc(alloc_spc=alloc_spc, rank_map_table=rank_map_table, allocated_table=alloctd_table, reset=True)

# COMMAND ----------

step = allocation_ca_nm(n=n, to_alloctd_df=to_allctd_df, rank_map_table=rank_map_table, allocated_table=alloctd_table, reset=True)

# COMMAND ----------

# check allocation table
allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, alloctd_table))
allctd.groupBy("ca_nm").count().display()

# COMMAND ----------

allctd.where(F.col("ca_nm")=="1hygiene").withColumn("rank", F.explode(F.map_values("gr_rank_map"))).orderBy(F.col("rank")).display()

# COMMAND ----------

# MAGIC %md ### Combination 2 : Repertoire

# COMMAND ----------

n = 2
print(f"Availble combination size (n_gr) : {n} to allocated {chk_size_combi_allctd(n, rank_map_table=rank_map_table,allocated_table=alloctd_table , reset=False):,d}")
to_allctd, to_allctd_df = get_to_alloc(alloc_spc=alloc_spc, rank_map_table=rank_map_table, allocated_table=alloctd_table, reset=False)

# COMMAND ----------

step = allocation_ca_nm(n=n, to_alloctd_df=to_allctd_df, rank_map_table=rank_map_table, allocated_table=alloctd_table, reset=False)

# COMMAND ----------

allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, alloctd_table))
allctd.groupBy("ca_nm").count().display()

# COMMAND ----------

# MAGIC %md ## Combination 3

# COMMAND ----------

n = 3
print(f"Availble combination size (n_gr) : {n} to allocated {chk_size_combi_allctd(n, rank_map_table=rank_map_table,allocated_table=alloctd_table , reset=False):,d}")
to_allctd, to_allctd_df = get_to_alloc(alloc_spc=alloc_spc, rank_map_table=rank_map_table, allocated_table=alloctd_table, reset=False)

# COMMAND ----------

step = allocation_ca_nm(n=n, to_alloctd_df=to_allctd_df, rank_map_table=rank_map_table, allocated_table=alloctd_table, reset=False)

# COMMAND ----------

allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, alloctd_table))
allctd.groupBy("ca_nm").count().display()

# COMMAND ----------

# MAGIC %md ## Combination 4

# COMMAND ----------

n = 4
print(f"Availble combination size (n_gr) : {n} to allocated {chk_size_combi_allctd(n, rank_map_table=rank_map_table,allocated_table=alloctd_table , reset=False):,d}")
to_allctd, to_allctd_df = get_to_alloc(alloc_spc=alloc_spc, rank_map_table=rank_map_table, allocated_table=alloctd_table, reset=False)

# COMMAND ----------

step = allocation_ca_nm(n=n, to_alloctd_df=to_allctd_df, rank_map_table=rank_map_table, allocated_table=alloctd_table, reset=False)

# COMMAND ----------

allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, alloctd_table))
allctd.groupBy("ca_nm").count().display()

# COMMAND ----------

# MAGIC %md -----

# COMMAND ----------

#---- Combine allocation check
from functools import reduce
ca_step = reduce(SparkDataFrame.unionByName, step)
ca_step.printSchema()

# COMMAND ----------

# MAGIC %md -----

# COMMAND ----------

try:
    allctd = spark.read.load(os.path.join(prjct_abfss_prefix, "allctd.delta"))
except Exception as e:
    allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, "gr_nm_list_sales_rank_map.parquet")).withColumn("ca_nm", F.lit(None))

n_gr = 1
allctd_sfs = []

for r in to_allctd_df.itertuples():
    print(f"Combination lv {n_gr}")
    print(f"'{r.gr_nm}' => '{r.ca_nm}' target to allocate {r.to_alloc:,d}")
    filtered_key = \
    (allctd
     .where(F.col("ca_nm").isNull())
     .where(F.col("n_gr")==n_gr)
     # filter map with specific key value
     .withColumn("filter_map", F.map_filter("gr_rank_map", lambda k, v : k == r.gr_nm))
     .where(F.size("filter_map")>0)
    )
    print(f"gr_nm '{r.gr_nm}' have {filtered_key.count():,d} records")
    rank = \
    (filtered_key
     # extract value from map pair 
     .withColumn("rank", F.explode(F.map_values(F.col("filter_map"))))
     .orderBy(F.col("rank"))
     .limit(r.to_alloc)
     .withColumn("alloc_ca_nm", F.lit(r.ca_nm))
    )
    allctd_sfs.append(rank)

# COMMAND ----------

#---- Combine allocation
from functools import reduce
sngl_ca = reduce(SparkDataFrame.unionByName, allctd_sfs)

#---- Update no_ca with allocation  
try:
    allctd = spark.read.load(os.path.join(prjct_abfss_prefix, "allctd.delta"))
    with_ca = allctd.where(F.col("ca_nm").isNotNull())
    no_ca = allctd.where(F.col("ca_nm").isNull())
except Exception as e:
    allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, "gr_nm_list_sales_rank_map.parquet")).withColumn("ca_nm", F.lit(None))
    with_ca = allctd.where(F.col("ca_nm").isNotNull())
    no_ca = allctd.where(F.col("ca_nm").isNull())

fill_ca = \
(no_ca.alias("a")
 .join(sngl_ca.select("household_id", "alloc_ca_nm").alias("b"), "household_id", "left")
 .withColumn("ca_nm", F.coalesce(F.col("b.alloc_ca_nm"), F.col("a.ca_nm")))
 .drop("alloc_ca_nm")
)

#---- Create new allctd
new_allctd = with_ca.unionByName(fill_ca)

new_allctd.write.format('delta').mode("overwrite").save(os.path.join(prjct_abfss_prefix, "allctd.delta"))

# COMMAND ----------

allctd = spark.read.load(os.path.join(prjct_abfss_prefix, "allctd.delta"))

# COMMAND ----------

allctd.groupBy("ca_nm").count().display()

# COMMAND ----------

chk_n_gr_allctd(2)

# COMMAND ----------

to_allctd, to_allctd_df = get_to_alloc(alloc_spc=alloc_spc)

# COMMAND ----------

# To fix with update "allctd.delta" in each gr allocation
try:
    allctd = spark.read.load(os.path.join(prjct_abfss_prefix, "allctd.delta"))
except Exception as e:
    allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, "gr_nm_list_sales_rank_map.parquet")).withColumn("ca_nm", F.lit(None))

n_gr = 2
allctd_sfs = []

for r in to_allctd_df.itertuples():
    print(f"Combination lv {n_gr}")
    print(f"'{r.gr_nm}' => '{r.ca_nm}' target to allocate {r.to_alloc:,d}")
    filtered_key = \
    (allctd
     .where(F.col("ca_nm").isNull())
     .where(F.col("n_gr")==n_gr)
     # filter map with specific key value
     .withColumn("filter_map", F.map_filter("gr_rank_map", lambda k, v : k == r.gr_nm))
     .where(F.size("filter_map")>0)
    )
    print(f"gr_nm '{r.gr_nm}' have {filtered_key.count():,d} records")
    rank = \
    (filtered_key
     # extract value from map pair 
     .withColumn("rank", F.explode(F.map_values(F.col("filter_map"))))
     .orderBy(F.col("rank"))
     .limit(r.to_alloc)
     .withColumn("alloc_ca_nm", F.lit(r.ca_nm))
    )
    allctd_sfs.append(rank)

# COMMAND ----------

from functools import reduce
bi_ca = reduce(SparkDataFrame.unionByName, allctd_sfs)

#---- Update no_ca with allocation  
try:
    allctd = spark.read.load(os.path.join(prjct_abfss_prefix, "allctd.delta"))
    with_ca = allctd.where(F.col("ca_nm").isNotNull())
    no_ca = allctd.where(F.col("ca_nm").isNull())
except Exception as e:
    allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, "gr_nm_list_sales_rank_map.parquet")).withColumn("ca_nm", F.lit(None))
    with_ca = allctd.where(F.col("ca_nm").isNotNull())
    no_ca = allctd.where(F.col("ca_nm").isNull())

fill_ca = \
(no_ca.alias("a")
 .join(bi_ca.select("household_id", "alloc_ca_nm").alias("b"), "household_id", "left")
 .withColumn("ca_nm", F.coalesce(F.col("b.alloc_ca_nm"), F.col("a.ca_nm")))
 .drop("alloc_ca_nm")
)

#---- Create new allctd
new_allctd = with_ca.unionByName(fill_ca)

new_allctd.write.format('delta').mode("overwrite").save(os.path.join(prjct_abfss_prefix, "allctd.delta"))

# COMMAND ----------

new_allctd.groupBy("ca_nm").count().display()

# COMMAND ----------

new_allctd.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

chk_n_gr_allctd(2)

# COMMAND ----------

#---- Allocation , sort by rest of ca_nm
x = "downy_reg"
g = "2downy_reg"
l = 200

(alloc_pool
 .where(F.col("n_gr")==2)
 # filter map with specific key value
 .withColumn("filter_map", F.map_filter("gr_rank_map", lambda k, v : k == x))
 .where(F.size("filter_map")>0)
 # extract value from map pair 
 .withColumn("rank", F.explode(F.map_values(F.col("filter_map"))))
 .orderBy(F.col("rank"))
 .limit(l)
 .withColumn("ca_nm", F.lit(g))
).display()

# COMMAND ----------

n_gr = 1
sngl_sfs = []
for r in alloc_spc_df.itertuples():
    print("Combination lv 2")
    print(r.gr_nm, r.n_alloc, r.ca_nm)
    out = \
    (alloc_pool
     .where(F.col("n_gr")==n_gr)
     # filter map with specific key value
     .withColumn("filter_map", F.map_filter("gr_rank_map", lambda k, v : k == r.gr_nm))
     .where(F.size("filter_map")>0)
     # extract value from map pair 
     .withColumn("rank", F.explode(F.map_values(F.col("filter_map"))))
     .orderBy(F.col("rank"))
     .limit(r.n_alloc)
     .withColumn("ca_nm", F.lit(r.ca_nm))
    )
    
    sngl_sfs.append(out)

# COMMAND ----------

from functools import reduce
sngl_ca = reduce(SparkDataFrame.unionByName, sngl_sfs)
sngl_ca.display()

# COMMAND ----------

def get_overlap_set(gr_nm, ca_spc):
    """
    """
    overlap = \
    (gr_nm
     .join(ca_spc)
     .withColumn("overlap", F.arrays_overlap("gr_nm_set", "ca_gr_nm_set"))
     .where(F.col("overlap"))
     .drop("overlap", "n_ca")
    )
    
    return overlap

# COMMAND ----------

ovrlp = get_overlap_set(gr_nm_set, ca_spc)

# COMMAND ----------

ovrlp.where(F.col("gr_set_size")==2).display()

# COMMAND ----------

single = \
(hde_gofresh_str_prd
 .join(cnddt_allc_spec_single, "str_prd_set", "inner")
 .select("household_id", "gr_alloc_nm", "str_prd_set")
 .withColumn("str_prd_gr", F.explode("str_prd_set"))
 .select("household_id", "gr_alloc_nm", "str_prd_gr")
)
single.display()

# COMMAND ----------

single.groupBy("gr_alloc_nm").count().display()

# COMMAND ----------

# MAGIC %md ##Map candidate group rank

# COMMAND ----------

hde_gofresh_str_prd_rank = spark.read.parquet(os.path.join(project_prefix, "hde_gofresh_str_prd_sales_rank.parquet"))
hde_gofresh_str_prd_rank.display()

# COMMAND ----------

hde_gofresh_str_prd_rank.where(F.col("str_prd_gr")=="Competitor Nescafe_HDE").count()

# COMMAND ----------

single_rnk = \
(single
 .join(hde_gofresh_str_prd_rank, ["household_id", "str_prd_gr"], "inner")
)
single_rnk.groupBy("gr_alloc_nm").count().display()

# COMMAND ----------

# DBTITLE 1,Check limit & tail result
# Check head / 
single_rnk.where(F.col("gr_alloc_nm")=="8_CompetitorNescafe_HDE").orderBy(F.col("sales_rank_in_gr")).limit(100).tail(10)

# COMMAND ----------

# MAGIC %md ##Limit number by spec

# COMMAND ----------

target_spec = [
{'gr_alloc_nm':'10_Nescafe_GoFresh','limit':128000},
{'gr_alloc_nm':'11_NescafeGold_GoFresh','limit':36000},
{'gr_alloc_nm':'12_PurelifeMinere_GoFresh','limit':63000},
{'gr_alloc_nm':'13_Milo_GoFresh','limit':45000},
{'gr_alloc_nm':'14_MultiBrand_GoFresh','limit':45000},
{'gr_alloc_nm':'2_Nescafe_HDE','limit':264000},
{'gr_alloc_nm':'3_NescafeGold_HDE','limit':45000},
{'gr_alloc_nm':'4_PurelifeMinere_HDE','limit':120000},
{'gr_alloc_nm':'5_Milo_HDE','limit':45000},
{'gr_alloc_nm':'6_Nestvita_HDE','limit':45000},
{'gr_alloc_nm':'7_MultiBrand_HDE','limit':45000},
{'gr_alloc_nm':'8_CompetitorNescafe_HDE','limit':50000},
{'gr_alloc_nm':'9_CompetitorMilo_HDE','limit':50000},
]

def limit_gr_rank(sf, spec):
    """
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    limited_sf = spark.createDataFrame([], sf.schema)
    
    for s in spec:
        print(f'{s["gr_alloc_nm"]} limit {s["limit"]}')
        gr_sf = sf.where(F.col("gr_alloc_nm")==s["gr_alloc_nm"]).orderBy(F.col("sales_rank_in_gr")).limit(s["limit"])
        gr_sf = gr_sf.checkpoint()
        limited_sf = limited_sf.unionByName(gr_sf)
        limited_sf = limited_sf.checkpoint()
    
    return limited_sf

# COMMAND ----------

limited_sf = limit_gr(single_rnk, target_spec)

# COMMAND ----------

limited_sf.groupBy("gr_alloc_nm").count().display()

# COMMAND ----------

limited_sf.groupBy("gr_alloc_nm").agg(F.min("sales_rank_in_gr"), F.max("sales_rank_in_gr"), F.count("*")).display()

# COMMAND ----------

single_trprce = limited_sf.join(last_6mth_cc.select("household_id", "truprice_seg_desc").drop_duplicates(), "household_id", "left")

# COMMAND ----------

single_trprce.groupBy("gr_alloc_nm").count().display()

# COMMAND ----------

(single_trprce
 .write
 .format("parquet")
 .mode("overwrite")
 .save(os.path.join(ca_prefix, "single_trprce.parquet"))
)

# COMMAND ----------

single_trprce = spark.read.parquet(os.path.join(ca_prefix, "single_trprce.parquet"))

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

test, ctrl = statify_2_step(single_trprce, "gr_alloc_nm", "truprice_seg_desc", fraction_dict=fraction)

# COMMAND ----------

test.groupBy("gr_alloc_nm").count().display()

# COMMAND ----------

ctrl.groupBy("gr_alloc_nm").count().display()

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

export_path = "/dbfs/FileStore/thanakrit/temp/nestle_cgcn"
map_group_golden_to_csv(ctrl, "gr_alloc_nm", dbfs_python_prefix=export_path, file_nm_suffix="ctrl")

# COMMAND ----------

export_path = "/dbfs/FileStore/thanakrit/temp/nestle_cgcn"
map_group_golden_to_csv(test, "gr_alloc_nm", dbfs_python_prefix=export_path, file_nm_suffix="test")

# COMMAND ----------


