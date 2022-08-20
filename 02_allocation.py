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

x = 1
y = 0
p = 1 if y == 0 else x/y
p

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
    reset : if True, will start over from rank map table, and re-create allocated_table with prop to allocation based on inexclusive count
    and order allocation in ascending order of prop_to_alloc_inexc
    """
    
    if not reset:
        allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, allocated_table))
        n_allctd = allctd.groupBy("ca_nm").agg(F.count("*").alias("n_allctd"))

        to_allctd = \
        (alloc_spc
         .join(n_allctd, "ca_nm", "left")
         .fillna(0, subset="n_allctd")
         .withColumn("to_alloc", F.col("n_ca") - F.col("n_allctd"))
         .withColumn("prop_to_alloc", F.col("to_alloc")/F.col("n_ca"))
         .orderBy(F.col("prop_to_alloc").desc(), F.col("ca_nm"))
        )
    else:
        print(f"Reset and create new allocated table {rank_map_table}, order to allocation (prop_to_alloc) base in exclusive count")
        allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, rank_map_table)).withColumn("ca_nm", F.lit(None))
    
        n_allctd = allctd.groupBy("ca_nm").agg(F.count("*").alias("n_allctd"))
        
        inexclsve_count = \
        (allctd.select("household_id", F.explode("gr_nm_list").alias("gr_nm"))
         .groupBy("gr_nm")
         .agg(F.count_distinct("household_id").alias("n_inexc"))
        )
        
        to_allctd = \
        (alloc_spc
         .join(n_allctd, "ca_nm", "left")
         .fillna(0, subset="n_allctd")
         .withColumn("to_alloc", F.col("n_ca") - F.col("n_allctd"))
         .join(inexclsve_count, "gr_nm", "left")
         .fillna(0, subset="n_inexc")
         .withColumn("prop_to_alloc_inexc", F.col("n_inexc")/F.col("n_ca"))
         .orderBy(F.col("prop_to_alloc_inexc"), F.col("ca_nm"))
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

# MAGIC %md ### Initial with inexlusive proportion

# COMMAND ----------

to_allctd, to_allctd_df = get_to_alloc(alloc_spc=alloc_spc, rank_map_table=rank_map_table, allocated_table=alloctd_table, reset=True)

# COMMAND ----------

# MAGIC %md ### Combination 1 : Exclusive Group

# COMMAND ----------

#---- Reset all allocation to n combination =1 
n = 1

print(f"Availble combination size (n_gr) : {n} to allocated {chk_size_combi_allctd(n, rank_map_table=rank_map_table,allocated_table=alloctd_table , reset=False):,d}")
# to_allctd, to_allctd_df = get_to_alloc(alloc_spc=alloc_spc, rank_map_table=rank_map_table, allocated_table=alloctd_table, reset=False)

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


