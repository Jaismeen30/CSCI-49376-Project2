from pyspark.sql import SparkSession, functions as F, Row
from operator import add

def init_spark():
    return SparkSession.builder.getOrCreate()

def query_top5_compounds_with_genes(spark, use_metaedge=True):
    nodes_df = (spark.read.option("header", True).option("delimiter","\t").csv("/Users/jaismeenkaur/CSCI-49376-Project2/nodes.tsv")
             .select("id", "name","kind"))
    edges_df = (spark.read.option("header", True).option("delimiter","\t").csv("/Users/jaismeenkaur/CSCI-49376-Project2/edges.tsv")
             .select("source","target","metaedge"))


    compounds_df = nodes_df.filter(F.col("kind")=="Compound").select(F.col("id").alias("compound_id"))

    edges_by_src = edges_df.rdd.map(lambda r: (r["source"], r["target"]))  

    compounds_rdd = compounds_df.rdd.map(lambda r: (r["compound_id"], None))      


    # (compound_id, neighbor)
    compound_edges_rdd = edges_by_src.join(compounds_rdd).map(lambda kv: (kv[0], kv[1][0]))

    # Map to (compound_id, neighbor_type)
    def neighbor_type(neighbor):
        if neighbor.startswith("Gene"):    
            return "gene"
        if neighbor.startswith("Disease"): 
            return "disease"
        return None

    typed_rdd = (compound_edges_rdd
                 .map(lambda x: (x[0], neighbor_type(x[1])))
                 .filter(lambda x: x[1] is not None)) 

    # Combiner step

    by_type_counts = (typed_rdd.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(add))  

    # Reducer step
    def seq(acc, kv):
        typ, cnt = kv
        if typ == "gene":    
            return (acc[0] + cnt, acc[1])
        else:                
            return (acc[0], acc[1] + cnt)

    def comb(a, b): 
        return (a[0]+b[0], a[1]+b[1])

    per_compound = (by_type_counts.map(lambda kv: (kv[0][0], (kv[0][1], kv[1]))).aggregateByKey((0,0), seq, comb))           

    
    pivot_df = spark.createDataFrame(
        per_compound.map(lambda kv: Row(compound_id=kv[0], gene=kv[1][0], disease=kv[1][1]))
    )


    order_df = pivot_df.orderBy(F.desc("gene"), F.desc("disease"), F.asc("compound_id")).limit(5)

   
    order_df = (order_df.join(nodes_df.select(F.col("id").alias("cid"), "name"), order_df.compound_id == F.col("cid"), "left").drop("cid"))


    top5_df = order_df.select("compound_id", "gene", "disease") 
    return top5_df

def query2(spark,use_metaedge=True):
    nodes_df = spark.read.option("delimiter", "\t").option("header", True).csv("/Users/jaismeenkaur/CSCI-49376-Project2/nodes.tsv").select("id","name", "kind")
    edges_df = spark.read.option("delimiter", "\t").option("header", True).csv("/Users/jaismeenkaur/CSCI-49376-Project2/edges.tsv").select("source", "target", "metaedge")
   
       # Filter
    if use_metaedge and "metaedge" in edges_df.columns:
        cd_src = edges_df.filter(F.col("metaedge").isin("CtD", "CpD"))
    else:
        cd_src = (edges_df
                  .filter(F.col("source").startswith("Compound"))
                  .filter(F.col("target").startswith("Disease")))


    diseases = nodes_df.filter(F.col("kind") == "Disease") \
                       .select(F.col("id").alias("disease_id"),"name")

    dc_df = (cd_src.join(diseases, cd_src.target == diseases.disease_id, "inner")
                  .select(F.col("disease_id"), F.col("source").alias("compound_id"))
                  .dropDuplicates()) 

 # MapReduce (pairs) 
    rdd = dc_df.rdd  # (disease_id, compound_id)


    # per-disease counts
    per_disease_counts = (rdd
        .map(lambda r: (r["disease_id"], 1))
        .reduceByKey(add))   # (disease_id, drugs_per_disease)


    counts = (per_disease_counts
        .map(lambda kv: (kv[1], 1))     # (drugs_per_disease, 1)
        .reduceByKey(add))              # (drugs_per_disease, num_diseases)

    # top-5 
    top5 = counts.takeOrdered(5, key=lambda kv: (-kv[1], kv[0]))
    return spark.createDataFrame(top5, ["drugs_per_disease","num_diseases"])

    return top5_df

def query_compound_names_from_top5(spark):
    nodes_df = (spark.read.option("header", True).option("delimiter","\t").csv("/Users/jaismeenkaur/CSCI-49376-Project2/nodes.tsv")
             .select("id", "name","kind"))
    edges_df = (spark.read.option("header", True).option("delimiter","\t").csv("/Users/jaismeenkaur/CSCI-49376-Project2/edges.tsv")
             .select("source","target","metaedge"))
    top5_compounds = query_top5_compounds_with_genes(spark)

    result = top5_compounds.join(nodes_df.select("id", "name"), top5_compounds.compound_id == nodes_df.id) \
                           .select("name", "compound_id", "gene", "disease") \
                           .orderBy(F.desc("gene"))
    return result.select("name", "gene")
def main():
    spark = init_spark()
    print("\nSelect a query to run:")
    print("1 - Top 5 compounds with most gene associations")
    print("2 - Top 5 diseases with most compound associations")
    print("3 - Compound names from top 5 gene-associated compounds")
 


 
    choice = input("Enter your choice (1/2/3): ")

    try:
        if choice == "1":
            result = query_top5_compounds_with_genes(spark)
        elif choice == "2":
                result = query2(spark)
        elif choice == "3":
            result = query_compound_names_from_top5(spark)
        else:
            raise ValueError("Invalid choice. Please enter 1, 2, or 3.")
    except Exception as e:
        print(f"Error: {e}")
    else:
        result.show(truncate=False)

if __name__ == "__main__":
    main()