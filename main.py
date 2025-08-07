from pyspark.sql import SparkSession, functions as F

def init_spark():
    return SparkSession.builder.getOrCreate()

def query_top5_compounds_with_genes(spark):
    nodes_df = spark.table("workspace.default.nodes")
    edges_df = spark.table("workspace.default.edges")

    compounds_df = nodes_df.filter(F.col("kind") == "Compound").select(F.col("id").alias("compound_id"))

    compound_edges = edges_df.join(compounds_df, edges_df.source == compounds_df.compound_id) \
                             .select(F.col("compound_id"), F.col("target").alias("neighbor"))

    compound_edges = compound_edges.withColumn(
        "neighbor_type",
        F.when(F.col("neighbor").startswith("Gene"), F.lit("gene"))
         .when(F.col("neighbor").startswith("Disease"), F.lit("disease"))
    ).filter(F.col("neighbor_type").isNotNull())

    counts_df = compound_edges.groupBy("compound_id", "neighbor_type") \
                              .agg(F.count("neighbor").alias("count"))

    pivot_df = counts_df.groupBy("compound_id") \
                        .pivot("neighbor_type", ["gene", "disease"]) \
                        .sum("count").fillna(0)

    result = pivot_df.orderBy(F.desc("gene")).limit(5)
    return result

def query_top5_diseases_with_compounds(spark):
    nodes_df = spark.table("workspace.default.nodes")
    edges_df = spark.table("workspace.default.edges")

    diseases_df = nodes_df.filter(F.col("kind") == "Disease").select(F.col("id").alias("disease_id"), "name")

    disease_edges = edges_df \
        .filter(F.col("target").startswith("Disease")) \
        .filter(F.col("source").startswith("Compound")) \
        .select(F.col("target").alias("disease_id"), F.col("source").alias("compound_id"))

    partial_counts = disease_edges.groupBy("disease_id", "compound_id").count()

    disease_compound_counts = partial_counts.groupBy("disease_id") \
                                            .agg(F.count("compound_id").alias("compound_count"))

    result = disease_compound_counts.join(diseases_df, "disease_id") \
                                    .select("name", "disease_id", "compound_count") \
                                    .orderBy(F.desc("compound_count")).limit(5)
    return result

def query_compound_names_from_top5(spark):
    nodes_df = spark.table("workspace.default.nodes")
    top5_compounds = query_top5_compounds_with_genes(spark)

    result = top5_compounds.join(nodes_df.select("id", "name"), top5_compounds.compound_id == nodes_df.id) \
                           .select("name", "compound_id", "gene", "disease") \
                           .orderBy(F.desc("gene"))
    return result.select("name")

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
            result = query_top5_diseases_with_compounds(spark)
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