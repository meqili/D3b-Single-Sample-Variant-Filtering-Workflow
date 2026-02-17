import argparse
from argparse import RawTextHelpFormatter
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import glow

# CLI setup
parser = argparse.ArgumentParser(
    description='Script for gene-based variant filtering and annotation.\n\
    MUST BE RUN WITH spark-submit. Example:\n\
    spark-submit --driver-memory 10G annotate_variant_gene_group.py',
    formatter_class=RawTextHelpFormatter
)

parser.add_argument('--input_file', required=True, help='Single Sample Workflow output (.tsv.gz)')
parser.add_argument('--cpg_gene', required=True, help='Cancer Predisposition Genes (.tsv.gz)')
parser.add_argument('--sfg_gene', required=True, help='Secondary Finding Genes (.tsv)')
parser.add_argument('--output_basename', required=True, help='Basename for output file (output will be .gene_group.tsv.gz)')
parser.add_argument('--spark_executor_mem', type=int, default=4, help='Spark executor memory in GB')
parser.add_argument('--spark_executor_instance', type=int, default=1, help='Number of Spark executor instances')
parser.add_argument('--spark_executor_core', type=int, default=1, help='Number of Spark executor cores')
parser.add_argument('--spark_driver_maxResultSize', type=int, default=1, help='Spark driver max result size in GB')
parser.add_argument('--sql_broadcastTimeout', type=int, default=300, help='Spark SQL broadcast timeout in seconds')
parser.add_argument('--spark_driver_core', type=int, default=1, help='Number of Spark driver cores')
parser.add_argument('--spark_driver_mem', type=int, default=4, help='Spark driver memory in GB')
args = parser.parse_args()

# Create SparkSession
spark = SparkSession \
    .builder \
    .appName('glow_pyspark') \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0,io.projectglow:glow-spark3_2.12:2.0.0') \
    .config('spark.jars.excludes', 'org.apache.hadoop:hadoop-client,io.netty:netty-all,io.netty:netty-handler,io.netty:netty-transport-native-epoll') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config('spark.sql.debug.maxToStringFields', '0') \
    .config('spark.hadoop.io.compression.codecs', 'io.projectglow.sql.util.BGZFCodec') \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider') \
    .config('spark.kryoserializer.buffer.max', '512m') \
    .config('spark.executor.memory', f'{args.spark_executor_mem}G') \
    .config('spark.executor.instances', args.spark_executor_instance) \
    .config('spark.executor.cores', args.spark_executor_core) \
    .config('spark.driver.maxResultSize', f'{args.spark_driver_maxResultSize}G') \
    .config('spark.sql.broadcastTimeout', args.sql_broadcastTimeout) \
    .config('spark.driver.cores', args.spark_driver_core) \
    .config('spark.driver.memory', f'{args.spark_driver_mem}G') \
    .getOrCreate()

# Register Glow
spark = glow.register(spark)

# Load input data
variant_list = spark.read.format('csv') \
    .options(compression='gzip', delimiter='\t', header=True) \
    .load(args.input_file)

cpg_gene = spark.read.format('csv') \
    .options(delimiter='\t', header=True) \
    .load(args.cpg_gene)

sfg_gene = spark.read.format('csv') \
    .options(delimiter='\t', header=True) \
    .load(args.sfg_gene)

# Ensure consistent types
variant_list = variant_list \
    .withColumn("entrez_gene_id", F.col("entrez_gene_id").cast(StringType())) \
    .withColumn("CSQ_SYMBOL", F.col("CSQ_SYMBOL").cast(StringType())) \
    .withColumn("hgnc", F.col("hgnc").cast(StringType()))

# Collect distinct sets
cpg_symbol = set(cpg_gene.select("symbol").distinct().rdd.map(lambda r: r[0]).collect())
cpg_hgnc   = set(cpg_gene.select("hgnc_id").distinct().rdd.map(lambda r: r[0]).collect())
cpg_entrez = set(cpg_gene.select("entrez_id").distinct().rdd.map(lambda r: r[0]).collect())

sfg_symbol = set(sfg_gene.select("symbol").distinct().rdd.map(lambda r: r[0]).collect())
sfg_hgnc   = set(sfg_gene.select("hgnc_id").distinct().rdd.map(lambda r: r[0]).collect())
sfg_entrez = set(sfg_gene.select("entrez_id").distinct().rdd.map(lambda r: r[0]).collect())

# Broadcast sets
cpg_symbol_bc = spark.sparkContext.broadcast(cpg_symbol)
cpg_hgnc_bc   = spark.sparkContext.broadcast(cpg_hgnc)
cpg_entrez_bc = spark.sparkContext.broadcast(cpg_entrez)

sfg_symbol_bc = spark.sparkContext.broadcast(sfg_symbol)
sfg_hgnc_bc   = spark.sparkContext.broadcast(sfg_hgnc)
sfg_entrez_bc = spark.sparkContext.broadcast(sfg_entrez)

# Compute classification flags using built-in PySpark functions
variant_list = variant_list \
    .withColumn(
        "cpg_flag",
        (F.col("entrez_gene_id").isin(cpg_entrez_bc.value) |
         F.col("hgnc").isin(cpg_hgnc_bc.value))
    ) \
    .withColumn(
        "sfg_flag",
        (F.col("entrez_gene_id").isin(sfg_entrez_bc.value) |
         F.col("hgnc").isin(sfg_hgnc_bc.value))
    )

# Fallback to symbol if neither entrez/hgnc matched
variant_list = variant_list \
    .withColumn(
        "cpg_final",
        F.when(
            (~F.col("cpg_flag") & ~F.col("sfg_flag") &
             F.col("CSQ_SYMBOL").isin(cpg_symbol_bc.value)),
            True
        ).otherwise(F.col("cpg_flag"))
    ) \
    .withColumn(
        "sfg_final",
        F.when(
            (~F.col("cpg_flag") & ~F.col("sfg_flag") &
             F.col("CSQ_SYMBOL").isin(sfg_symbol_bc.value)),
            True
        ).otherwise(F.col("sfg_flag"))
    )

# Create final gene_group label
variant_list = variant_list.withColumn(
    "gene_group",
    F.when(F.col("cpg_final") & F.col("sfg_final"), "ECPG/EMGP")
     .when(F.col("cpg_final"), "ECPG")
     .when(F.col("sfg_final"), "EMGP")
     .otherwise("-")
).drop("cpg_flag", "sfg_flag", "cpg_final", "sfg_final")

# Save to gzipped TSV
output_file = args.output_basename + '.gene_group.tsv.gz'
variant_list \
    .toPandas() \
    .to_csv(output_file, sep="\t", index=False, na_rep='-', compression='gzip')