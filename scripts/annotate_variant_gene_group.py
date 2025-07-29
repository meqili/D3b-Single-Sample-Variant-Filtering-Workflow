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
# Register so that glow functions like read vcf work with spark. Must be run in spark shell or in context described in help
spark = glow.register(spark)

# Load input data
variant_list = spark.read.format('csv') \
    .options(compression='gzip', delimiter='\t', header=True) \
    .load(args.input_file)

cpg_gene = spark.read.format('csv') \
    .options(compression='gzip', delimiter='\t', header=True) \
    .load(args.cpg_gene)

sfg_gene = spark.read.format('csv') \
    .options(delimiter='\t', header=True) \
    .load(args.sfg_gene)

# Ensure consistent types
variant_list = variant_list \
    .withColumn("entrez_gene_id", F.col("entrez_gene_id").cast(StringType())) \
    .withColumn("CSQ_Gene", F.col("CSQ_Gene").cast(StringType()))

cpg_gene = cpg_gene.withColumn("entrez_id", F.col("entrez_id").cast(StringType()))
sfg_gene = sfg_gene.withColumn("Gene", F.col("Gene").cast(StringType()))

# Collect as sets
cpg_ids = cpg_gene.select("entrez_id").rdd.flatMap(lambda x: x).collect()
sfg_symbols = sfg_gene.select("Gene").rdd.flatMap(lambda x: x).collect()

# Broadcast
cpg_broadcast = spark.sparkContext.broadcast(set(cpg_ids))
sfg_broadcast = spark.sparkContext.broadcast(set(sfg_symbols))

def classify_gene_group(entrez_id, gene_symbol):
    if entrez_id is None and gene_symbol is None:
        return "-"
    cpg = entrez_id in cpg_broadcast.value
    sfg = gene_symbol in sfg_broadcast.value
    if cpg and sfg:
        return "CPG/SFG"
    elif cpg:
        return "CPG"
    elif sfg:
        return "SFG"
    else:
        return "-"

group_udf = F.udf(classify_gene_group, StringType())

# Annotate
variant_list = variant_list.withColumn(
    "gene_group",
    group_udf(F.col("entrez_gene_id"), F.col("CSQ_Gene"))
)

# Save to gzipped TSV
output_file = args.output_basename + '_gene_group.tsv.gz'
variant_list \
    .toPandas() \
    .to_csv(output_file, sep="\t", index=False, na_rep='-', compression='gzip')
print(f"[DEBUG] Writing file to: {output_file}")