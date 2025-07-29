import argparse
from argparse import RawTextHelpFormatter
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, size, expr, when, flatten, concat, collect_set, array_join, split, element_at, regexp_replace, array_contains, greatest, asc, lpad
from pyspark.sql.types import StringType, LongType
import glow

parser = argparse.ArgumentParser()

parser.add_argument('--output_basename', help='Base name for the output TSV.GZ file')
parser.add_argument('--input_file', help='Input file path for AR_comphet file')
parser.add_argument('--clinvar', action='store_true', help='Include clinvar data')
parser.add_argument('--genes', action='store_true', help='Include genes data')
parser.add_argument('--dbsnp', action='store_true', help='Include dbsnp data')
parser.add_argument('--dbnsfp', help='dbnsfp annovar parquet file dir')
parser.add_argument('--gencc', help='gencc parquet file dir')
parser.add_argument('--intervar', help='intervar parquet file dir')
parser.add_argument('--Cosmic_CancerGeneCensus', help='Cosmic_CancerGeneCensus parquet file dir')
parser.add_argument('--regeneron', help='regeneron parquet file dir')
parser.add_argument('--allofus', help='allofus parquet file dir')
parser.add_argument('--hgmd_var', help='hgmd_var parquet file dir')
parser.add_argument('--hgmd_gene', help='hgmd_gene parquet file dir')
parser.add_argument('--omim_gene', help='omim_gene parquet file dir')
parser.add_argument('--orphanet_gene', help='orphanet_gene parquet file dir')
parser.add_argument('--topmed', help='topmed parquet file dir')
parser.add_argument('--maf', default=0.001, help='minor allele frequency (MAF) threshold in gnomAD and TOPMed')
parser.add_argument('--dpc_l', default=0.5,
        help='damage predict count lower threshold')
parser.add_argument('--dpc_u', default=1,
        help='damage predict count upper threshold')
parser.add_argument('--known_variants_l', nargs='+', default=['ClinVar', 'HGMD'],
                    help='known variant databases used, default is ClinVar and HGMD')
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

# --------------------------------------
# Part 1: loading databases 
# --------------------------------------
cond = ['chromosome', 'start', 'reference', 'alternate']
# Table ClinVar
if args.clinvar:
    clinvar = spark.read.format("delta") \
        .load('s3a://kf-strides-public-vwb-prd/clinvar/')
# Table dbSNP
if args.dbsnp:
    dbsnp = spark.read.format("delta") \
        .load('s3a://kf-strides-public-vwb-prd/dbsnp/') \
        .withColumnRenamed('name', 'DBSNP_RSID') \
        .select(cond + ['DBSNP_RSID'])
# Table dbnsfp_annovar, added a column for ratio of damage predictions to all predictions
dbnsfp = spark.read.parquet(args.dbnsfp).select(cond + [col('DamagePredCount')])
c_dbn = ['DamagePredCount', 'PredCountRatio_D2T']
t_dbn = dbnsfp \
    .withColumn('PredCountRatio_D2T', \
                when(split(col('DamagePredCount'), '_')[1] == 0, lit(None).cast(StringType())) \
                .otherwise(split(col('DamagePredCount'), '_')[0] / split(col('DamagePredCount'), '_')[1])) \
    .select(cond + c_dbn)
# GenCC genes
gencc = spark.read.parquet(args.gencc)
g_genc = gencc \
    .select('gene_curie', 'disease_curie', 'disease_title', 'classification_title', 'moi_title') \
    .withColumnRenamed('gene_curie', 'GenCC_hgnc_id') \
    .withColumnRenamed('disease_curie', 'GenCC_disease_curie') \
    .withColumnRenamed('disease_title', 'GenCC_disease_title') \
    .withColumnRenamed('classification_title', 'GenCC_classification_title') \
    .withColumnRenamed('moi_title', 'GenCC_moi_title') \
    .groupBy('GenCC_hgnc_id') \
    .agg(collect_set('GenCC_disease_curie').alias('GenCC_disease_curie_combined'), \
         collect_set('GenCC_disease_title').alias('GenCC_disease_title_combined'), \
         collect_set('GenCC_classification_title').alias('GenCC_classification_title_combined'), \
         collect_set('GenCC_moi_title').alias('GenCC_moi_title_combined'))

hgmd_var = spark.read.parquet(args.hgmd_var)
regeneron = spark.read.parquet(args.regeneron).select(cond + ['REGENERON_ALL_AF'])
intervar = spark.read.parquet(args.intervar).select(cond + ['InterVar_automated'])
allofus = spark.read.parquet(args.allofus) \
                        .withColumnRenamed('gvs_all_af', 'ALLOFUS_GVS_ALL_AF') \
                        .select(cond + ['ALLOFUS_GVS_ALL_AF'])

# HGMD genes
if args.genes:
    genes = spark.read.format("delta") \
        .load('s3a://kf-strides-public-vwb-prd/genes/') \
        .select('entrez_gene_id', 'hgnc', 'ensembl_gene_id')
hgmd_gene = spark.read.parquet(args.hgmd_gene)
g_hgmd = hgmd_gene.alias('h') \
    .join(genes.alias('g'), col('h.entrez_gene_id') == col('g.entrez_gene_id'), how='left') \
    .select(
        col('h.entrez_gene_id').alias('entrez_gene_id'),
        col('g.hgnc'),
        col('g.ensembl_gene_id'),
        col('h.symbol').alias('HGMD_symbol'),
        col('h.DM').alias('HGMD_DM'),
        col('h.`DM?`').alias('HGMD_DM?')
    ) \
    .withColumn(
        "HGMD_DM",
        when(
            (size(col("HGMD_DM")) == 0) | (col("HGMD_DM") == expr("array('')")),
            lit(None)
        ).otherwise(col("HGMD_DM"))
    ) \
    .withColumn(
        "HGMD_DM?",
        when(
            (size(col("HGMD_DM?")) == 0) | (col("HGMD_DM?") == expr("array('')")),
            lit(None)
        ).otherwise(col("HGMD_DM?"))
    )

# OMIM genes
omim_gene = spark.read.parquet(args.omim_gene)
g_omim = omim_gene \
    .select('entrez_gene_id', 'omim_gene_id', 'phenotype') \
    .withColumnRenamed('entrez_gene_id', 'OMIM_entrez_gene_id') \
    .withColumnRenamed('omim_gene_id', 'OMIM_gene_id') \
    .withColumnRenamed('phenotype', 'OMIM_phenotype') \
    .groupBy('OMIM_entrez_gene_id') \
    .agg(collect_set('OMIM_gene_id').alias('OMIM_gene_id'), \
         collect_set('OMIM_phenotype').cast(StringType()).alias('OMIM_phenotype_combined')) \
    .withColumn('OMIM_phenotype_combined', \
                when(col('OMIM_phenotype_combined') == '[]', lit(None)).otherwise(col('OMIM_phenotype_combined')))

# Orphanet genes
orphanet_gene = spark.read.parquet(args.orphanet_gene)
g_orph = orphanet_gene \
    .withColumn(
        "Orphanet_HGNC_gene_id",
        when(
            col("HGNC_gene_id").isNotNull(),
            concat(lit("HGNC:"), col("HGNC_gene_id").cast("string"))
        ).otherwise(None)
    ) \
    .select(
        'gene_symbol', 'disorder_id', 'gene_source_of_validation',
        'Orphanet_HGNC_gene_id', 'type_of_inheritance', 'ensembl_gene_id'
    ) \
    .withColumnRenamed('gene_symbol', 'Orphanet_gene_symbol') \
    .withColumnRenamed('disorder_id', 'Orphanet_disorder_id') \
    .withColumnRenamed('gene_source_of_validation', 'Orphanet_gene_source_of_validation') \
    .withColumnRenamed('type_of_inheritance', 'Orphanet_type_of_inheritance') \
    .groupBy('Orphanet_gene_symbol') \
    .agg(
        collect_set('Orphanet_disorder_id').alias('Orphanet_disorder_id_combined'),
        collect_set('Orphanet_HGNC_gene_id').alias('Orphanet_HGNC_gene_id_array'),
        collect_set('ensembl_gene_id').alias('ensembl_gene_id_array'),
        collect_set('Orphanet_gene_source_of_validation').alias('Orphanet_gene_source_of_validation_combined'),
        collect_set('Orphanet_type_of_inheritance').alias('Orphanet_type_of_inheritance_combined')
    ) \
    .withColumn(
        'Orphanet_HGNC_gene_id',
        array_join('Orphanet_HGNC_gene_id_array', ',')
    ) \
    .withColumn(
        'ensembl_gene_id',
        array_join('ensembl_gene_id_array', ',')
    ) \
    .drop('Orphanet_HGNC_gene_id_array', 'ensembl_gene_id_array') \
    .withColumn(
        'Orphanet_type_of_inheritance_combined',
        flatten(col('Orphanet_type_of_inheritance_combined'))
    ) \
    .withColumn(
        'Orphanet_type_of_inheritance_combined',
        when(
            size(expr("filter(Orphanet_type_of_inheritance_combined, x -> x != '')")) == 0,
            lit(None)
        ).otherwise(
            expr("filter(Orphanet_type_of_inheritance_combined, x -> x != '')")
        )
    ) \
    .withColumn(
        'Orphanet_gene_source_of_validation_combined',
        when(
            size(expr("filter(Orphanet_gene_source_of_validation_combined, x -> x != '')")) == 0,
            lit(None)
        ).otherwise(
            expr("filter(Orphanet_gene_source_of_validation_combined, x -> x != '')")
        )
    )

topmed = spark.read.parquet(args.topmed).select(cond + [col('af')])

Cosmic_CancerGeneCensus = spark.read.parquet(args.Cosmic_CancerGeneCensus).withColumnRenamed(
    'Tier', 'CGC_Tier') \
 .withColumnRenamed('Mutation_Types', 'CGC_Mutation_Types') \
 .select('CGC_Tier', 'CGC_Mutation_Types', 'Entrez_GeneId')


# --------------------------------------
# part 2: Setting parameters
# --------------------------------------
# Keep only high impact variants
consequences_to_keep = ["transcript_ablation",
    "splice_acceptor_variant",
    "splice_donor_variant",
    "stop_gained",
    "frameshift_variant",
    "stop_lost",
    "start_lost",
    "transcript_amplification",
    "inframe_insertion",
    "inframe_deletion",
    "missense_variant",
    "protein_altering_variant",
    "splice_region_variant",
    "incomplete_terminal_codon_variant",
    "start_retained_variant",
    "stop_retained_variant",
    "coding_sequence_variant"]


# Set minor allele frequency (MAF) threshold in gnomAD and TOPMed
maf = args.maf # default: 0.001

# Set range for the ratio of DamagePredCount to AllPredCount in dbNSFP
dpc_l = args.dpc_l # default: 0.5
dpc_u = args.dpc_u # default: 1

# Check known variants
known_variants_l = args.known_variants_l # default: ('HGMD', 'Clinvar')

# --------------------------------------
# part 3: running
# --------------------------------------

input_file = args.input_file
singles_sample_variants = spark \
    .read \
    .options(delimiter="\t", header=True) \
    .csv(input_file) \
    .withColumnRenamed('#CHROM', 'chromosome') \
    .withColumnRenamed('POS', 'start') \
    .withColumnRenamed('REF', 'reference') \
    .withColumnRenamed('ALT', 'alternate') \
    .withColumnRenamed('QUAL', 'quality')
singles_sample_variants = singles_sample_variants \
    .withColumn('chromosome', \
                when(singles_sample_variants.chromosome.startswith('chr'), regexp_replace('chromosome', 'chr', '')) \
                .otherwise(singles_sample_variants.chromosome))

# Keep high impact variants
table_imported_exon = singles_sample_variants \
    .where(col('CSQ_Consequence').isin(consequences_to_keep))

# join tables regeneron, allofus, dbsnp and intervar
table_imported_exon = table_imported_exon.join(
    regeneron, cond, 'left').join( \
    allofus, cond, 'left').join(\
    dbsnp, cond, 'left').join(\
    intervar, cond, 'left')

# Attach TOPMed and max gnomAD/TOPMed frequencies
table_imported_exon = table_imported_exon \
    .join(topmed.alias('g'), cond, 'left') \
    .select([col(x) for x in table_imported_exon.columns] + [col('g.af')]) \
    .withColumnRenamed('af', 'TOPMed_af')
table_imported_exon = table_imported_exon \
    .withColumn('max_gnomad_topmed', greatest( \
        lit(0), \
        col('CSQ_gnomAD_AF').cast('double'), \
        col('INFO_gnomad_3_1_1_AF').cast('double'), \
        col('TOPMed_af').cast('double')))

# Flag using MAF
table_imported_exon = table_imported_exon \
        .withColumn('flag_keep', when(col('max_gnomad_topmed') <= maf, 1).otherwise(0))

# Table ClinVar, restricted to those seen in variants and labeled as pathogenic/likely_pathogenic
c_clv = ['VariationID', 'clin_sig']
t_clv = clinvar \
    .withColumnRenamed('name', 'VariationID') \
    .where((array_contains(col('clin_sig'), 'Pathogenic') | array_contains(col('clin_sig'), 'Likely_pathogenic'))) \
    .join(table_imported_exon, cond) \
    .select(cond + c_clv)

# Table HGMD, restricted to those seen in variants and labeled as DM or DM?
c_hgmd = ['HGMDID', 'variant_class']
t_hgmd = hgmd_var \
    .withColumnRenamed('id', 'HGMDID') \
    .where(col('variant_class').startswith('DM')) \
    .join(table_imported_exon, cond) \
    .select(cond + c_hgmd)

# Join imported table to dbnsfp, keep variants with PredCountRatio_D2T between dpc_l and dpc_u (both inclusive)
table_imported_exon_dbn = table_imported_exon \
    .join(t_dbn, cond, how='left') \
    .withColumn('flag_keep', when((table_imported_exon.flag_keep == 1) \
        & col('PredCountRatio_D2T').isNotNull() \
        & (col('PredCountRatio_D2T') >= dpc_l) \
        & (col('PredCountRatio_D2T') <= dpc_u), 1) \
    .otherwise(table_imported_exon.flag_keep))

# Include ClinVar if specified
if 'Clinvar' in known_variants_l and t_clv.take(1):
    table_imported_exon_dbn = table_imported_exon_dbn \
        .join(t_clv, cond, how='left') \
        .withColumn('flag_keep', when(col('VariationID').isNotNull(), 1) \
        .otherwise(table_imported_exon_dbn.flag_keep))

# Include HGMD if specified
if 'HGMD' in known_variants_l and t_hgmd.take(1):
    table_imported_exon_dbn = table_imported_exon_dbn \
        .join(t_hgmd, cond, how='left') \
        .withColumn('flag_keep', when(col('HGMDID').isNotNull(), 1) \
        .otherwise(table_imported_exon_dbn.flag_keep))

# Attach HGMD gene-disease relationships
table_imported_exon_dbn_phenotypes = table_imported_exon_dbn \
    .join(g_hgmd.alias('g'), table_imported_exon_dbn.CSQ_HGNC_ID == g_hgmd.hgnc, 'left') \
    .select([col(x) for x in table_imported_exon_dbn.columns] + \
            [col('g.entrez_gene_id'), \
                col('g.HGMD_DM'), \
                col('g.HGMD_DM?')])

# join Cosmic_CancerGeneCensus table
table_imported_exon_dbn_phenotypes = table_imported_exon_dbn_phenotypes.join(
    Cosmic_CancerGeneCensus,
    table_imported_exon_dbn_phenotypes.entrez_gene_id == Cosmic_CancerGeneCensus.Entrez_GeneId,
    how='left'
).drop('Entrez_GeneId')

# Attach OMIM gene-disease relationships
table_imported_exon_dbn_phenotypes = table_imported_exon_dbn_phenotypes \
    .join(g_omim.alias('g'), table_imported_exon_dbn_phenotypes.entrez_gene_id == g_omim.OMIM_entrez_gene_id, 'left') \
    .select([col(x) for x in table_imported_exon_dbn_phenotypes.columns] + \
            [col('g.OMIM_gene_id'), \
                col('g.OMIM_phenotype_combined')])

# Attach Orphanet gene-disease relationships
table_imported_exon_dbn_phenotypes = table_imported_exon_dbn_phenotypes \
        .join(g_orph.alias('g'), 
          (table_imported_exon_dbn_phenotypes.CSQ_HGNC_ID == g_orph.Orphanet_HGNC_gene_id) |
          (table_imported_exon_dbn_phenotypes.CSQ_Gene == g_orph.ensembl_gene_id), 'left') \
        .select([col(x) for x in table_imported_exon_dbn_phenotypes.columns] \
        + [col('g.Orphanet_disorder_id_combined'), \
            col('g.Orphanet_gene_source_of_validation_combined'), \
            col('g.Orphanet_HGNC_gene_id'), \
            col('g.Orphanet_type_of_inheritance_combined')])

# Attach GenCC gene-disease relationships
table_imported_exon_dbn_phenotypes = table_imported_exon_dbn_phenotypes \
    .join(g_genc.alias('g'), table_imported_exon_dbn_phenotypes.CSQ_HGNC_ID == g_genc.GenCC_hgnc_id, 'left') \
    .select([col(x) for x in table_imported_exon_dbn_phenotypes.columns] \
        + [col('g.GenCC_disease_curie_combined'), \
            col('g.GenCC_disease_title_combined'), \
            col('g.GenCC_classification_title_combined'), \
            col('g.GenCC_moi_title_combined')])

# Generate output
output_file = args.output_basename + '.VWB_result.tsv.gz'
table_imported_exon_dbn_phenotypes.distinct() \
    .withColumn("start", col("start").cast(LongType())) \
    .sort(asc( \
            when(col('chromosome').isin(['X', 'Y', 'x', 'y']), lpad('chromosome', 2, '2')) \
                .otherwise(lpad('chromosome', 2, '0')) \
            ), \
            asc(col('start'))
        ) \
    .toPandas() \
    .to_csv(output_file, sep="\t", index=False, na_rep='-', compression='gzip')