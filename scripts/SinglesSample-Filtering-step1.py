import argparse
from argparse import RawTextHelpFormatter
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import glow

parser = argparse.ArgumentParser(
    description='Script of gene based variant filtering. \n\
    MUST BE RUN WITH spark-submit. For example: \n\
    spark-submit --driver-memory 10G yours.py',
    formatter_class=RawTextHelpFormatter)

parser = argparse.ArgumentParser()


######
parser.add_argument('--output_basemame')
parser.add_argument('--input_file_path', help='Input file path for annovar annotated txt file')
parser.add_argument('--clinvar', help='clinvar parquet file dir')
parser.add_argument('--dbnsfp', help='dbnsfp annovar parquet file dir')
parser.add_argument('--gencc', help='gencc parquet file dir')
parser.add_argument('--hgmd_var', help='hgmd_var parquet file dir')
parser.add_argument('--hgmd_gene', help='hgmd_gene parquet file dir')
parser.add_argument('--omim_gene', help='omim_gene parquet file dir')
parser.add_argument('--orphanet_gene', help='orphanet_gene parquet file dir')
parser.add_argument('--topmed', help='topmed parquet file dir')
parser.add_argument('--maf', default=0.0001, help='Set minor allele frequency (MAF) threshold in gnomAD and TOPMed')
parser.add_argument('--dpc_l', default=0.5,
        help='damage predict count lower threshold')
parser.add_argument('--dpc_u', default=1,
        help='damage predict count upper threshold')
parser.add_argument('--known_variants_l', nargs='+', default=['ClinVar', 'HGMD'],
                    help='known variant databases used, default is ClinVar and HGMD')
args = parser.parse_args()

# Create spark session
spark = (
    pyspark.sql.SparkSession.builder.appName("PythonPi")
    .getOrCreate()
    )
# Register so that glow functions like read vcf work with spark. Must be run in spark shell or in context described in help
spark = glow.register(spark)

# --------------------------------------
# Part 1: loading databases 
# --------------------------------------
cond = ['chromosome', 'start', 'reference', 'alternate']
# Table ClinVar
clinvar = spark.read.parquet(args.clinvar)
# Table dbnsfp_annovar, added a column for ratio of damage predictions to all predictions
dbnsfp = spark.read.parquet(args.dbnsfp).select(cond + [F.col('DamagePredCount')])
c_dbn = ['DamagePredCount', 'PredCountRatio_D2T']
t_dbn = dbnsfp \
    .withColumn('PredCountRatio_D2T', \
                F.when(F.split(F.col('DamagePredCount'), '_')[1] == 0, F.lit(None).cast(F.StringType())) \
                .otherwise(F.split(F.col('DamagePredCount'), '_')[0] / F.split(F.col('DamagePredCount'), '_')[1])) \
    .select(cond + c_dbn)
# GenCC genes
gencc = spark.read.parquet(args.gencc)
g_genc = gencc \
    .select('gene_symbol', 'disease_curie', 'disease_title', 'classification_title', 'moi_title') \
    .withColumnRenamed('gene_symbol', 'GenCC_gene_symbol') \
    .withColumnRenamed('disease_curie', 'GenCC_disease_curie') \
    .withColumnRenamed('disease_title', 'GenCC_disease_title') \
    .withColumnRenamed('classification_title', 'GenCC_classification_title') \
    .withColumnRenamed('moi_title', 'GenCC_moi_title') \
    .groupBy('GenCC_gene_symbol') \
    .agg(F.collect_set('GenCC_disease_curie').alias('GenCC_disease_curie_combined'), \
         F.collect_set('GenCC_disease_title').alias('GenCC_disease_title_combined'), \
         F.collect_set('GenCC_classification_title').alias('GenCC_classification_title_combined'), \
         F.collect_set('GenCC_moi_title').alias('GenCC_moi_title_combined'))

hgmd_var = spark.read.parquet(args.hgmd_var)

# HGMD genes
hgmd_gene = spark.read.parquet(args.hgmd_gene)
g_hgmd = hgmd_gene \
    .select('entrez_gene_id', 'symbol', 'DM', 'DM?') \
    .withColumnRenamed('symbol', 'HGMD_symbol') \
    .withColumnRenamed('DM', 'HGMD_DM') \
    .withColumnRenamed('DM?', 'HGMD_DM?')

# OMIM genes
omim_gene = spark.read.parquet(args.omim_gene)
g_omim = omim_gene \
    .select('entrez_gene_id', 'omim_gene_id', 'phenotype') \
    .withColumnRenamed('entrez_gene_id', 'OMIM_entrez_gene_id') \
    .withColumnRenamed('omim_gene_id', 'OMIM_gene_id') \
    .withColumnRenamed('phenotype', 'OMIM_phenotype') \
    .groupBy('OMIM_entrez_gene_id') \
    .agg(F.collect_set('OMIM_gene_id').alias('OMIM_gene_id'), \
         F.collect_set('OMIM_phenotype').cast(StringType()).alias('OMIM_phenotype_combined')) \
    .withColumn('OMIM_phenotype_combined', \
                F.when(F.col('OMIM_phenotype_combined') == '[]', F.lit(None)).otherwise(F.col('OMIM_phenotype_combined')))

# Orphanet genes
orphanet_gene = spark.read.parquet(args.orphanet_gene)
g_orph = orphanet_gene \
    .select('gene_symbol', 'disorder_id', 'gene_source_of_validation', 'HGNC_gene_id', 'type_of_inheritance') \
    .withColumnRenamed('gene_symbol', 'Orphanet_gene_symbol') \
    .withColumnRenamed('disorder_id', 'Orphanet_disorder_id') \
    .withColumnRenamed('gene_source_of_validation', 'Orphanet_gene_source_of_validation') \
    .withColumnRenamed('HGNC_gene_id', 'Orphanet_HGNC_gene_id') \
    .withColumnRenamed('type_of_inheritance', 'Orphanet_type_of_inheritance') \
    .groupBy('Orphanet_gene_symbol') \
    .agg(F.collect_set('Orphanet_disorder_id').cast(StringType()).alias('Orphanet_disorder_id_combined'), \
         F.collect_set('Orphanet_gene_source_of_validation').cast(StringType()).alias('Orphanet_gene_source_of_validation_combined'), \
         F.collect_set('Orphanet_HGNC_gene_id').cast(StringType()).alias('Orphanet_HGNC_gene_id'), \
         F.collect_set('Orphanet_type_of_inheritance').cast(StringType()).alias('Orphanet_type_of_inheritance_combined')) \
    .withColumn('Orphanet_type_of_inheritance_combined', \
                F.when(F.col('Orphanet_type_of_inheritance_combined') == '[]', F.lit(None)).otherwise(F.col('Orphanet_type_of_inheritance_combined'))) \
    .withColumn('Orphanet_gene_source_of_validation_combined', \
                F.when(F.col('Orphanet_gene_source_of_validation_combined') == '[]', F.lit(None)).otherwise(F.col('Orphanet_gene_source_of_validation_combined'))) \
    .withColumn('Orphanet_HGNC_gene_id', \
                F.when(F.col('Orphanet_HGNC_gene_id') == '[]', F.lit(None)).otherwise(F.col('Orphanet_HGNC_gene_id')))

topmed = spark.read.parquet(args.topmed).select(cond + [F.col('af')])


# --------------------------------------
# part 2: Setting parameters
# --------------------------------------
# Keep only high impact variants
exonic_consequences_to_keep = ["frameshift insertion", "frameshift deletion",
                        "frameshift block substitution", "stopgain",
                        "stoploss", "nonframeshift insertion",
                        "nonframeshift deletion", "nonframeshift block substitution",
                        "nonsynonymous SNV"]

# Set minor allele frequency (MAF) threshold in gnomAD and TOPMed
maf = args.maf # default: 0.0001

# Set range for the ratio of DamagePredCount to AllPredCount in dbNSFP
dpc_l = args.dpc_l # default: 0.5
dpc_u = args.dpc_u # default: 1

# Check known variants
known_variants_l = args.known_variants_l # default: ('HGMD', 'Clinvar')

# --------------------------------------
# part 3: running
# --------------------------------------
input_path = args.input_file_path
input_file = spark \
    .read \
    .options(delimiter="\t", header=True) \
    .csv(input_path) \
    .withColumnRenamed('Chr', 'chromosome') \
    .withColumnRenamed('Start', 'start') \
    .withColumnRenamed('Ref', 'reference') \
    .withColumnRenamed('Alt', 'alternate') \
    .withColumnRenamed('Func.refGene', 'Func_refGene') \
    .withColumnRenamed('ExonicFunc.refGene', 'ExonicFunc_refGene')
    
input_file = input_file \
    .withColumn('chromosome', \
                F.when(input_file.chromosome.startswith('chr'), F.regexp_replace('chromosome', 'chr', '')) \
                .otherwise(input_file.chromosome)) \
    .select(cond + ["Func_refGene", "ExonicFunc_refGene"])

# Keep high impact variants
table_imported_exon = input_file \
    .where((F.col("Func_refGene").contains('exonic') & F.col("ExonicFunc_refGene").isin(exonic_consequences_to_keep) |
                (F.col("Func_refGene") == "splicing")))

# Attach TOPMed and max gnomAD/TOPMed frequencies
table_imported_exon = table_imported_exon \
    .join(topmed.alias('g'), cond, 'left') \
    .select([F.col(x) for x in table_imported_exon.columns] + [F.col('g.af')]) \
    .withColumnRenamed('af', 'TOPMed_af')
table_imported_exon = table_imported_exon \
    .withColumn('max_gnomad_topmed', F.greatest( \
        F.lit(0), \
        F.col('gnomad211_genome_AF').cast('double'), \
        F.col('gnomad211_exome_AF').cast('double'), \
        F.col('gnomad30_genome_AF').cast('double')))

# Flag using MAF
table_imported_exon = table_imported_exon \
        .withColumn('flag_keep', F.when(F.col('max_gnomad_topmed') <= maf, 1).otherwise(0))

# Table ClinVar, restricted to those seen in variants and labeled as pathogenic/likely_pathogenic
c_clv = ['VariationID', 'clin_sig']
t_clv = clinvar \
    .withColumnRenamed('name', 'VariationID') \
    .where((F.array_contains(F.col('clin_sig'), 'Pathogenic') | F.array_contains(F.col('clin_sig'), 'Likely_pathogenic'))) \
    .join(table_imported_exon, cond) \
    .select(cond + c_clv)

# Table HGMD, restricted to those seen in variants and labeled as DM or DM?
c_hgmd = ['HGMDID', 'variant_class']
t_hgmd = hgmd_var \
    .withColumnRenamed('id', 'HGMDID') \
    .where(F.col('variant_class').startswith('DM')) \
    .join(table_imported_exon, cond) \
    .select(cond + c_hgmd)

# Join imported table to dbnsfp, keep variants with PredCountRatio_D2T between dpc_l and dpc_u (both inclusive)
table_imported_exon_dbn = table_imported_exon \
    .join(t_dbn, cond, how='left') \
    .withColumn('flag_keep', F.when((table_imported_exon.flag_keep == 1) \
        & F.col('PredCountRatio_D2T').isNotNull() \
        & (F.col('PredCountRatio_D2T') >= dpc_l) \
        & (F.col('PredCountRatio_D2T') <= dpc_u), 1) \
    .otherwise(table_imported_exon.flag_keep))
# print(table_imported_exon_dbn.count())

# Include ClinVar if specified
if 'Clinvar' in known_variants_l and t_clv.count() > 0:
    table_imported_exon_dbn = table_imported_exon_dbn \
        .join(t_clv, cond, how='left') \
        .withColumn('flag_keep', F.when(F.col('VariationID').isNotNull(), 1) \
        .otherwise(table_imported_exon_dbn.flag_keep))
# print(table_imported_exon_dbn.count())

# Include HGMD if specified
if 'HGMD' in known_variants_l and t_hgmd.count() > 0:
    table_imported_exon_dbn = table_imported_exon_dbn \
        .join(t_hgmd, cond, how='left') \
        .withColumn('flag_keep', F.when(F.col('HGMDID').isNotNull(), 1) \
        .otherwise(table_imported_exon_dbn.flag_keep))
# print(table_imported_exon_dbn.count())

# Attach HGMD gene-disease relationships
table_imported_exon_dbn_phenotypes = table_imported_exon_dbn \
    .join(g_hgmd.alias('g'), table_imported_exon_dbn.CSQ_SYMBOL == g_hgmd.HGMD_symbol, 'left') \
    .select([F.col(x) for x in table_imported_exon_dbn.columns] + \
            [F.col('g.entrez_gene_id'), \
                F.col('g.HGMD_DM'), \
                F.col('g.HGMD_DM?')])

# Attach OMIM gene-disease relationships
table_imported_exon_dbn_phenotypes = table_imported_exon_dbn_phenotypes \
    .join(g_omim.alias('g'), table_imported_exon_dbn_phenotypes.entrez_gene_id == g_omim.OMIM_entrez_gene_id, 'left') \
    .select([F.col(x) for x in table_imported_exon_dbn_phenotypes.columns] + \
            [F.col('g.OMIM_gene_id'), \
                F.col('g.OMIM_phenotype_combined')])

# Attach Orphanet gene-disease relationships
table_imported_exon_dbn_phenotypes = table_imported_exon_dbn_phenotypes \
    .join(g_orph.alias('g'), table_imported_exon_dbn_phenotypes.CSQ_SYMBOL == g_orph.Orphanet_gene_symbol, 'left') \
    .select([F.col(x) for x in table_imported_exon_dbn_phenotypes.columns] \
        + [F.col('g.Orphanet_disorder_id_combined'), \
            F.col('g.Orphanet_gene_source_of_validation_combined'), \
            F.col('g.Orphanet_HGNC_gene_id'), \
            F.col('g.Orphanet_type_of_inheritance_combined')])

# Attach GenCC gene-disease relationships
table_imported_exon_dbn_phenotypes = table_imported_exon_dbn_phenotypes \
    .join(g_genc.alias('g'), table_imported_exon_dbn_phenotypes.CSQ_SYMBOL == g_genc.GenCC_gene_symbol, 'left') \
    .select([F.col(x) for x in table_imported_exon_dbn_phenotypes.columns] \
        + [F.col('g.GenCC_disease_curie_combined'), \
            F.col('g.GenCC_disease_title_combined'), \
            F.col('g.GenCC_classification_title_combined'), \
            F.col('g.GenCC_moi_title_combined')]) \
    .sort(F.asc( \
            F.when(F.col('chromosome').isin(['X', 'Y', 'x', 'y']), F.lpad('chromosome', 2, '2')) \
                .otherwise(F.lpad('chromosome', 2, '0')) \
            ), \
            F.asc(F.col('start'))
        )

# Generate output
cols_json = [F.to_json(c[0]).alias(c[0]) if c[1].startswith("struct") else F.col(c[0]) for c in table_imported_exon_dbn_phenotypes.dtypes]
cols_ws = [F.concat_ws(',', c[0]).alias(c[0]) if c[1].startswith("array") else F.col(c[0]) for c in table_imported_exon_dbn_phenotypes.dtypes]
output_file = args.output_basemame + '.VWB_result.tsv'
table_imported_exon_dbn_phenotypes = table_imported_exon_dbn_phenotypes \
    .select(cols_json) \
    .select(cols_ws) \
    .toPandas() \
    .to_csv(output_file, sep='\t', header=True, index=False, line_terminator='\n')