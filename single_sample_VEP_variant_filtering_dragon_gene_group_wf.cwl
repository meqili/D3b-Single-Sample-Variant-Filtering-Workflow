#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: Workflow
id: single-sample-VEP-variant-filtering-gene-group-wf
label: Single Sample Variant Filtering Workflow (Dragon VEP Inputs + Gene Group Annotation)
$namespaces:
  sbg: https://sevenbridges.com

requirements:
- class: InlineJavascriptRequirement
- class: StepInputExpressionRequirement

inputs:
- id: vep_vcf
  type: File
- id: topmed
  type: File
  sbg:suggestedValue:
    name: topmed_bravo.tar.gz
    class: File
    path: 65b1451caa2e154c2a722ec2
- id: orphanet_gene
  type: File
  sbg:suggestedValue:
    name: orphanet_gene_set.tar.gz
    class: File
    path: 65b1451daa2e154c2a722ed6
- id: omim_gene
  type: File
  sbg:suggestedValue:
    name: omim_gene_set.tar.gz
    class: File
    path: 65b1451daa2e154c2a722ece
- id: hgmd_var
  doc: the latest HGMD variant  parquet file dir
  type: File
  sbg:suggestedValue:
    name: hg38_HGMD2025Q4_variant.tar.gz
    class: File
    path: 695be4d15ddfaa35efcb6b7c
- id: hgmd_gene
  type: File
  sbg:suggestedValue:
    name: hg38_HGMD2025Q4_gene_sorted.tar.gz
    class: File
    path: 695be4d15ddfaa35efcb6b7a
- id: gencc
  doc: gencc parquet file dir
  type: File
  sbg:suggestedValue:
    name: gencc20260113.tar.gz
    class: File
    path: 6967ca7f5ddfaa35efdddd65
- id: dbnsfp_annovar_parquet
  doc: dbnsfp annovar parquet file dir
  type: File
  sbg:suggestedValue:
    name: dbnsfp.tar.gz
    class: File
    path: 65b03e76b2d0f428e1c6f049
- id: Cosmic_CancerGeneCensus
  doc: Cosmic_CancerGeneCensus parquet tarred file
  type: File
  sbg:suggestedValue:
    name: Cosmic_CancerGeneCensus_v102_GRCh38.tar.gz
    class: File
    path: 687a982ef8492c6e34e93913
- id: allofus
  doc: allofus parquet tarred file
  type: File
  sbg:suggestedValue:
    name: hg38_allofus.tar.gz
    class: File
    path: 685c408e8629c5590195b80e
- id: regeneron
  doc: regeneron parquet tarred file
  type: File
  sbg:suggestedValue:
    name: hg38_regeneron.tar.gz
    class: File
    path: 685c1481799cc51990763d98
- id: intervar
  doc: intervar parquet tarred file
  type: File
  sbg:suggestedValue:
    name: hg38_intervar.tar.gz
    class: File
    path: 6893c80d6f8ef420f2196ca8
- id: clinvar
  doc: clinvar parquet delta file
  type: boolean
- id: genes
  doc: genes parquet delta file
  type: boolean
- id: dbsnp
  doc: dbsnp parquet delta file
  type: boolean
- id: maf
  doc: minor allele frequency (MAF) threshold in gnomAD and TOPMed
  type: double?
  default: 0.001
- id: damage_predict_count_lower
  doc: the lower ratio of DamagePredCount to AllPredCount in dbNSFP
  type: double?
  default: 0.5
- id: damage_predict_count_upper
  doc: the upper ratio of DamagePredCount to AllPredCount in dbNSFP
  type: double?
  default: 1
- id: known_variants_l
  doc: Check known variants in following database(s)
  type:
  - 'null'
  - name: known_variants_list
    type: enum
    symbols:
    - Clinvar
    - HGMD
    - Clinvar HGMD
  default: Clinvar HGMD
- id: cpg_gene_file
  doc: "Cancer Predisposition Genes (.tsv.gz)"
  type: File
  sbg:suggestedValue:
    name: CancerPredispositionGenes_clean.txt
    class: File
    path: 6871829a799cc51990884450
- id: sfg_gene_file
  doc: "Secondary Finding Genes (.tsv)"
  type: File
  sbg:suggestedValue:
    name: Miller_2023_GenetMed_ACMG_S.txt
    class: File
    path: 68718780f8492c6e34ca8369
- id: spark_driver_mem
  doc: GB of RAM to allocate to this task
  type: int?
  default: 48
- id: spark_executor_instance
  doc: number of instances used 
  type: int?
  default: 3
- id: spark_executor_mem
  doc: GB of executor memory
  type: int?
  default: 34
- id: spark_executor_core
  doc: number of executor cores
  type: int?
  default: 5
- id: spark_driver_core
  doc: number of driver cores
  type: int?
  default: 2
- id: spark_driver_maxResultSize
  doc: GB of driver maxResultSize
  type: int?
  default: 2
- id: sql_broadcastTimeout
  doc: .config("spark.sql.broadcastTimeout", 36000)
  type: int?
  default: 36000

outputs:
- id: gene_group_annotated_output
  type: File
  outputSource:
  - annotate_variant_gene_group/gene_group_annotated_output

steps:
- id: vep_to_annovar
  in:
  - id: vep_vcf
    source: vep_vcf
  run: tools/vep_to_annovar.cwl
  out:
  - id: vep_to_annovar_output
  - id: vcf_header
- id: vep_vcf_to_tsv_header
  in:
    vcf_header: vep_to_annovar/vcf_header
    vcf_wo_header: vep_to_annovar/vep_to_annovar_output
  run: tools/vep_vcf_to_tsv_header.cwl
  out:
  - id: output_vcf_w_header
- id: SingleSample-VEP-Filtering-step1
  in:
  - id: input_file
    source: vep_vcf_to_tsv_header/output_vcf_w_header
  - id: spark_driver_mem
    source: spark_driver_mem
  - id: hgmd_var
    source: hgmd_var
  - id: dbnsfp_annovar
    source: dbnsfp_annovar_parquet
  - id: clinvar
    source: clinvar
  - id: genes
    source: genes
  - id: dbsnp
    source: dbsnp
  - id: maf
    source: maf
  - id: damage_predict_count_lower
    source: damage_predict_count_lower
  - id: damage_predict_count_upper
    source: damage_predict_count_upper
  - id: known_variants_l
    source:
    - known_variants_l
  - id: sql_broadcastTimeout
    source: sql_broadcastTimeout
  - id: spark_driver_mem
    source: spark_driver_mem
  - id: spark_executor_instance
    source: spark_executor_instance
  - id: spark_executor_mem
    source: spark_executor_mem
  - id: spark_executor_core
    source: spark_executor_core
  - id: spark_driver_core
    source: spark_driver_core
  - id: spark_driver_maxResultSize
    source: spark_driver_maxResultSize
  - id: gencc
    source: gencc
  - id: hgmd_gene
    source: hgmd_gene
  - id: omim_gene
    source: omim_gene
  - id: orphanet_gene
    source: orphanet_gene
  - id: topmed
    source: topmed
  - id: allofus
    source: allofus
  - id: Cosmic_CancerGeneCensus
    source: Cosmic_CancerGeneCensus
  - id: regeneron
    source: regeneron
  - id: intervar
    source: intervar
  run: tools/SingleSample-VEP-Filtering-Dragon-step1.cwl
  out:
  - id: VWB_output
- id: annotate_variant_gene_group
  in:
  - id: input_file
    source: SingleSample-VEP-Filtering-step1/VWB_output
  - id: cpg_gene_file
    source: cpg_gene_file
  - id: sfg_gene_file
    source: sfg_gene_file
  - id: sql_broadcastTimeout
    source: sql_broadcastTimeout
  - id: spark_driver_mem
    source: spark_driver_mem
  - id: spark_executor_instance
    source: spark_executor_instance
  - id: spark_executor_mem
    source: spark_executor_mem
  - id: spark_executor_core
    source: spark_executor_core
  - id: spark_driver_core
    source: spark_driver_core
  - id: spark_driver_maxResultSize
    source: spark_driver_maxResultSize
  run: tools/annotate_variant_gene_group.cwl
  out:
  - id: gene_group_annotated_output
