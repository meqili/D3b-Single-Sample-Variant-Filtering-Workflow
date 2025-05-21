#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: Workflow
id: single-sample-annovar-variant-filtering-wf
label: single sample variant filtering workflow (annovar inputs)
$namespaces:
  sbg: https://sevenbridges.com

requirements:
- class: InlineJavascriptRequirement
- class: StepInputExpressionRequirement

inputs:
- id: annovar_vcf
  type: File
- id: topmed
  type: File
  sbg:suggestedValue:
    name: topmed_bravo.tar.gz
    class: File
    path: 65b1451caa2e154c2a722ec2
- id: spliceai
  type: File
  sbg:suggestedValue:
    name: hg38_SpliceAI.tar.gz
    class: File
    path: 669fd3e1eead7f7aca7e7e65
- id: mmsplice
  type: File
  sbg:suggestedValue:
    name: hg38_mmsplice.tar.gz
    class: File
    path: 669fd3e1eead7f7aca7e7e69
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
    name: hg38_HGMD2025Q1_variant.tar.gz
    class: File
    path: 67efe4ae799cc5199079fea1
- id: hgmd_gene
  type: File
  sbg:suggestedValue:
    name: hg38_HGMD2024Q4_gene_sorted.tar.gz
    class: File
    path: 6780431109c1a319b8e0d18a
- id: gencc
  doc: gencc parquet file dir
  type: File
  sbg:suggestedValue:
    name: gencc20240725.tar.gz
    class: File
    path: 66a2e482938544533ad4efbc
- id: dbnsfp_annovar_parquet
  doc: dbnsfp annovar parquet file dir
  type: File
  sbg:suggestedValue:
    name: dbnsfp.tar.gz
    class: File
    path: 65b03e76b2d0f428e1c6f049
- id: clinvar
  doc: clinvar parquet file dir
  type: File
  sbg:suggestedValue:
    name: clinvar_20250504.tar.gz
    class: File
    path: 682cce2cf8492c6e34394dc5
- id: maf
  doc: minor allele frequency (MAF) threshold in gnomAD and TOPMed
  type: double?
  default: 0.0001
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
- id: spark_driver_mem
  doc: GB of RAM to allocate to this task
  type: int?
  default: 10
- id: sql_broadcastTimeout
  doc: .config("spark.sql.broadcastTimeout", 36000)
  type: int?
  default: 36000

outputs:
- id: sample_VWB_filtering_ouput
  type: File
  outputSource:
  - SinglesSample-annovar-Filtering-step1/sample_VWB_filtering_ouput

steps:
- id: annovar_vcf_to_tsv
  in:
    - id: annovar_vcf
      source: annovar_vcf
  run: tools/annovar_vcf_to_tsv.cwl
  out:
  - id: annovar_tsv
- id: SinglesSample-annovar-Filtering-step1
  in:
  - id: input_file_path
    source: annovar_vcf_to_tsv/annovar_tsv
  - id: spark_driver_mem
    source: spark_driver_mem
  - id: hgmd_var
    source: hgmd_var
  - id: dbnsfp_annovar
    source: dbnsfp_annovar_parquet
  - id: clinvar
    source: clinvar
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
  - id: spliceai
    source: spliceai
  - id: mmsplice
    source: mmsplice
  run: tools/SinglesSample-ANNOVAR-Filtering-step1.cwl
  out:
  - id: sample_VWB_filtering_ouput