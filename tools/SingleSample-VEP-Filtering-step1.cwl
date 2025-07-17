#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool
id: SingleSample-VEP-Filtering-step1
label: SingleSample-VEP-Filtering-step1
doc: |-
  Get a list of deleterious variants in interested genes from specified study cohort(s) in the Kids First program.
$namespaces:
  sbg: https://sevenbridges.com

requirements:
- class: ShellCommandRequirement
- class: ResourceRequirement
  coresMin: 16
  ramMin: $(inputs.spark_driver_mem * 1000)
- class: DockerRequirement
  dockerPull: pgc-images.sbgenomics.com/qqlii44/pyspark:3.5.1
- class: InitialWorkDirRequirement
  listing:
  - entryname: FamilyAnalysis_trios_step1.py
    entry:
      $include: ../scripts/SingleSample-VEP-Filtering-step1.py
- class: InlineJavascriptRequirement
  expressionLib:
  - |2-

    var setMetadata = function(file, metadata) {
        if (!('metadata' in file))
            file['metadata'] = metadata;
        else {
            for (var key in metadata) {
                file['metadata'][key] = metadata[key];
            }
        }
        return file
    };

    var inheritMetadata = function(o1, o2) {
        var commonMetadata = {};
        if (!Array.isArray(o2)) {
            o2 = [o2]
        }
        for (var i = 0; i < o2.length; i++) {
            var example = o2[i]['metadata'];
            for (var key in example) {
                if (i == 0)
                    commonMetadata[key] = example[key];
                else {
                    if (!(commonMetadata[key] == example[key])) {
                        delete commonMetadata[key]
                    }
                }
            }
        }
        if (!Array.isArray(o1)) {
            o1 = setMetadata(o1, commonMetadata)
        } else {
            for (var i = 0; i < o1.length; i++) {
                o1[i] = setMetadata(o1[i], commonMetadata)
            }
        }
        return o1;
    };

inputs:
- id: spark_driver_mem
  doc: GB of RAM to allocate to this task
  type: int?
  default: 48
  inputBinding:
    position: 3
    prefix: --spark_driver_mem
- id: spark_executor_instance
  doc: number of instances used 
  type: int?
  default: 3
  inputBinding:
    position: 3
    prefix: --spark_executor_instance
- id: spark_executor_mem
  doc: GB of executor memory
  type: int?
  default: 34
  inputBinding:
    position: 3
    prefix: --spark_executor_mem
- id: spark_executor_core
  doc: number of executor cores
  type: int?
  default: 5
  inputBinding:
    position: 3
    prefix: --spark_executor_core
- id: spark_driver_core
  doc: number of driver cores
  type: int?
  default: 2
  inputBinding:
    position: 3
    prefix: --spark_driver_core
- id: spark_driver_maxResultSize
  doc: GB of driver maxResultSize
  type: int?
  default: 2
  inputBinding:
    position: 3
    prefix: --spark_driver_maxResultSize
- id: sql_broadcastTimeout
  doc: .config("spark.sql.broadcastTimeout", 36000)
  type: int?
  default: 36000
  inputBinding:
    position: 3
    prefix: --sql_broadcastTimeout
- id: hgmd_var
  doc: the latest HGMD variant  parquet file dir
  type: File
- id: dbnsfp_annovar
  doc: dbnsfp annovar parquet file dir
  type: File
- id: Cosmic_CancerGeneCensus
  doc: the Cosmic_CancerGeneCensus parquet tarred file
  type: File
- id: regeneron
  doc: the regeneron parquet tarred file
  type: File
- id: allofus
  doc: the allofus parquet tarred file
  type: File
- id: clinvar
  type: boolean
  inputBinding:
    position: 3
    prefix: --clinvar
- id: dbsnp
  type: boolean
  inputBinding:
    position: 3
    prefix: --dbsnp
- id: genes
  type: boolean
  inputBinding:
    position: 3
    prefix: --genes
- id: gencc
  doc: gencc parquet file dir
  type: File
- id: hgmd_gene
  type: File
- id: omim_gene
  type: File
- id: orphanet_gene
  type: File
- id: topmed
  type: File
- id: maf
  doc: minor allele frequency (MAF) threshold for homozygotes in gnomAD and TOPMed
  type: double?
  default: 0.0001
  inputBinding:
    prefix: --maf
    position: 3
    shellQuote: false
- id: damage_predict_count_lower
  type: double?
  default: 0.5
  inputBinding:
    prefix: --dpc_l
    position: 3
    shellQuote: false
- id: damage_predict_count_upper
  type: double?
  default: 1
  inputBinding:
    prefix: --dpc_u
    position: 3
    shellQuote: false
- id: known_variants_l
  doc: Check known variants in following database(s)
  type:
  - 'null'
  - name: buildver
    type: enum
    symbols:
    - Clinvar
    - HGMD
    - Clinvar HGMD
  default: Clinvar HGMD
  inputBinding:
    prefix: --known_variants_l
    position: 3
    shellQuote: false
- id: input_file
  type: File

outputs:
- id: VWB_output
  type: File
  outputBinding:
    glob: '*.VWB_result.tsv.gz'
    outputEval: $(inheritMetadata(self, inputs.input_file))

baseCommand:
- tar
- -xvf
arguments:
- position: 1
  valueFrom: |-
    $(inputs.dbnsfp_annovar.path) && tar -xvf $(inputs.gencc.path) && \
     tar -xvf $(inputs.hgmd_gene.path) && tar -xvf $(inputs.omim_gene.path) && \
     tar -xvf $(inputs.orphanet_gene.path) && tar -xvf $(inputs.topmed.path) && \
     tar -xvf $(inputs.hgmd_var.path) && tar -xvf $(inputs.Cosmic_CancerGeneCensus.path)&& \
     tar -xvf $(inputs.regeneron.path) && tar -xvf $(inputs.allofus.path)
  shellQuote: false
- position: 2
  valueFrom: |-
    && python FamilyAnalysis_trios_step1.py  \
     --dbnsfp ./$(inputs.dbnsfp_annovar.nameroot.replace(".tar", ""))/   \
     --gencc ./$(inputs.gencc.nameroot.replace(".tar", ""))/ \
     --hgmd_gene ./$(inputs.hgmd_gene.nameroot.replace(".tar", ""))/  \
     --hgmd_var ./$(inputs.hgmd_var.nameroot.replace(".tar", ""))/  \
     --omim_gene ./$(inputs.omim_gene.nameroot.replace(".tar", ""))/   \
     --orphanet_gene ./$(inputs.orphanet_gene.nameroot.replace(".tar", ""))/ \
     --topmed ./$(inputs.topmed.nameroot.replace(".tar", ""))/ \
     --Cosmic_CancerGeneCensus ./$(inputs.Cosmic_CancerGeneCensus.nameroot.replace(".tar", ""))/ \
     --regeneron ./$(inputs.regeneron.nameroot.replace(".tar", ""))/ \
     --allofus ./$(inputs.allofus.nameroot.replace(".tar", ""))/ \
     --input_file $(inputs.input_file.path) --output_basename $(inputs.input_file.nameroot)
  shellQuote: false
