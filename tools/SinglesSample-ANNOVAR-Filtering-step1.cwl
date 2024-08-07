#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool
id: SinglesSample-ANNOVAR-Filtering-step1
label: SinglesSample-ANNOVAR-Filtering-step1
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
  dockerPull: pgc-images.sbgenomics.com/d3b-bixu/pyspark:3.1.2
- class: InitialWorkDirRequirement
  listing:
  - entryname: SinglesSample-ANNOVAR-Filtering-step1.py
    entry:
      $include: ../scripts/SinglesSample-ANNOVAR-Filtering-step1.py
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
  default: 10
- id: sql_broadcastTimeout
  doc: .config("spark.sql.broadcastTimeout", 36000)
  type: int?
  default: 36000
- id: hgmd_var
  doc: the latest HGMD variant  parquet file dir
  type: File
- id: dbnsfp_annovar
  doc: dbnsfp annovar parquet file dir
  type: File
- id: clinvar
  doc: clinvar parquet file dir
  type: File
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
- id: spliceai
  type: File
- id: mmsplice
  type: File
- id: input_file_path
  type: File
  inputBinding:
    prefix: --input_file_path
    position: 3
    shellQuote: false
- id: maf
  doc: minor allele frequency (MAF) threshold in gnomAD and TOPMed
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

outputs:
- id: sample_VWB_filtering_ouput
  type: File
  outputBinding:
    glob: '*.VWB_result.tsv'
    outputEval: $(inheritMetadata(self, inputs.input_file_path))

baseCommand:
- tar
- -xvf
arguments:
- position: 1
  valueFrom: |-
    $(inputs.dbnsfp_annovar.path)  && tar -xvf $(inputs.clinvar.path)  && tar -xvf $(inputs.gencc.path) && tar -xvf $(inputs.hgmd_gene.path) && tar -xvf $(inputs.omim_gene.path) && tar -xvf $(inputs.orphanet_gene.path) && tar -xvf $(inputs.topmed.path) && tar -xvf $(inputs.spliceai.path) && tar -xvf $(inputs.mmsplice.path) && tar -xvf $(inputs.hgmd_var.path)
  shellQuote: false
- position: 2
  valueFrom: |-
    && spark-submit --packages io.projectglow:glow-spark3_2.12:1.1.2 \
    --conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec  \
    --conf spark.executor.memory=34G \
    --conf spark.executor.instances=3 \
    --conf spark.executor.cores=5 \
    --conf spark.kryoserializer.buffer.max=512m \
    --conf spark.sql.broadcastTimeout=$(inputs.sql_broadcastTimeout)  \
    --driver-memory $(inputs.spark_driver_mem)G  \
    SinglesSample-ANNOVAR-Filtering-step1.py  --clinvar ./$(inputs.clinvar.nameroot.replace(".tar", ""))/  \
    --dbnsfp ./$(inputs.dbnsfp_annovar.nameroot.replace(".tar", ""))/   --gencc ./$(inputs.gencc.nameroot.replace(".tar", ""))/ \
    --hgmd_gene ./$(inputs.hgmd_gene.nameroot.replace(".tar", ""))/  --hgmd_var ./$(inputs.hgmd_var.nameroot.replace(".tar", ""))/  \
    --omim_gene ./$(inputs.omim_gene.nameroot.replace(".tar", ""))/   --orphanet_gene ./$(inputs.orphanet_gene.nameroot.replace(".tar", ""))/ \
    --topmed ./$(inputs.topmed.nameroot.replace(".tar", ""))/ --spliceai ./$(inputs.spliceai.nameroot.replace(".tar", ""))/ \
    --mmsplice ./$(inputs.mmsplice.nameroot.replace(".tar", ""))/ --output_basemame $(inputs.input_file_path.nameroot)
  shellQuote: false
