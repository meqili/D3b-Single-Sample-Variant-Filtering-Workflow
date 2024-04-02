cwlVersion: v1.2
class: CommandLineTool
id: vep-vcf-to-tsv-header
label: VEP_vcf_to_TSV_header
$namespaces:
  sbg: https://sevenbridges.com

requirements:
- class: ShellCommandRequirement
- class: DockerRequirement
  dockerPull: 'perl:5.38.2'
- class: InlineJavascriptRequirement
- class: InitialWorkDirRequirement
  listing:
    - entryname: VEP_to_TSV_header.pl
      entry:
        $include: ../scripts/VEP_to_TSV_header.pl

inputs:
- id: vcf_header
  type: File
- id: vcf_wo_header
  type: File

outputs:
- id: output_vcf_w_header
  type: File
  outputBinding:
    glob: '*.tsv.gz'

baseCommand: [/bin/bash, -c]
arguments:
- position: 0
  valueFrom: |-
    set -eo pipefail; perl VEP_to_TSV_header.pl $(inputs.vcf_header.path) $(inputs.vcf_wo_header.path) | gzip -c > $(inputs.vcf_wo_header.basename).tsv.gz
  shellQuote: true