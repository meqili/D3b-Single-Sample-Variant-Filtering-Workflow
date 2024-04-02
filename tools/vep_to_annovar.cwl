cwlVersion: v1.2
class: CommandLineTool
id: VEP_to_Annovar
$namespaces:
  sbg: https://sevenbridges.com

requirements:
- class: ShellCommandRequirement
- class: DockerRequirement
  dockerPull: 'perl:5.38.2'
- class: InlineJavascriptRequirement
- class: InitialWorkDirRequirement
  listing:
    - entryname: Gene_VEP.pl
      entry:
        $include: ../scripts/Gene_VEP.pl
baseCommand: [/bin/bash, -c]
arguments:
- position: 0
  valueFrom: |-
    set -eo pipefail; gunzip -c $(inputs.vep_vcf.path) | perl Gene_VEP.pl | gzip -c > $(inputs.vep_vcf.nameroot).Annovar.vcf.gz
  shellQuote: true

inputs:
  vep_vcf: {type: File}

outputs:
  vep_to_annovar_output:
    type: File
    outputBinding:
      glob: '*.Annovar.vcf.gz'