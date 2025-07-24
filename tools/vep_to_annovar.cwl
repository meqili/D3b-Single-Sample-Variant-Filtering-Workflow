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
- class: InitialWorkDirRequirement
  listing:
    - entryname: Gene_VEP_20250721.pl
      entry:
        $include: ../scripts/Gene_VEP_20250721.pl
baseCommand: [/bin/bash, -c]
arguments:
- position: 0
  valueFrom: |-
    set -eo pipefail; gunzip -c $(inputs.vep_vcf.path) | perl Gene_VEP_20250721.pl 1> vcf_header.txt 2> $(inputs.vep_vcf.nameroot).Annovar.vcf
  shellQuote: true

inputs:
  vep_vcf: {type: File}

outputs:
  vep_to_annovar_output:
    type: File
    outputBinding:
      glob: '*.Annovar.vcf'
      outputEval: $(inheritMetadata(self, inputs.vep_vcf))
  vcf_header:
    type: File
    outputBinding:
      glob: 'vcf_header.txt'