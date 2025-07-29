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
    - entryname: VEP_to_TSV_header_20240402.pl
      entry:
        $include: ../scripts/VEP_to_TSV_header_20240402.pl

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
    outputEval: $(inheritMetadata(self, inputs.vcf_wo_header))

baseCommand: [/bin/bash, -c]
arguments:
- position: 0
  valueFrom: |-
    set -eo pipefail; perl VEP_to_TSV_header_20240402.pl $(inputs.vcf_header.path) $(inputs.vcf_wo_header.path) | gzip -c > $(inputs.vcf_wo_header.nameroot.replace(/\.tsv/g, "")).tsv.gz
  shellQuote: true