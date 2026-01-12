cwlVersion: v1.2
class: CommandLineTool
id: filter_by_gene
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
    - entryname: AttachGeneList_20240404.pl
      entry:
        $include: ../scripts/AttachGeneList_20240404.pl
baseCommand: [/bin/bash, -c]
arguments:
- position: 0
  valueFrom: |-
    set -eo pipefail; perl AttachGeneList_20240404.pl $(inputs.gene_list.path) $(inputs.vep_VWB_vcf.path) > $(inputs.vep_VWB_vcf.nameroot).GOI.tsv
  shellQuote: true

inputs:
  vep_VWB_vcf: {type: File}
  gene_list: {type: File}

outputs:
  gene_filtered_vcf_output:
    type: File
    outputBinding:
      glob: '*.GOI.tsv'
      outputEval: $(inheritMetadata(self, inputs.vep_VWB_vcf))