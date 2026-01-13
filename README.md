# Single Sample Variant Filtering Workflow Updates
# Summary of Changes
## Dragon VEP Inputs
### January 2026
1. Updated scripts based on Dragon inputs
   - Removed CSQ_gnomAD_AF from step1.py

---
## VEP Inputs
### January 2026
1. **Databases update**
   - **HGMD**: `2025Q3` → `2025Q4`
   - **GenCC**
   - **intervar**
2. **Maximum frequency update**
   - Renamed `max_gnomad_topmed` to `max_gtar` (includes gnomAD, TOPMed, All of Us, and Regeneron)

### October 2025
1. **HGMD update**
   - `2025Q1` → `2025Q3`

### June/July 2025
1. **PySpark Upgrade**
   - Upgraded from **3.1** to **3.5**.
   - This was necessary to support reading **Delta tables**, which are now used for several annotation sources.

2. **Delta Table Integration**
   - Several annotation datasets are now read directly from **Delta tables**, enabling better performance, scalability, and schema evolution:
     - **ClinVar**: Previously a tarred Parquet file; now a Delta table.
     - **Reference gene table** (`genes`)
     - **dbSNP**

3. **Column Standardization and Renaming**
   - To ensure consistency across joined datasets, several columns have been renamed:
     - **dbSNP**: 
       - column `name` → `DBSNP_RSID`
       - for those with multiple rsIDs we only use the one with the smallest rsID
     - **Cosmic Cancer Gene Census**:
       - `Tier` → `CGC_Tier`
       - `Mutation_Types` → `CGC_Mutation_Types`
     - **All of Us (AoU)**: `gvs_all_af` → `ALLOFUS_GVS_ALL_AF`
     - **Regeneron**: Supports `REGENERON_ALL_AF`

4. **Gene Reference Table Enhancements**
   - Now sourced from a Delta table.
   - Key columns used for downstream joins:
     - `entrez_gene_id`
     - `hgnc`
     - `ensembl_gene_id`

5. **Expanded Gene-Based Table Joins**
   - Gene ID normalization allows robust joins across:
     - `hgmd_gene`
     - `orphanet_gene`
     - `gencc`

6. **HGMD update**
   - `2025Q1` → `2025Q2`

7. **Basic quality control for vcf files (YG)**
   - Added following filterings to script `Gene_VEP_20250721.pl` replacing `Gene_VEP_20240617.pl`
      ```
      FILTER == "PASS"
      FORMAT_DP >= 10
      FORMAT_GQ >= 20
      INFO_QD >= 2.0
      INFO_FS <= 60.0
      INFO_MQ >= 40.0
      INFO_MQRankSum > -12.5
      INFO_ReadPosRankSum > -8.0
      INFO_SOR <= 3.0
      ```

8. **Minor Allele Frequency (MAF) Filtering**
   - Default MAF threshold changed from `0.0001` → `0.001`

---
## VEP Inputs + Gene Group Annotation 
### January 2026
1. update CPG and SFG gene list files: add a few genes 

### June/July 2025
1. after running workflow, add column `gene_group`: CPG, SFG, CPG/SFG, -
   - CPG: CancerPredispositionGenes_clean.txt
   - SFG: Miller_2023_GenetMed_ACMG_S.txt
   
---
## ANNOVAR Inputs – June 2025

_Changes to be documented here..._
