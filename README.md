# Single Sample Variant Filtering Workflow Updates

## VEP Inputs – June 2025

### Summary of Changes

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
     - **dbSNP**: `name` → `DBSNP_RSID`
     - **Cosmic Cancer Gene Census**:
       - `TIER` → `CGC_TIER`
       - `MUTATION_TYPES` → `CGC_MUTATION_TYPES`
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

---

## ANNOVAR Inputs – June 2025

_Changes to be documented here..._
