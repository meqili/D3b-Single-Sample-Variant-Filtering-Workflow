# Single Sample Variant Filtering Workflow Updates

## VEP Inputs – June 2025

### Summary of Changes

1. **Upgraded PySpark** from version **3.1** to **3.5**
2. **ClinVar** is now read from a **Delta table** instead of a tarred Parquet file
3. **Integrated new annotation databases**:
   - `dbSNP`: includes `rs` names
   - `Cosmic_CancerGeneCensus`
   - `All of Us`
   - `Regeneron`
4. **Reference gene table** (`genes`) is now sourced from a Delta table with key columns:
   - `entrez_gene_id`
   - `hgnc`
   - `ensembl_gene_id`
5. **Table joins using gene IDs**:
   - `hgmd_gene`
   - `Orphanet_gene`
   - `gencc`

---

## ANNOVAR Inputs – June 2025

_Changes to be documented here..._
