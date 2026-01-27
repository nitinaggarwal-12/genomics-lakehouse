# Genomics Lakehouse (Databricks + Glow)

This repo provides a Databricks workflow that reads VCF files with Glow and writes analytics-ready Delta tables for variant, consequence, and genotype analysis. It follows the approach described in:
["Building a modern genomics lakehouse with Databricks and Glow"](https://medium.com/@nitinaggarwal12/building-a-modern-genomics-lakehouse-with-databricks-and-glow-f7f5ac55deff).

## What is included

- `genomics_vcf_glow_genie_databricks.py` - main Databricks/Glow pipeline
- `genomics_vcf_glow_genie_databricks.ipynb` - notebook version for exploration
- `.databricks/` - Databricks metadata

## Architecture overview

The pipeline converts raw VCF data into three Delta tables:

- `variants_flat` - one row per variant allele (core fields + INFO_* columns)
- `variant_consequences_flat` - flattened CSQ annotations from INFO_CSQ
- `variant_calls` - one row per sample genotype call

The design keeps high-cardinality arrays and nested structures as JSON strings where appropriate to preserve fidelity while keeping the tables query-friendly.

## Prerequisites

- Databricks workspace
- Glow installed on the cluster
- A VCF file accessible via DBFS or Volumes

## Quick start

1. Upload a VCF to a path like `dbfs:/Volumes/users/nitin_aggarwal/vcf/test.vcf`.
2. Open `genomics_vcf_glow_genie_databricks.py` or the notebook in Databricks.
3. Update the config block:
   - `VCF_IN` - input VCF path
   - `DB` - target schema/database
4. Run the notebook/script to materialize the Delta tables.

## Output tables

**variants_flat**
- Core variant columns (chrom, pos, ref, alt)
- INFO_* fields (excluding INFO_CSQ)
- Stable keys: `variant_key`, `variant_id`

**variant_consequences_flat**
- Exploded and flattened INFO_CSQ entries
- One row per consequence per allele

**variant_calls**
- Genotype-level metrics (GT, DP, GQ, AD, PL)
- One row per sample per allele

## Notes

- The pipeline auto-merges schema evolution for INFO_* fields.
- CSQ and genotype sections gracefully create empty placeholders when missing.
