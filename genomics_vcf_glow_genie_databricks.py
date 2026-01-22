# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC **FULL VCF -> DELTA FLATTEN (CSQ in separate table)**
# MAGIC
# MAGIC `Requires: Glow installed on cluster`

# COMMAND ----------

from pyspark.sql import functions as F, types as T

# ----------------------------
# 0) CONFIG
# ----------------------------
VCF_IN = "dbfs:/Volumes/users/nitin_aggarwal/vcf/test.vcf"   # <- your file
DB = "users.nitin_aggarwal"

TBL_VARIANTS   = f"{DB}.variants_flat"
TBL_CALLS      = f"{DB}.variant_calls"
TBL_CSQ        = f"{DB}.variant_consequences_flat"

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **1) HELPERS**
# MAGIC

# COMMAND ----------

def _dtype_of(df, colname: str):
    for f in df.schema.fields:
        if f.name == colname:
            return f.dataType
    return None

def _is_complex(dt: T.DataType) -> bool:
    return isinstance(dt, (T.ArrayType, T.StructType, T.MapType))

def _safe_col(df, name: str, cast_to: str = None):
    """Return df[name] if exists else NULL, optionally cast."""
    if name in df.columns:
        c = F.col(name)
        return c.cast(cast_to) if cast_to else c
    return F.lit(None).cast(cast_to) if cast_to else F.lit(None)

def _struct_has_field(struct_dt: T.DataType, field_name: str) -> bool:
    return isinstance(struct_dt, T.StructType) and any(f.name == field_name for f in struct_dt.fields)

def _flatten_struct_cols(prefix: str, struct_col: F.Column, struct_dt: T.StructType):
    """
    Recursively flatten a struct into a list of Columns.
    Arrays/maps are JSON-stringified (safe + generic).
    """
    out = []
    for f in struct_dt.fields:
        nm = f.name
        dt = f.dataType
        full = f"{prefix}{nm}"
        c = struct_col.getField(nm)
        if isinstance(dt, T.StructType):
            out.extend(_flatten_struct_cols(full + "_", c, dt))
        elif isinstance(dt, (T.ArrayType, T.MapType)):
            out.append(F.to_json(c).alias(full))
        else:
            out.append(c.cast("string").alias(full))
    return out

def _normalize_path(p: str) -> str:
    """
    Glow expects a Spark-readable path. Volumes can work as:
      - dbfs:/Volumes/...
      - /Volumes/... (sometimes)
    We'll try to keep as provided.
    """
    return p


# COMMAND ----------

# MAGIC %md
# MAGIC **2) READ VCF VIA GLOW**

# COMMAND ----------

vcf_path = _normalize_path(VCF_IN)

# Glow reader
base = (spark.read
        .format("vcf")
        # .option("includeSampleIds", "true")     # optional
        # .option("flattenInfoFields", "true")    # many builds already flatten INFO_*
        .load(vcf_path))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **3) NORMALIZE CORE VARIANT FIELDS**
# MAGIC
# MAGIC **Glow schema commonly uses:**
# MAGIC
# MAGIC `contigName, 
# MAGIC start, end, names(array), referenceAllele, alternateAlleles(array), qual, filters(array), splitFromMultiAllelic, genotypes(array<struct>)`

# COMMAND ----------

chrom_col = "contigName" if "contigName" in base.columns else ("chrom" if "chrom" in base.columns else None)
start_col = "start" if "start" in base.columns else ("pos" if "pos" in base.columns else None)
ref_col   = "referenceAllele" if "referenceAllele" in base.columns else ("ref" if "ref" in base.columns else None)
alts_col  = "alternateAlleles" if "alternateAlleles" in base.columns else ("alt" if "alt" in base.columns else None)

if chrom_col is None or start_col is None or ref_col is None or alts_col is None:
    raise ValueError(f"VCF schema missing required core columns. Found: {base.columns}")

# explode ALT to one row per allele
variants = (base
    .withColumn("chrom", F.col(chrom_col).cast("string"))
    # Glow start is 0-based; VCF POS is 1-based
    .withColumn("pos", (F.col(start_col) + F.lit(1)).cast("long") if start_col == "start" else F.col(start_col).cast("long"))
    .withColumn("ref", F.col(ref_col).cast("string"))
    .withColumn("alt", F.explode_outer(F.col(alts_col)))
    .withColumn("alt", F.col("alt").cast("string"))
)

# stable keys
variants = (variants
    .withColumn("variant_key", F.concat_ws(":", F.col("chrom"), F.col("pos").cast("string"), F.col("ref"), F.col("alt")))
    .withColumn("variant_id", F.xxhash64("variant_key").cast("string"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **4) BUILD variants_flat (EXCLUDES INFO_CSQ)**
# MAGIC
# MAGIC `Collect INFO_* columns dynamically; exclude INFO_CSQ from this table by design.`

# COMMAND ----------

info_cols = sorted([c for c in variants.columns if c.startswith("INFO_") and c != "INFO_CSQ"])

# For each INFO_* column:
# - if complex type -> stringify as JSON
# - else keep as-is
info_exprs = []
for c in info_cols:
    dt = _dtype_of(variants, c)
    if dt is None:
        continue
    if _is_complex(dt):
        info_exprs.append(F.to_json(F.col(c)).alias(c))
    else:
        # keep native type (double/int/string/bool) for performance
        info_exprs.append(F.col(c).alias(c))

variants_flat = variants.select(
    "variant_id",
    "variant_key",
    F.col("chrom"),
    F.col("pos"),
    F.col("ref"),
    F.col("alt"),
    # names/filters often arrays -> keep as JSON strings
    (F.to_json(_safe_col(variants, "names"))).alias("names") if "names" in variants.columns else F.lit(None).cast("string").alias("names"),
    F.col("qual").cast("double").alias("qual") if "qual" in variants.columns else F.lit(None).cast("double").alias("qual"),
    (F.to_json(_safe_col(variants, "filters"))).alias("filters") if "filters" in variants.columns else F.lit(None).cast("string").alias("filters"),
    _safe_col(variants, "splitFromMultiAllelic", "boolean").alias("splitFromMultiAllelic"),
    *info_exprs
)

# Overwrite with overwriteSchema (safe when schema evolves)
(variants_flat.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(TBL_VARIANTS)
)

print(f"✅ Wrote {TBL_VARIANTS}")

# COMMAND ----------

# MAGIC %md
# MAGIC **5) BUILD variant_consequences_flat (explode INFO_CSQ if present)**

# COMMAND ----------

if "INFO_CSQ" in variants.columns and isinstance(_dtype_of(variants, "INFO_CSQ"), T.ArrayType):
    csq_arr_dt = _dtype_of(variants, "INFO_CSQ")
    elem_dt = csq_arr_dt.elementType if isinstance(csq_arr_dt, T.ArrayType) else None

    csq = (variants
        .select("variant_id", "variant_key", "chrom", "pos", "ref", "alt", F.posexplode_outer("INFO_CSQ").alias("csq_index", "csq"))
    )

    # Flatten csq struct generically
    if isinstance(elem_dt, T.StructType):
        csq_flat_cols = _flatten_struct_cols("CSQ_", F.col("csq"), elem_dt)
        csq_flat = csq.select(
            "variant_id", "variant_key", "chrom", "pos", "ref", "alt", "csq_index",
            *csq_flat_cols
        )
    else:
        # If CSQ is not struct (some VCFs store it as raw string), keep raw
        csq_flat = csq.select(
            "variant_id", "variant_key", "chrom", "pos", "ref", "alt", "csq_index",
            F.col("csq").cast("string").alias("CSQ_raw")
        )

    (csq_flat.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(TBL_CSQ)
    )

    print(f"✅ Wrote {TBL_CSQ}")
else:
    # Create an empty table (optional) so downstream demos don't break
    empty_csq = spark.createDataFrame([], schema=T.StructType([
        T.StructField("variant_id", T.StringType(), True),
        T.StructField("variant_key", T.StringType(), True),
        T.StructField("chrom", T.StringType(), True),
        T.StructField("pos", T.LongType(), True),
        T.StructField("ref", T.StringType(), True),
        T.StructField("alt", T.StringType(), True),
        T.StructField("csq_index", T.IntegerType(), True),
        T.StructField("CSQ_raw", T.StringType(), True),
    ]))

    (empty_csq.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(TBL_CSQ)
    )

    print(f"⚠️ INFO_CSQ not present; wrote empty placeholder {TBL_CSQ}")


# COMMAND ----------

# MAGIC %md
# MAGIC **6) BUILD variant_calls (explode genotypes if present)**

# COMMAND ----------


# ----------------------------
# Glow commonly provides genotypes: array<struct<sampleId:string, calls:array<int>, phased:boolean, ...>>
if "genotypes" in base.columns and isinstance(_dtype_of(base, "genotypes"), T.ArrayType):
    gts_dt = _dtype_of(base, "genotypes")
    gte_dt = gts_dt.elementType if isinstance(gts_dt, T.ArrayType) else None

    calls = (variants
        .select("variant_id", "variant_key", "chrom", "pos", "ref", "alt", F.explode_outer("genotypes").alias("g"))
    )

    # Determine genotype subfields safely
    if isinstance(gte_dt, T.StructType):
        has_sample = _struct_has_field(gte_dt, "sampleId")
        has_calls  = _struct_has_field(gte_dt, "calls")
        has_phased = _struct_has_field(gte_dt, "phased")
        has_dp     = _struct_has_field(gte_dt, "depth")
        has_gq     = _struct_has_field(gte_dt, "conditionalQuality") or _struct_has_field(gte_dt, "GQ")
        has_ad     = _struct_has_field(gte_dt, "alleleDepths") or _struct_has_field(gte_dt, "AD")
        has_pl     = _struct_has_field(gte_dt, "genotypeLikelihoods") or _struct_has_field(gte_dt, "PL")

        sample_id = F.col("g.sampleId").cast("string") if has_sample else F.lit(None).cast("string")
        phased    = F.col("g.phased").cast("boolean") if has_phased else F.lit(None).cast("boolean")

        # DP, GQ, AD, PL (stringify arrays)
        DP = (F.col("g.depth").cast("int") if has_dp else F.lit(None).cast("int")).alias("DP")

        if _struct_has_field(gte_dt, "conditionalQuality"):
            GQc = F.col("g.conditionalQuality")
        elif _struct_has_field(gte_dt, "GQ"):
            GQc = F.col("g.GQ")
        else:
            GQc = F.lit(None)
        GQ = GQc.cast("int").alias("GQ")

        if _struct_has_field(gte_dt, "alleleDepths"):
            ADc = F.col("g.alleleDepths")
        elif _struct_has_field(gte_dt, "AD"):
            ADc = F.col("g.AD")
        else:
            ADc = F.lit(None)
        AD = F.to_json(ADc).alias("AD")

        if _struct_has_field(gte_dt, "genotypeLikelihoods"):
            PLc = F.col("g.genotypeLikelihoods")
        elif _struct_has_field(gte_dt, "PL"):
            PLc = F.col("g.PL")
        else:
            PLc = F.lit(None)
        PL = F.to_json(PLc).alias("PL")

        # Build GT ONLY if calls exists and is array<int>
        if has_calls and isinstance([f for f in gte_dt.fields if f.name == "calls"][0].dataType, T.ArrayType):
            calls_arr = F.col("g.calls")

            # Convert int calls to strings, -1 -> "."
            gt_tokens = F.transform(
                calls_arr,
                lambda x: F.when(x < 0, F.lit(".")).otherwise(x.cast("string"))
            )

            # array_join expects a literal separator; do phased true/false separately
            GT = (F.when(calls_arr.isNull(), F.lit(None).cast("string"))
                    .when(phased == True,  F.array_join(gt_tokens, "|"))
                    .otherwise(             F.array_join(gt_tokens, "/"))
                 ).alias("GT")
        else:
            GT = F.lit(None).cast("string").alias("GT")

        calls_out = (calls
            .select(
                "variant_id",
                sample_id.alias("sample_id"),
                GT,
                DP,
                GQ,
                AD,
                PL,
                phased.alias("phased")
            )
        )

    else:
        # If genotype element is not struct, just store raw
        calls_out = calls.select(
            "variant_id",
            F.lit(None).cast("string").alias("sample_id"),
            F.col("g").cast("string").alias("GT"),
            F.lit(None).cast("int").alias("DP"),
            F.lit(None).cast("int").alias("GQ"),
            F.lit(None).cast("string").alias("AD"),
            F.lit(None).cast("string").alias("PL"),
            F.lit(None).cast("boolean").alias("phased"),
        )

    (calls_out.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(TBL_CALLS)
    )

    print(f"✅ Wrote {TBL_CALLS}")

else:
    # Create empty placeholder table
    empty_calls = spark.createDataFrame([], schema=T.StructType([
        T.StructField("variant_id", T.StringType(), True),
        T.StructField("sample_id", T.StringType(), True),
        T.StructField("GT", T.StringType(), True),
        T.StructField("DP", T.IntegerType(), True),
        T.StructField("GQ", T.IntegerType(), True),
        T.StructField("AD", T.StringType(), True),
        T.StructField("PL", T.StringType(), True),
        T.StructField("phased", T.BooleanType(), True),
    ]))

    (empty_calls.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(TBL_CALLS)
    )

    print(f"⚠️ genotypes not present; wrote empty placeholder {TBL_CALLS}")

# COMMAND ----------

# MAGIC %md
# MAGIC **7) QUICK SANITY CHECKS**
# MAGIC

# COMMAND ----------


print("\n=== COUNTS ===")
print("variants_flat:", spark.table(TBL_VARIANTS).count())
print("variant_consequences_flat:", spark.table(TBL_CSQ).count())
print("variant_calls:", spark.table(TBL_CALLS).count())

print("\n=== SAMPLE PREVIEW (variants_flat) ===")
display(spark.table(TBL_VARIANTS).limit(20))

print("\n=== SAMPLE PREVIEW (variant_calls) ===")
display(spark.table(TBL_CALLS).limit(20))

print("\n=== SAMPLE PREVIEW (variant_consequences_flat) ===")
display(spark.table(TBL_CSQ).limit(20))
