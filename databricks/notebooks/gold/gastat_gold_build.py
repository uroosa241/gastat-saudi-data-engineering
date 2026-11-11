# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # GASTAT Gold Layer Build (Power BI star schema)
# MAGIC
# MAGIC Builds `gastatksa.gold` from `gastatksa.silver` only: conformed dimensions with surrogate keys, three fact tables (expenditure, income, quarterly activity), data-quality tables derived from `silver._dq_audit` / `silver._quarantine`, and six analytical views. All values are nominal (current prices, SAR million). Periods that failed a silver hard gate are not published, and missing components return NULL. Tests run at the end and the run fails if any test fails. Rerun the whole notebook; all inputs come from the widgets and the contract cell.

# COMMAND ----------

# DBTITLE 1,Parameters and model contract
# Parameters (widgets) and the model contract. Every input of the run is defined in this cell.
import datetime as dt
from functools import reduce
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

dbutils.widgets.text("catalog", "gastatksa", "Catalog")
dbutils.widgets.text("silver_schema", "silver", "Silver schema (read only)")
dbutils.widgets.text("gold_schema", "gold", "Gold schema (written)")
dbutils.widgets.text("powerbi_principal", "", "Power BI group or SP application id (read-only grant)")

GOLD_LOCATION = "abfss://gold@stgastsaudi12.dfs.core.windows.net/"
TOLERANCE_SAR_MN = 1          # identity tolerance for rounded source values
SHARE_TOLERANCE_PP = 0.1      # tolerance when component shares are summed (percentage points)
VALUE_BASIS = "nominal (current prices)"
TAGS = {"domain": "national_accounts", "layer": "gold", "source": "GASTAT", "value_basis": "nominal"}

FACTS = {
    "fact_gdp_expenditure": {"silver": "gdp_expenditure", "dim": "dim_expenditure_item", "key": "expenditure_item_key",
                             "grain": "annual", "expected_range": ("2010", "2025"), "gdp_code": "EX_GDP"},
    "fact_gdp_income": {"silver": "gdp_income", "dim": "dim_income_factor", "key": "income_factor_key",
                        "grain": "annual", "expected_range": ("2018", "2024"), "gdp_code": "IN_GDP"},
    "fact_gdp_activity_qtr": {"silver": "gdp_isic_qtr", "dim": "dim_economic_activity", "key": "economic_activity_key",
                              "grain": "quarterly", "expected_range": ("2010-Q1", "2026-Q2"), "gdp_code": "EA_GDP"},
}
# Silver cross-dataset identities and the datasets whose periods they validate
CROSS_RULE_DATASETS = {"cross_exp_gdp_eq_income_gdp": ["gdp_expenditure", "gdp_income"],
                       "cross_4_quarters_eq_annual_gdp": ["gdp_expenditure", "gdp_isic_qtr"]}
# GASTAT analytical grouping of GVA leaf activities: oil activities, government, non-oil private sector
OIL_ACTIVITIES = {"EA_MIN_OIL", "EA_MFG_REF"}
GOVERNMENT_ACTIVITIES = {"EA_GOV"}

def get_params():
    """Read the widgets once per run and stamp the refresh time."""
    g = dbutils.widgets.get
    # Safety guard: every write in this notebook goes to the gold schema; refuse to run if it could point at silver
    if g("gold_schema") != "gold" or g("gold_schema") == g("silver_schema"):
        raise ValueError(f"Refusing to run: writes must target the 'gold' schema (got gold_schema={g('gold_schema')!r}, silver_schema={g('silver_schema')!r})")
    return {"catalog": g("catalog"), "silver_schema": g("silver_schema"), "gold_schema": g("gold_schema"),
            "powerbi_principal": g("powerbi_principal").strip(),
            "run_ts": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")}

def silver_t(p, t):
    return spark.table(f"{p['catalog']}.{p['silver_schema']}.{t}")

def gold(p, t):
    return f"{p['catalog']}.{p['gold_schema']}.{t}"

def pkey(y, q):
    return str(y) if q is None else f"{y}-Q{q}"

def dkey(y, q):
    return y * 100 + (q or 0)

def period_range(grain, lo, hi):
    """All (year, quarter) periods from lo to hi inclusive; quarter is None for annual."""
    if grain == "annual":
        return [(y, None) for y in range(lo[0], hi[0] + 1)]
    return [(i // 4, i % 4 + 1) for i in range(lo[0] * 4 + lo[1] - 1, hi[0] * 4 + hi[1])]

# COMMAND ----------

# DBTITLE 1,Table definitions and DDL helpers
# Gold table definitions with EN + AR column comments, and idempotent create / overwrite helpers
def item_dim_cols(key, activity=False):
    cols = [
        (key, "BIGINT NOT NULL", "Surrogate key (stable hash of the code) | المفتاح البديل"),
        ("code", "STRING NOT NULL", "Stable item code | رمز البند"),
        ("name_en", "STRING NOT NULL", "Name in English | الاسم بالإنجليزية"),
        ("name_ar", "STRING", "Name in Arabic | الاسم بالعربية"),
        ("parent_code", "STRING", "Parent item code | رمز البند الأب"),
        ("parent_name_en", "STRING", "Parent name in English | اسم البند الأب"),
        ("hierarchy_level", "INT NOT NULL", "Hierarchy level, 0 = top total | المستوى في التسلسل الهرمي"),
        ("is_aggregate", "BOOLEAN NOT NULL", "True if the item is a total of other items; never add it to its components | بند تجميعي لا يجمع مع مكوناته"),
        ("is_leaf", "BOOLEAN NOT NULL", "True if the item has no children | بند بلا مكونات فرعية"),
        ("is_memo", "BOOLEAN NOT NULL", "Memo item outside the additive hierarchy | بند للعلم خارج التجميع"),
        ("sign_in_parent", "INT", "Sign when added into the parent (+1 or -1) | الإشارة عند الجمع في البند الأب"),
        ("sort_order", "INT NOT NULL", "Display order | ترتيب العرض"),
    ]
    if activity:
        cols += [("oil_group", "STRING", "oil or non_oil (GVA leaf activities only) | نفطي أو غير نفطي"),
                 ("institutional_sector", "STRING", "oil, government or private_non_oil (GVA leaf activities only) | نفطي أو حكومي أو خاص غير نفطي")]
    return cols

def fact_cols(key):
    return [
        ("date_key", "INT NOT NULL", "Key to dim_date (YYYY00 for a year, YYYYQQ for a quarter) | مفتاح الفترة"),
        (key, "BIGINT NOT NULL", "Key to the item dimension | مفتاح البند"),
        ("obs_value", "BIGINT NOT NULL", "Nominal value, SAR million, current prices | القيمة الاسمية بالمليون ريال بالأسعار الجارية"),
        ("is_complete_period", "BOOLEAN NOT NULL", "True if every series of the dataset is present in this period | الفترة مكتملة"),
        ("dq_status", "STRING NOT NULL", "PASS, PASS_WITH_WARNINGS or INCOMPLETE, from silver._dq_audit; failed periods are not published | حالة جودة البيانات"),
        ("last_refreshed_utc", "TIMESTAMP NOT NULL", "Gold refresh time (UTC) | وقت آخر تحديث"),
    ]

DIM_DATE_COLS = [
    ("date_key", "INT NOT NULL", "Period key: YYYY00 for a year, YYYYQQ for a quarter | مفتاح الفترة"),
    ("date", "DATE NOT NULL", "First day of the period | تاريخ بداية الفترة"),
    ("year", "INT NOT NULL", "Calendar year | السنة"),
    ("quarter", "INT", "Quarter 1-4; NULL for a year | الربع"),
    ("quarter_label", "STRING NOT NULL", "Q1-Q4, or FY for a year | تسمية الربع"),
    ("period_key", "STRING NOT NULL", "YYYY or YYYY-Qn | رمز الفترة"),
    ("period_label", "STRING NOT NULL", "Display label, e.g. 2012 or 2012 Q1 | تسمية الفترة"),
    ("period_type", "STRING NOT NULL", "annual or quarterly | نوع الفترة"),
    ("period_start_date", "DATE NOT NULL", "First day of the period | تاريخ بداية الفترة"),
    ("period_end_date", "DATE NOT NULL", "Last day of the period | تاريخ نهاية الفترة"),
    ("period_seq", "INT NOT NULL", "Sort sequence: year, or year*4+quarter-1 | التسلسل الزمني"),
    ("prev_period_date_key", "INT NOT NULL", "date_key of the previous period (year or quarter) | مفتاح الفترة السابقة"),
    ("same_period_prev_year_date_key", "INT NOT NULL", "date_key of the same period one year earlier | مفتاح نفس الفترة من العام السابق"),
]
DQ_PERIOD_COLS = [
    ("dataset", "STRING NOT NULL", "Silver dataset | مجموعة البيانات"),
    ("date_key", "INT NOT NULL", "Key to dim_date | مفتاح الفترة"),
    ("period_key", "STRING NOT NULL", "YYYY or YYYY-Qn | رمز الفترة"),
    ("year", "INT NOT NULL", "Calendar year | السنة"),
    ("quarter", "INT", "Quarter, NULL for a year | الربع"),
    ("rows_present", "INT NOT NULL", "Series present in the period | عدد السلاسل المتوفرة"),
    ("rows_expected", "INT NOT NULL", "Series expected (all dimension members) | عدد السلاسل المتوقعة"),
    ("completeness_pct", "DOUBLE NOT NULL", "rows_present / rows_expected x 100 | نسبة الاكتمال"),
    ("dq_status", "STRING NOT NULL", "PASS, PASS_WITH_WARNINGS, INCOMPLETE, MISSING or FAILED | حالة الجودة"),
    ("failed_checks", "ARRAY<STRING>", "Hard silver checks failed in this period | الفحوصات الفاشلة"),
    ("warning_checks", "ARRAY<STRING>", "Warning-level silver checks failed in this period | فحوصات التحذير"),
    ("is_published", "BOOLEAN NOT NULL", "False if the period failed a hard gate and is withheld from the facts | الفترة منشورة"),
    ("silver_run_id", "STRING NOT NULL", "Silver run used | معرف تشغيل الطبقة الفضية"),
]
DQ_DATASET_COLS = [
    ("dataset", "STRING NOT NULL", "Silver dataset | مجموعة البيانات"),
    ("gold_table", "STRING NOT NULL", "Gold fact table | جدول الحقائق"),
    ("indicator_code", "STRING", "GASTAT indicator code | رمز المؤشر"),
    ("silver_run_id", "STRING NOT NULL", "Latest merged silver run | آخر تشغيل ناجح للطبقة الفضية"),
    ("silver_run_utc", "TIMESTAMP", "Time of that silver run (UTC) | وقت التشغيل"),
    ("silver_newer_failed_run", "BOOLEAN NOT NULL", "A newer silver run exists that did not merge | يوجد تشغيل أحدث فاشل"),
    ("source_container", "STRING", "Container silver read (bronze expected) | الحاوية المصدر"),
    ("silver_complete", "BOOLEAN", "Silver marked the feed complete | اكتمال البيانات"),
    ("silver_gold_ready", "BOOLEAN", "Silver marked the dataset gold ready | جاهزية البيانات للطبقة الذهبية"),
    ("silver_blockers", "ARRAY<STRING>", "Silver checks blocking gold | العوائق"),
    ("rows_expected", "BIGINT", "Members x periods | السجلات المتوقعة"),
    ("rows_present", "BIGINT", "Rows present in silver | السجلات المتوفرة"),
    ("rows_published", "BIGINT", "Rows published to the gold fact | السجلات المنشورة"),
    ("completeness_pct", "DOUBLE", "rows_present / rows_expected x 100 | نسبة الاكتمال"),
    ("first_period", "STRING", "First period with data | أول فترة"),
    ("latest_period", "STRING", "Latest published period with data | آخر فترة متاحة"),
    ("periods_total", "BIGINT", "Periods between first and last | عدد الفترات"),
    ("periods_complete", "BIGINT", "Periods with every series present | الفترات المكتملة"),
    ("periods_not_published", "BIGINT", "Periods withheld because they failed a hard gate | الفترات غير المنشورة"),
    ("last_refreshed_utc", "TIMESTAMP NOT NULL", "Gold refresh time (UTC) | وقت آخر تحديث"),
]

TABLES = {
    "dim_date": ("Shared period dimension: one row per year and per quarter. | بُعد الفترات المشترك: سنوات وأرباع", DIM_DATE_COLS, ["date_key"]),
    "dim_economic_activity": ("Economic activities (ISIC) with hierarchy and oil / government grouping. | الأنشطة الاقتصادية مع التسلسل الهرمي", item_dim_cols("economic_activity_key", True), ["economic_activity_key"]),
    "dim_expenditure_item": ("Expenditure components of GDP with hierarchy. | بنود الإنفاق على الناتج المحلي الإجمالي", item_dim_cols("expenditure_item_key"), ["expenditure_item_key"]),
    "dim_income_factor": ("Income components of GDP with hierarchy. | عناصر الدخل في الناتج المحلي الإجمالي", item_dim_cols("income_factor_key"), ["income_factor_key"]),
    "fact_gdp_expenditure": ("GDP by expenditure, annual, nominal SAR million. Grain: item x year. | الناتج حسب الإنفاق، سنوي، بالقيمة الاسمية", fact_cols("expenditure_item_key"), ["date_key", "expenditure_item_key"]),
    "fact_gdp_income": ("GDP by income, annual, nominal SAR million. Grain: item x year. | الناتج حسب الدخل، سنوي، بالقيمة الاسمية", fact_cols("income_factor_key"), ["date_key", "income_factor_key"]),
    "fact_gdp_activity_qtr": ("GDP by economic activity, quarterly, nominal SAR million. Grain: activity x quarter. INCOMPLETE feed: check dq_dataset_status. | الناتج حسب النشاط، ربع سنوي، بيانات غير مكتملة", fact_cols("economic_activity_key"), ["date_key", "economic_activity_key"]),
    "dq_period_status": ("Data quality per dataset and period, derived from silver._dq_audit and silver._quarantine. | جودة البيانات لكل فترة", DQ_PERIOD_COLS, ["dataset", "date_key"]),
    "dq_dataset_status": ("Data quality and freshness per dataset. | جودة وحداثة البيانات لكل مجموعة", DQ_DATASET_COLS, ["dataset"]),
}

def esc(s):
    return s.replace("\\", "\\\\").replace("'", "\\'")

def spark_type(tp):
    return tp.replace(" NOT NULL", "").lower()

def tag_sql():
    return ", ".join(f"'{k}' = '{v}'" for k, v in TAGS.items())

def ensure_table(p, name, comment, cols, pk):
    """Create the table if missing; recreate it only if columns, types, nullability or comments changed."""
    t = gold(p, name)
    want = [(n, spark_type(tp), "NOT NULL" not in tp, c) for n, tp, c in cols]
    have = None
    if spark.catalog.tableExists(t):
        have = [(f.name, f.dataType.simpleString(), f.nullable, f.metadata.get("comment")) for f in spark.table(t).schema]
    if have != want:
        col_ddl = ",\n  ".join(f"`{n}` {tp} COMMENT '{esc(c)}'" for n, tp, c in cols)
        spark.sql(f"CREATE OR REPLACE TABLE {t} (\n  {col_ddl},\n  CONSTRAINT pk_{name} PRIMARY KEY ({', '.join(pk)})\n)")
    spark.sql(f"COMMENT ON TABLE {t} IS '{esc(comment)}'")
    spark.sql(f"ALTER TABLE {t} SET TAGS ({tag_sql()})")

def overwrite(p, name, df):
    """Atomic full refresh that keeps the table definition, comments and tags."""
    cols = TABLES[name][1]
    df.select(*[F.col(n).cast(spark_type(tp)).alias(n) for n, tp, _ in cols]).write.insertInto(gold(p, name), overwrite=True)

def setup_schema(p):
    sch = f"{p['catalog']}.{p['gold_schema']}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {sch} MANAGED LOCATION '{GOLD_LOCATION}'")
    spark.sql(f"COMMENT ON SCHEMA {sch} IS '{esc('GASTAT national accounts, gold layer: star schema and views for Power BI, nominal values. | الحسابات القومية - الطبقة الذهبية')}'")
    spark.sql(f"ALTER SCHEMA {sch} SET TAGS ({tag_sql()})")
    for name, (comment, cols, pk) in TABLES.items():
        ensure_table(p, name, comment, cols, pk)

# COMMAND ----------

# DBTITLE 1,Silver run selection and data-quality tables
# Pick the latest MERGED silver run per dataset and derive period-level quality from its audit and quarantine (SELECT only)
def latest_silver_runs(p):
    st = silver_t(p, "_dataset_status")
    merged = st.filter("merged").groupBy("dataset").agg(F.max_by(F.struct(*st.columns), "run_ts").alias("s")).select("s.*")
    latest_any = st.groupBy("dataset").agg(F.max("run_ts").alias("latest_any_ts"))
    runs_df = merged.join(latest_any, "dataset")
    run_ids = {r["dataset"]: r["run_id"] for r in runs_df.select("dataset", "run_id").collect()}
    missing = [s["silver"] for s in FACTS.values() if s["silver"] not in run_ids]
    if missing:
        raise RuntimeError(f"No merged silver run for {missing}; gold cannot be built")
    return runs_df, run_ids

def build_period_status(p, run_ids):
    audit, quarantine = silver_t(p, "_dq_audit"), silver_t(p, "_quarantine")
    frames = []
    for spec in FACTS.values():
        ds, run_id = spec["silver"], run_ids[spec["silver"]]
        s = silver_t(p, ds)
        rng = s.agg(F.min(F.struct("year", "quarter")).alias("lo"), F.max(F.struct("year", "quarter")).alias("hi")).first()
        grid = spark.createDataFrame([(pkey(y, q), y, q, dkey(y, q)) for y, q in period_range(spec["grain"], rng["lo"], rng["hi"])],
                                     "period_key string, year int, quarter int, date_key int")
        present = s.groupBy("period_key").agg(F.count("*").alias("rows_present"))
        expected = silver_t(p, spec["dim"]).count()
        cross = [r for r, dss in CROSS_RULE_DATASETS.items() if ds in dss]
        in_scope = (F.col("dataset") == ds) | ((F.col("dataset") == "cross") & F.col("check_name").isin(cross))
        fails = (audit.filter((F.col("run_id") == run_id) & (F.col("check_type") == "identity") & (F.col("status") == "FAIL") & in_scope)
                 .select("check_name", "severity", F.explode(F.from_json("details", "struct<failing_periods: array<array<string>>>")["failing_periods"]).alias("fp"))
                 .select("check_name", "severity", F.col("fp")[0].alias("fail_period")))
        quarantined = (quarantine.filter((F.col("_run_id") == run_id) & (F.col("dataset") == ds) & F.col("period_key").isNotNull())
                       .select(F.lit("quarantined_rows").alias("check_name"), F.lit("error").alias("severity"), F.col("period_key").alias("fail_period")))
        issues = (grid.alias("g").join(fails.unionByName(quarantined).alias("i"),
                      F.expr("i.fail_period = g.period_key OR (length(i.fail_period) = 4 AND g.year = try_cast(i.fail_period AS INT))"))
                  .groupBy(F.col("g.period_key").alias("period_key"))
                  .agg(F.array_sort(F.collect_set(F.when(F.col("severity") == "error", F.col("check_name")))).alias("failed_checks"),
                       F.array_sort(F.collect_set(F.when(F.col("severity") != "error", F.col("check_name")))).alias("warning_checks")))
        empty = F.array().cast("array<string>")
        out = (grid.join(present, "period_key", "left").join(issues, "period_key", "left")
               .withColumn("rows_present", F.coalesce("rows_present", F.lit(0)))
               .withColumn("rows_expected", F.lit(expected))
               .withColumn("failed_checks", F.coalesce("failed_checks", empty))
               .withColumn("warning_checks", F.coalesce("warning_checks", empty))
               .withColumn("completeness_pct", F.round(F.col("rows_present") * 100.0 / F.col("rows_expected"), 2))
               .withColumn("is_published", F.size("failed_checks") == 0)
               .withColumn("dq_status", F.when(~F.col("is_published"), "FAILED")
                                         .when(F.col("rows_present") == 0, "MISSING")
                                         .when(F.col("rows_present") < F.col("rows_expected"), "INCOMPLETE")
                                         .when(F.size("warning_checks") > 0, "PASS_WITH_WARNINGS").otherwise("PASS"))
               .withColumn("dataset", F.lit(ds)).withColumn("silver_run_id", F.lit(run_id)))
        frames.append(out)
    return reduce(lambda a, b: a.unionByName(b), frames)

def build_dataset_status(p, runs_df):
    q = spark.table(gold(p, "dq_period_status"))
    agg = q.groupBy("dataset").agg(
        F.sum("rows_expected").alias("rows_expected"), F.sum("rows_present").alias("rows_present"),
        F.sum(F.when(F.col("is_published"), F.col("rows_present")).otherwise(0)).alias("rows_published"),
        F.min(F.when(F.col("rows_present") > 0, F.col("period_key"))).alias("first_period"),
        F.max(F.when((F.col("rows_present") > 0) & F.col("is_published"), F.col("period_key"))).alias("latest_period"),
        F.count("*").alias("periods_total"),
        F.count(F.when(F.col("rows_present") == F.col("rows_expected"), 1)).alias("periods_complete"),
        F.count(F.when(~F.col("is_published"), 1)).alias("periods_not_published"))
    tables = spark.createDataFrame([(s["silver"], f) for f, s in FACTS.items()], "dataset string, gold_table string")
    return (runs_df.select("dataset", "indicator_code", F.col("run_id").alias("silver_run_id"), F.col("run_ts").alias("silver_run_utc"),
                           (F.col("latest_any_ts") > F.col("run_ts")).alias("silver_newer_failed_run"), "source_container",
                           F.col("complete").alias("silver_complete"), F.col("gold_ready").alias("silver_gold_ready"), F.col("blockers").alias("silver_blockers"))
            .join(tables, "dataset").join(agg, "dataset")
            .withColumn("completeness_pct", F.round(F.col("rows_present") * 100.0 / F.col("rows_expected"), 2))
            .withColumn("last_refreshed_utc", F.lit(p["run_ts"]).cast("timestamp")))

# COMMAND ----------

# DBTITLE 1,Dimension and fact builders
# Dimensions (surrogate key = stable xxhash64 of dimension name + code) and facts
def build_item_dim(p, spec):
    activity = spec["dim"] == "dim_economic_activity"
    cols = TABLES[spec["dim"]][1]
    rows = [r.asDict() for r in silver_t(p, spec["dim"]).orderBy("sort_order").collect()]
    by_code = {r["item_code"]: r for r in rows}
    parents = {r["parent_code"] for r in rows if r["parent_code"]}

    def ancestors(code):
        out, c = [], by_code[code]["parent_code"]
        while c:
            out.append(c)
            c = by_code[c]["parent_code"]
        return out

    recs = []
    for r in rows:
        code, is_leaf = r["item_code"], r["item_code"] not in parents
        rec = [0, code, r["label_en"], r["label_ar"], r["parent_code"], by_code[r["parent_code"]]["label_en"] if r["parent_code"] else None,
               r["hierarchy_level"], r["is_aggregate"], is_leaf, r["is_memo"],
               None if r["sign_in_parent"] is None else int(r["sign_in_parent"]), r["sort_order"]]
        if activity:
            gva_leaf = is_leaf and "EA_GVA" in ancestors(code)
            oil = code in OIL_ACTIVITIES
            rec += [("oil" if oil else "non_oil") if gva_leaf else None,
                    ("oil" if oil else "government" if code in GOVERNMENT_ACTIVITIES else "private_non_oil") if gva_leaf else None]
        recs.append(tuple(rec))
    schema = ", ".join(f"`{n}` {spark_type(tp)}" for n, tp, _ in cols)
    return spark.createDataFrame(recs, schema).withColumn(spec["key"], F.xxhash64(F.lit(spec["dim"]), F.col("code")))

def build_dim_date(p):
    """Every year and every quarter spanned by the datasets."""
    rows = spark.table(gold(p, "dq_period_status")).select("year", "quarter").distinct().collect()
    years = [r["year"] for r in rows]
    quarters = sorted((r["year"], r["quarter"]) for r in rows if r["quarter"] is not None)
    periods = [(y, None) for y in range(min(years), max(years) + 1)]
    if quarters:
        periods += period_range("quarterly", quarters[0], quarters[-1])
    recs = []
    for y, q in periods:
        if q is None:
            start, end, seq, prev, prev_year = dt.date(y, 1, 1), dt.date(y, 12, 31), y, dkey(y - 1, None), dkey(y - 1, None)
            qlabel, label, ptype = "FY", str(y), "annual"
        else:
            start = dt.date(y, 3 * q - 2, 1)
            end = (dt.date(y + 1, 1, 1) if q == 4 else dt.date(y, 3 * q + 1, 1)) - dt.timedelta(days=1)
            seq, prev, prev_year = y * 4 + q - 1, (dkey(y, q - 1) if q > 1 else dkey(y - 1, 4)), dkey(y - 1, q)
            qlabel, label, ptype = f"Q{q}", f"{y} Q{q}", "quarterly"
        recs.append((dkey(y, q), start, y, q, qlabel, pkey(y, q), label, ptype, start, end, seq, prev, prev_year))
    schema = ", ".join(f"`{n}` {spark_type(tp)}" for n, tp, _ in DIM_DATE_COLS)
    return spark.createDataFrame(recs, schema)

def build_fact(p, spec):
    """Silver rows -> surrogate keys; periods that failed a hard silver gate are not published."""
    s = silver_t(p, spec["silver"]).select("item_code", "period_key", "obs_value")
    dim = spark.table(gold(p, spec["dim"])).select(F.col("code").alias("item_code"), spec["key"])
    dq = (spark.table(gold(p, "dq_period_status")).filter(F.col("dataset") == spec["silver"])
          .select("period_key", "date_key", "dq_status", "rows_present", "rows_expected", "is_published"))
    return (s.join(dim, "item_code").join(dq, "period_key").filter("is_published")
            .select("date_key", spec["key"], "obs_value",
                    (F.col("rows_present") == F.col("rows_expected")).alias("is_complete_period"), "dq_status",
                    F.lit(p["run_ts"]).cast("timestamp").alias("last_refreshed_utc")))

# COMMAND ----------

# DBTITLE 1,Analytical views
# Analytical views. Every value is nominal; missing periods or components give NULL; growth only between periods that both exist.
# Each body is a CTE chain ending in `final`; the column list (with EN + AR comments) defines the output.
def c(en, ar):
    return f"{en} | {ar}"

COMMON = {
    "value_basis": c("Always nominal (current prices); no value or growth here is real | القيم اسمية بالأسعار الجارية", "ليست حقيقية"),
    "period_type": c("annual or quarterly", "نوع الفترة"), "date_key": c("Key to dim_date", "مفتاح الفترة"),
    "period_key": c("YYYY or YYYY-Qn", "رمز الفترة"), "year": c("Year", "السنة"), "quarter": c("Quarter; NULL for a year", "الربع"),
    "is_complete_period": c("Every series of the dataset is present in the period", "الفترة مكتملة"),
    "dq_status": c("PASS, PASS_WITH_WARNINGS, INCOMPLETE or MISSING", "حالة الجودة"),
}
def cols(*spec):
    """Column specs: a name from COMMON, or (name, en, ar)."""
    return [(s, COMMON[s]) if isinstance(s, str) else (s[0], c(s[1], s[2])) for s in spec]

NOM = "nominal, SAR million"
NOM_AR = "بالقيمة الاسمية، مليون ريال"

VW_GDP_GROWTH = (
    "GDP level and nominal growth (YoY, QoQ, CAGR) for each approach. Growth is computed only between periods that both exist. | الناتج المحلي الإجمالي ومعدلات النمو الاسمية",
    cols(("series", "expenditure_gdp, income_gdp, activity_gdp_qtr or activity_gdp_4q_sum", "معرف السلسلة"),
         ("series_name_en", "Series name (English)", "اسم السلسلة بالإنجليزية"), ("series_name_ar", "Series name (Arabic)", "اسم السلسلة بالعربية"),
         "value_basis", "period_type", "date_key", "period_key", "year", "quarter",
         ("gdp_nominal_sar_mn", f"GDP, {NOM}; NULL if missing", f"الناتج المحلي الإجمالي {NOM_AR}"),
         ("prev_period_gdp_nominal_sar_mn", "GDP of the previous year or quarter", "الناتج في الفترة السابقة"),
         ("same_period_prev_year_gdp_nominal_sar_mn", "GDP in the same period one year earlier", "الناتج في نفس الفترة من العام السابق"),
         ("yoy_change_nominal_sar_mn", "Change vs same period last year, SAR million", "التغير السنوي"),
         ("yoy_growth_nominal_pct", "Nominal year-on-year growth %; NULL unless both periods exist", "النمو السنوي الاسمي"),
         ("qoq_growth_nominal_pct", "Nominal quarter-on-quarter growth %, not seasonally adjusted; NULL unless both quarters exist", "النمو الربعي الاسمي غير معدل موسمياً"),
         ("cagr_full_period_nominal_pct", "Nominal CAGR over the full span; annual series with no gaps only", "معدل النمو السنوي المركب الاسمي للفترة كاملة"),
         ("cagr_full_period_span", "Years used for the full-period CAGR", "فترة الحساب"),
         ("cagr_last_5y_nominal_pct", "Nominal CAGR over the last 5 years (6 consecutive years present)", "معدل النمو المركب لآخر 5 سنوات"),
         ("cagr_last_5y_span", "Years used for the 5-year CAGR", "فترة الحساب"),
         ("is_largest_yoy_increase", "Period with the highest YoY growth in the series", "أعلى نمو سنوي"),
         ("is_largest_yoy_decrease", "Period with the lowest YoY growth in the series", "أدنى نمو سنوي"),
         "is_complete_period", "dq_status"),
    """
WITH
s AS (
  SELECT 'expenditure_gdp' AS series, f.date_key, f.obs_value AS v, f.is_complete_period, f.dq_status
  FROM {g}.fact_gdp_expenditure f JOIN {g}.dim_expenditure_item i ON f.expenditure_item_key = i.expenditure_item_key WHERE i.code = 'EX_GDP'
  UNION ALL
  SELECT 'income_gdp', f.date_key, f.obs_value, f.is_complete_period, f.dq_status
  FROM {g}.fact_gdp_income f JOIN {g}.dim_income_factor i ON f.income_factor_key = i.income_factor_key WHERE i.code = 'IN_GDP'
  UNION ALL
  SELECT 'activity_gdp_qtr', f.date_key, f.obs_value, f.is_complete_period, f.dq_status
  FROM {g}.fact_gdp_activity_qtr f JOIN {g}.dim_economic_activity a ON f.economic_activity_key = a.economic_activity_key WHERE a.code = 'EA_GDP'
  UNION ALL
  SELECT 'activity_gdp_4q_sum', d.year * 100, SUM(f.obs_value), BOOL_AND(f.is_complete_period),
         CASE WHEN NOT BOOL_AND(f.is_complete_period) THEN 'INCOMPLETE' WHEN BOOL_AND(f.dq_status = 'PASS') THEN 'PASS' ELSE 'PASS_WITH_WARNINGS' END
  FROM {g}.fact_gdp_activity_qtr f JOIN {g}.dim_economic_activity a ON f.economic_activity_key = a.economic_activity_key
  JOIN {g}.dim_date d ON f.date_key = d.date_key
  WHERE a.code = 'EA_GDP' GROUP BY d.year HAVING COUNT(*) = 4
),
meta AS (
  SELECT * FROM VALUES
    ('expenditure_gdp', 'annual', 1, 'GDP, expenditure approach', 'الناتج المحلي الإجمالي - نهج الإنفاق'),
    ('income_gdp', 'annual', 1, 'GDP, income approach', 'الناتج المحلي الإجمالي - نهج الدخل'),
    ('activity_gdp_qtr', 'quarterly', 4, 'Quarterly GDP, activity approach', 'الناتج المحلي الإجمالي الربعي - نهج النشاط'),
    ('activity_gdp_4q_sum', 'annual', 1, 'GDP, sum of 4 quarters (activity approach)', 'مجموع الأرباع الأربعة للناتج المحلي الإجمالي')
  AS t(series, period_type, periods_per_year, name_en, name_ar)
),
rng AS (
  SELECT 'expenditure_gdp' AS series, MIN(date_key) AS lo, MAX(date_key) AS hi FROM {g}.fact_gdp_expenditure
  UNION ALL SELECT 'income_gdp', MIN(date_key), MAX(date_key) FROM {g}.fact_gdp_income
  UNION ALL SELECT 'activity_gdp_qtr', MIN(date_key), MAX(date_key) FROM {g}.fact_gdp_activity_qtr
  UNION ALL SELECT 'activity_gdp_4q_sum', MIN(date_key) DIV 100 * 100, MAX(date_key) DIV 100 * 100 FROM {g}.fact_gdp_activity_qtr
),
grid AS (
  SELECT m.series, m.period_type, m.periods_per_year, m.name_en, m.name_ar, d.date_key, d.period_key, d.year, d.quarter,
         d.period_seq, d.prev_period_date_key, d.same_period_prev_year_date_key
  FROM meta m JOIN rng r ON m.series = r.series
  JOIN {g}.dim_date d ON d.period_type = m.period_type AND d.date_key BETWEEN r.lo AND r.hi
),
gv AS (SELECT grid.*, s.v, s.is_complete_period, s.dq_status FROM grid LEFT JOIN s ON s.series = grid.series AND s.date_key = grid.date_key),
ends AS (
  SELECT series, MIN(CASE WHEN v IS NOT NULL THEN period_seq END) AS first_seq, MAX(CASE WHEN v IS NOT NULL THEN period_seq END) AS last_seq
  FROM gv GROUP BY series
),
cg AS (
  SELECT e.series, MAX(e.first_seq) AS first_seq, MAX(e.last_seq) AS last_seq, MAX(gv.periods_per_year) AS ppy,
    MAX(CASE WHEN gv.period_seq = e.first_seq THEN gv.v END) AS v_first,
    MAX(CASE WHEN gv.period_seq = e.last_seq THEN gv.v END) AS v_last,
    MAX(CASE WHEN gv.period_seq = e.last_seq - 5 THEN gv.v END) AS v_5y,
    MAX(CASE WHEN gv.period_seq = e.first_seq THEN gv.period_key END) AS first_key,
    MAX(CASE WHEN gv.period_seq = e.last_seq THEN gv.period_key END) AS last_key,
    MAX(CASE WHEN gv.period_seq = e.last_seq - 5 THEN gv.period_key END) AS key_5y,
    COUNT(CASE WHEN gv.period_seq BETWEEN e.first_seq AND e.last_seq THEN 1 END) AS n_all,
    COUNT(CASE WHEN gv.period_seq BETWEEN e.first_seq AND e.last_seq THEN gv.v END) AS n_present,
    COUNT(CASE WHEN gv.period_seq BETWEEN e.last_seq - 5 AND e.last_seq THEN gv.v END) AS n5_present
  FROM gv JOIN ends e ON gv.series = e.series GROUP BY e.series
),
cagr AS (
  SELECT series,
    CASE WHEN ppy = 1 AND n_present = n_all AND last_seq > first_seq AND v_first > 0 AND v_last > 0
         THEN ROUND((POWER(v_last / v_first, 1.0 / (last_seq - first_seq)) - 1) * 100, 4) END AS cagr_full,
    CASE WHEN ppy = 1 AND n_present = n_all AND last_seq > first_seq THEN first_key || '-' || last_key END AS cagr_full_span,
    CASE WHEN ppy = 1 AND n5_present = 6 AND v_5y > 0 AND v_last > 0 THEN ROUND((POWER(v_last / v_5y, 1.0 / 5) - 1) * 100, 4) END AS cagr_5y,
    CASE WHEN ppy = 1 AND n5_present = 6 THEN key_5y || '-' || last_key END AS cagr_5y_span
  FROM cg
),
base AS (
  SELECT gv.*, pp.v AS prev_v, py.v AS py_v, ct.cagr_full, ct.cagr_full_span, ct.cagr_5y, ct.cagr_5y_span,
         ROUND(try_divide(gv.v - py.v, py.v) * 100, 4) AS yoy
  FROM gv
  LEFT JOIN gv pp ON pp.series = gv.series AND pp.date_key = gv.prev_period_date_key
  LEFT JOIN gv py ON py.series = gv.series AND py.date_key = gv.same_period_prev_year_date_key
  LEFT JOIN cagr ct ON ct.series = gv.series
),
final AS (
  SELECT series, name_en AS series_name_en, name_ar AS series_name_ar, '{vb}' AS value_basis, period_type, date_key, period_key, year, quarter,
    v AS gdp_nominal_sar_mn, prev_v AS prev_period_gdp_nominal_sar_mn, py_v AS same_period_prev_year_gdp_nominal_sar_mn,
    v - py_v AS yoy_change_nominal_sar_mn, yoy AS yoy_growth_nominal_pct,
    CASE WHEN period_type = 'quarterly' THEN ROUND(try_divide(v - prev_v, prev_v) * 100, 4) END AS qoq_growth_nominal_pct,
    cagr_full AS cagr_full_period_nominal_pct, cagr_full_span AS cagr_full_period_span,
    cagr_5y AS cagr_last_5y_nominal_pct, cagr_5y_span AS cagr_last_5y_span,
    COALESCE(yoy = MAX(yoy) OVER (PARTITION BY series), false) AS is_largest_yoy_increase,
    COALESCE(yoy = MIN(yoy) OVER (PARTITION BY series), false) AS is_largest_yoy_decrease,
    COALESCE(is_complete_period, false) AS is_complete_period, COALESCE(dq_status, 'MISSING') AS dq_status
  FROM base
)""")

VW_SECTOR_CONTRIBUTION = (
    "Activity shares of GVA / GDP, oil vs non-oil, oil / private / government, sub-sector splits, growth, YTD, seasonality and top-3 concentration. Annual rows are sums of 4 quarters, only when all 4 are present. Filter on one breakdown before summing. | مساهمة الأنشطة الاقتصادية",
    cols(("breakdown", "gdp_component, sector, sub_sector, oil_vs_non_oil or oil_private_government; members of one breakdown never overlap", "نوع التصنيف"),
         ("member_code", "Activity code or group code (OIL, NON_OIL, PRIVATE_NON_OIL, GOVERNMENT)", "رمز النشاط أو المجموعة"),
         ("member_name_en", "Name (English)", "الاسم بالإنجليزية"), ("member_name_ar", "Name (Arabic)", "الاسم بالعربية"),
         ("parent_code", "Parent used for share_of_parent_pct", "البند الأب"), ("sort_order", "Display order", "ترتيب العرض"),
         "value_basis", "period_type", "date_key", "period_key", "year", "quarter",
         ("value_nominal_sar_mn", f"Value, {NOM}; group values are NULL unless every leaf activity is present", f"القيمة {NOM_AR}"),
         ("parent_value_nominal_sar_mn", f"Parent value, {NOM}", "قيمة البند الأب"),
         ("gva_nominal_sar_mn", f"Gross value added, {NOM}", "القيمة المضافة الإجمالية"),
         ("gdp_nominal_sar_mn", f"GDP, {NOM}", "الناتج المحلي الإجمالي"),
         ("share_of_parent_pct", "Share of the parent %", "الحصة من البند الأب"),
         ("share_of_gva_pct", "Share of GVA %", "الحصة من القيمة المضافة"),
         ("share_of_gdp_pct", "Share of GDP %", "الحصة من الناتج"),
         ("yoy_growth_nominal_pct", "Nominal growth vs same period last year %", "النمو السنوي الاسمي"),
         ("qoq_growth_nominal_pct", "Nominal growth vs previous quarter %, not seasonally adjusted", "النمو الربعي الاسمي"),
         ("ytd_value_nominal_sar_mn", "Year-to-date sum (Q1..this quarter); NULL if any quarter is missing", "المجموع منذ بداية العام"),
         ("ytd_prev_year_value_nominal_sar_mn", "Same YTD one year earlier", "المجموع منذ بداية العام السابق"),
         ("ytd_growth_nominal_pct", "Nominal YTD growth vs same period last year %", "نمو المجموع منذ بداية العام"),
         ("share_of_year_pct", "Quarter as % of its full year (seasonality); NULL unless all 4 quarters exist", "حصة الربع من السنة (الموسمية)"),
         ("rank_in_gva", "Rank of the sector by value; only when all sectors are present", "ترتيب القطاع"),
         ("top3_share_of_gva_pct", "Combined GVA share of the 3 largest sectors; only when all sectors are present", "حصة أكبر 3 قطاعات"),
         "is_complete_period", "dq_status"),
    """
WITH
a AS (SELECT * FROM {g}.dim_economic_activity),
q AS (SELECT f.date_key, a.code, f.obs_value AS v FROM {g}.fact_gdp_activity_qtr f JOIN a ON f.economic_activity_key = a.economic_activity_key),
qy AS (SELECT q.date_key, q.code, q.v, d.year FROM q JOIN {g}.dim_date d ON q.date_key = d.date_key),
yr AS (SELECT year * 100 AS date_key, code, SUM(v) AS v FROM qy GROUP BY year, code HAVING COUNT(*) = 4),
vals AS (SELECT date_key, code, v FROM q UNION ALL SELECT date_key, code, v FROM yr),
pq AS (SELECT date_key, COUNT(*) AS n, BOOL_AND(is_complete_period) AS complete, BOOL_AND(dq_status = 'PASS') AS all_pass
       FROM {g}.fact_gdp_activity_qtr GROUP BY date_key),
pstat AS (
  SELECT d.date_key, COALESCE(pq.complete, false) AS is_complete_period,
         CASE WHEN pq.n IS NULL THEN 'MISSING' WHEN NOT pq.complete THEN 'INCOMPLETE' WHEN pq.all_pass THEN 'PASS' ELSE 'PASS_WITH_WARNINGS' END AS dq_status
  FROM {g}.dim_date d LEFT JOIN pq ON d.date_key = pq.date_key WHERE d.period_type = 'quarterly'
  UNION ALL
  SELECT d.year * 100, COUNT(CASE WHEN pq.complete THEN 1 END) = 4,
         CASE WHEN COUNT(CASE WHEN pq.complete THEN 1 END) = 4 THEN CASE WHEN BOOL_AND(pq.all_pass) THEN 'PASS' ELSE 'PASS_WITH_WARNINGS' END
              WHEN COUNT(pq.n) = 0 THEN 'MISSING' ELSE 'INCOMPLETE' END
  FROM {g}.dim_date d LEFT JOIN pq ON d.date_key = pq.date_key WHERE d.period_type = 'quarterly' GROUP BY d.year
),
rng AS (SELECT MIN(date_key) AS lo, MAX(date_key) AS hi FROM {g}.fact_gdp_activity_qtr),
periods AS (
  SELECT d.date_key, d.period_key, d.year, d.quarter, d.period_type, d.prev_period_date_key, d.same_period_prev_year_date_key
  FROM {g}.dim_date d CROSS JOIN rng
  WHERE (d.period_type = 'quarterly' AND d.date_key BETWEEN rng.lo AND rng.hi)
     OR (d.period_type = 'annual' AND d.year BETWEEN rng.lo DIV 100 AND rng.hi DIV 100)
),
members AS (
  SELECT 'gdp_component' AS breakdown, code AS member_code, name_en AS member_name_en, name_ar AS member_name_ar, parent_code, sort_order FROM a WHERE parent_code = 'EA_GDP'
  UNION ALL SELECT 'sector', code, name_en, name_ar, parent_code, sort_order FROM a WHERE parent_code = 'EA_GVA'
  UNION ALL SELECT 'sub_sector', code, name_en, name_ar, parent_code, sort_order FROM a WHERE hierarchy_level = 3
  UNION ALL SELECT * FROM VALUES
    ('oil_vs_non_oil', 'OIL', 'Oil activities', 'الأنشطة النفطية', 'EA_GVA', 1),
    ('oil_vs_non_oil', 'NON_OIL', 'Non-oil activities', 'الأنشطة غير النفطية', 'EA_GVA', 2),
    ('oil_private_government', 'OIL', 'Oil activities', 'الأنشطة النفطية', 'EA_GVA', 1),
    ('oil_private_government', 'PRIVATE_NON_OIL', 'Non-oil private sector', 'القطاع الخاص غير النفطي', 'EA_GVA', 2),
    ('oil_private_government', 'GOVERNMENT', 'Government sector', 'القطاع الحكومي', 'EA_GVA', 3)
    AS t(breakdown, member_code, member_name_en, member_name_ar, parent_code, sort_order)
),
leaf AS (
  SELECT 'oil_vs_non_oil' AS breakdown, upper(oil_group) AS member_code, code FROM a WHERE oil_group IS NOT NULL
  UNION ALL SELECT 'oil_private_government', upper(institutional_sector), code FROM a WHERE institutional_sector IS NOT NULL
),
leaf_n AS (SELECT breakdown, member_code, COUNT(*) AS n_expected FROM leaf GROUP BY breakdown, member_code),
grp AS (SELECT l.breakdown, l.member_code, v.date_key, SUM(v.v) AS v, COUNT(*) AS n
        FROM leaf l JOIN vals v ON v.code = l.code GROUP BY l.breakdown, l.member_code, v.date_key),
mv AS (
  SELECT m.breakdown, m.member_code, m.member_name_en, m.member_name_ar, m.parent_code, m.sort_order,
         p.date_key, p.period_key, p.year, p.quarter, p.period_type, p.prev_period_date_key, p.same_period_prev_year_date_key,
         CASE WHEN m.breakdown IN ('oil_vs_non_oil', 'oil_private_government') THEN CASE WHEN gp.n = ln.n_expected THEN gp.v END ELSE iv.v END AS v
  FROM members m CROSS JOIN periods p
  LEFT JOIN vals iv ON m.breakdown IN ('gdp_component', 'sector', 'sub_sector') AND iv.code = m.member_code AND iv.date_key = p.date_key
  LEFT JOIN grp gp ON gp.breakdown = m.breakdown AND gp.member_code = m.member_code AND gp.date_key = p.date_key
  LEFT JOIN leaf_n ln ON ln.breakdown = m.breakdown AND ln.member_code = m.member_code
),
mv2 AS (
  SELECT mv.*, CASE WHEN period_type = 'quarterly' AND COUNT(v) OVER w = quarter THEN SUM(v) OVER w END AS ytd_v
  FROM mv
  WINDOW w AS (PARTITION BY breakdown, member_code, period_type, year ORDER BY date_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
den AS (SELECT date_key, MAX(CASE WHEN code = 'EA_GVA' THEN v END) AS gva, MAX(CASE WHEN code = 'EA_GDP' THEN v END) AS gdp FROM vals GROUP BY date_key),
x AS (
  SELECT cur.*, par.v AS parent_v, den.gva, den.gdp, pp.v AS prev_v, py.v AS py_v, py.ytd_v AS py_ytd, ann.v AS year_v,
         try_divide(cur.v, den.gva) * 100 AS share_gva
  FROM mv2 cur
  LEFT JOIN vals par ON par.code = cur.parent_code AND par.date_key = cur.date_key
  LEFT JOIN den ON den.date_key = cur.date_key
  LEFT JOIN mv2 pp ON pp.breakdown = cur.breakdown AND pp.member_code = cur.member_code AND pp.date_key = cur.prev_period_date_key
  LEFT JOIN mv2 py ON py.breakdown = cur.breakdown AND py.member_code = cur.member_code AND py.date_key = cur.same_period_prev_year_date_key
  LEFT JOIN mv2 ann ON ann.breakdown = cur.breakdown AND ann.member_code = cur.member_code AND ann.date_key = cur.year * 100 AND cur.period_type = 'quarterly'
),
r AS (
  SELECT x.*,
    CASE WHEN breakdown = 'sector' AND gva IS NOT NULL
          AND COUNT(v) OVER (PARTITION BY breakdown, date_key) = COUNT(*) OVER (PARTITION BY breakdown, date_key)
         THEN RANK() OVER (PARTITION BY breakdown, date_key ORDER BY v DESC) END AS rank_in_gva
  FROM x
),
r2 AS (
  SELECT r.*, CASE WHEN rank_in_gva IS NOT NULL THEN SUM(CASE WHEN rank_in_gva <= 3 THEN share_gva END) OVER (PARTITION BY breakdown, date_key) END AS top3
  FROM r
),
final AS (
  SELECT r2.breakdown, r2.member_code, r2.member_name_en, r2.member_name_ar, r2.parent_code, r2.sort_order, '{vb}' AS value_basis,
    r2.period_type, r2.date_key, r2.period_key, r2.year, r2.quarter,
    r2.v AS value_nominal_sar_mn, r2.parent_v AS parent_value_nominal_sar_mn, r2.gva AS gva_nominal_sar_mn, r2.gdp AS gdp_nominal_sar_mn,
    ROUND(try_divide(r2.v, r2.parent_v) * 100, 4) AS share_of_parent_pct,
    ROUND(r2.share_gva, 4) AS share_of_gva_pct,
    ROUND(try_divide(r2.v, r2.gdp) * 100, 4) AS share_of_gdp_pct,
    ROUND(try_divide(r2.v - r2.py_v, r2.py_v) * 100, 4) AS yoy_growth_nominal_pct,
    CASE WHEN r2.period_type = 'quarterly' THEN ROUND(try_divide(r2.v - r2.prev_v, r2.prev_v) * 100, 4) END AS qoq_growth_nominal_pct,
    r2.ytd_v AS ytd_value_nominal_sar_mn, r2.py_ytd AS ytd_prev_year_value_nominal_sar_mn,
    ROUND(try_divide(r2.ytd_v - r2.py_ytd, r2.py_ytd) * 100, 4) AS ytd_growth_nominal_pct,
    CASE WHEN r2.period_type = 'quarterly' THEN ROUND(try_divide(r2.v, r2.year_v) * 100, 4) END AS share_of_year_pct,
    r2.rank_in_gva, ROUND(r2.top3, 4) AS top3_share_of_gva_pct,
    COALESCE(ps.is_complete_period, false) AS is_complete_period, COALESCE(ps.dq_status, 'MISSING') AS dq_status
  FROM r2 LEFT JOIN pstat ps ON ps.date_key = r2.date_key
)""")

_EXP_ITEMS = [  # (alias, code, column stem, EN, AR)
    ("gdp", "EX_GDP", "gdp", "GDP", "الناتج المحلي الإجمالي"),
    ("c", "EX_GFCE", "final_consumption", "Final consumption expenditure", "الاستهلاك النهائي"),
    ("c_gov", "EX_GFCE_GOV", "government_consumption", "Government final consumption", "الاستهلاك الحكومي"),
    ("c_prv", "EX_GFCE_PRV", "private_consumption", "Private final consumption", "الاستهلاك الخاص"),
    ("gcf", "EX_GCF", "gross_capital_formation", "Gross capital formation", "تكوين رأس المال الإجمالي"),
    ("gfcf", "EX_GFCF", "gfcf", "Gross fixed capital formation", "تكوين رأس المال الثابت"),
    ("gfcf_gov", "EX_GFCF_GOV", "gfcf_government", "GFCF, government", "تكوين رأس المال الثابت الحكومي"),
    ("gfcf_ngov", "EX_GFCF_NGOV", "gfcf_non_government", "GFCF, non-government", "تكوين رأس المال الثابت غير الحكومي"),
    ("gfcf_oil", "EX_GFCF_NGOV_OIL", "gfcf_non_government_oil", "Non-government GFCF, oil", "تكوين رأس المال غير الحكومي - نفطي"),
    ("gfcf_noil", "EX_GFCF_NGOV_NOIL", "gfcf_non_government_non_oil", "Non-government GFCF, non-oil", "تكوين رأس المال غير الحكومي - غير نفطي"),
    ("inv", "EX_INV", "change_in_inventories", "Change in inventories", "التغير في المخزون"),
    ("nx", "EX_NX", "net_exports", "Net exports", "صافي الصادرات"),
    ("x", "EX_X", "exports", "Exports of goods and services", "الصادرات"),
    ("m", "EX_M", "imports", "Imports of goods and services", "الواردات"),
    ("dfd", "EX_DFD", "domestic_final_demand", "Domestic final demand (memo: C + GCF)", "الطلب المحلي النهائي"),
]
_EXP_SHARES = [  # (column, numerator alias, denominator alias, EN, AR)
    ("final_consumption_share_of_gdp_pct", "c", "gdp", "Final consumption, % of GDP", "الاستهلاك النهائي % من الناتج"),
    ("government_consumption_share_of_gdp_pct", "c_gov", "gdp", "Government consumption, % of GDP", "الاستهلاك الحكومي % من الناتج"),
    ("private_consumption_share_of_gdp_pct", "c_prv", "gdp", "Private consumption, % of GDP", "الاستهلاك الخاص % من الناتج"),
    ("government_share_of_consumption_pct", "c_gov", "c", "Government share of final consumption %", "حصة الحكومة من الاستهلاك"),
    ("private_share_of_consumption_pct", "c_prv", "c", "Private share of final consumption %", "حصة القطاع الخاص من الاستهلاك"),
    ("gross_capital_formation_share_of_gdp_pct", "gcf", "gdp", "Gross capital formation (investment), % of GDP", "الاستثمار % من الناتج"),
    ("gfcf_share_of_gdp_pct", "gfcf", "gdp", "GFCF, % of GDP", "تكوين رأس المال الثابت % من الناتج"),
    ("inventories_share_of_gdp_pct", "inv", "gdp", "Change in inventories, % of GDP", "التغير في المخزون % من الناتج"),
    ("net_exports_share_of_gdp_pct", "nx", "gdp", "Net exports, % of GDP", "صافي الصادرات % من الناتج"),
    ("exports_share_of_gdp_pct", "x", "gdp", "Exports, % of GDP", "الصادرات % من الناتج"),
    ("imports_share_of_gdp_pct", "m", "gdp", "Imports, % of GDP", "الواردات % من الناتج"),
    ("domestic_final_demand_share_of_gdp_pct", "dfd", "gdp", "Domestic final demand, % of GDP", "الطلب المحلي % من الناتج"),
    ("gfcf_government_share_of_gfcf_pct", "gfcf_gov", "gfcf", "Government share of GFCF %", "حصة الحكومة من تكوين رأس المال"),
    ("gfcf_non_government_share_of_gfcf_pct", "gfcf_ngov", "gfcf", "Non-government share of GFCF %", "حصة غير الحكومي من تكوين رأس المال"),
    ("gfcf_oil_share_of_non_government_gfcf_pct", "gfcf_oil", "gfcf_ngov", "Oil share of non-government GFCF %", "حصة النفط"),
    ("gfcf_non_oil_share_of_non_government_gfcf_pct", "gfcf_noil", "gfcf_ngov", "Non-oil share of non-government GFCF %", "حصة غير النفط"),
]
_EXP_CONTRIB = [  # (column stem, alias, sign, EN, AR)
    ("final_consumption", "c", 1, "Final consumption", "الاستهلاك النهائي"), ("government_consumption", "c_gov", 1, "Government consumption", "الاستهلاك الحكومي"),
    ("private_consumption", "c_prv", 1, "Private consumption", "الاستهلاك الخاص"), ("gross_capital_formation", "gcf", 1, "Gross capital formation", "تكوين رأس المال"),
    ("gfcf", "gfcf", 1, "Gross fixed capital formation", "تكوين رأس المال الثابت"), ("inventories", "inv", 1, "Change in inventories", "التغير في المخزون"),
    ("net_exports", "nx", 1, "Net exports", "صافي الصادرات"), ("exports", "x", 1, "Exports", "الصادرات"), ("imports", "m", -1, "Imports (an increase lowers GDP growth)", "الواردات"),
]

def _exp_view():
    pivot = ",\n    ".join(f"MAX(CASE WHEN e.code = '{code}' THEN e.v END) AS {a}" for a, code, *_ in _EXP_ITEMS)
    prev = ", ".join(f"p.{a} AS p_{a}" for a, *_ in _EXP_ITEMS)
    ctb = ",\n    ".join(f"ROUND({'-' if s < 0 else ''}try_divide({a} - p_{a}, p_gdp) * 100, 4) AS ct_{a}" for _, a, s, *_ in _EXP_CONTRIB)
    levels = ",\n    ".join(f"{a} AS {stem}_nominal_sar_mn" for a, _, stem, *_ in _EXP_ITEMS)
    shares = ",\n    ".join(f"ROUND(try_divide({n}, {d}) * 100, 4) AS {col}" for col, n, d, *_ in _EXP_SHARES)
    contrib = ",\n    ".join(f"ct_{a} AS contribution_{stem}_pp" for stem, a, *_ in _EXP_CONTRIB)
    detail = ", ".join(f"named_struct('a', abs(ct_{a}), 'n', '{en}')" for a, en in
                       [("c_gov", "Government consumption"), ("c_prv", "Private consumption"), ("gfcf", "Gross fixed capital formation"),
                        ("inv", "Change in inventories"), ("x", "Exports"), ("m", "Imports")])
    body = f"""
WITH
e AS (SELECT f.date_key, i.code, f.obs_value AS v, f.is_complete_period, f.dq_status
      FROM {{g}}.fact_gdp_expenditure f JOIN {{g}}.dim_expenditure_item i ON f.expenditure_item_key = i.expenditure_item_key),
rng AS (SELECT MIN(date_key) AS lo, MAX(date_key) AS hi FROM {{g}}.fact_gdp_expenditure),
w AS (
  SELECT d.date_key, d.period_key, d.year, d.prev_period_date_key,
    {pivot},
    COALESCE(BOOL_AND(e.is_complete_period), false) AS is_complete_period, COALESCE(MAX(e.dq_status), 'MISSING') AS dq_status
  FROM {{g}}.dim_date d CROSS JOIN rng LEFT JOIN e ON e.date_key = d.date_key
  WHERE d.period_type = 'annual' AND d.date_key BETWEEN rng.lo AND rng.hi
  GROUP BY d.date_key, d.period_key, d.year, d.prev_period_date_key
),
k AS (SELECT w.*, {prev} FROM w LEFT JOIN w p ON p.date_key = w.prev_period_date_key),
ct AS (SELECT k.*, ROUND(try_divide(gdp - p_gdp, p_gdp) * 100, 4) AS yoy,
    {ctb}
  FROM k),
final AS (
  SELECT date_key, period_key, year, '{{vb}}' AS value_basis,
    {levels},
    gdp - p_gdp AS gdp_yoy_change_nominal_sar_mn, yoy AS gdp_yoy_growth_nominal_pct, x - m AS trade_balance_nominal_sar_mn,
    {shares},
    {contrib},
    CASE WHEN ct_c IS NOT NULL AND ct_gcf IS NOT NULL AND ct_nx IS NOT NULL THEN
      CASE WHEN abs(ct_c) >= abs(ct_gcf) AND abs(ct_c) >= abs(ct_nx) THEN 'Final consumption'
           WHEN abs(ct_gcf) >= abs(ct_nx) THEN 'Gross capital formation' ELSE 'Net exports' END END AS main_growth_driver,
    CASE WHEN ct_c_gov IS NOT NULL AND ct_c_prv IS NOT NULL AND ct_gfcf IS NOT NULL AND ct_inv IS NOT NULL AND ct_x IS NOT NULL AND ct_m IS NOT NULL
         THEN array_max(array({detail})).n END AS main_growth_driver_detail,
    COALESCE(yoy = MAX(yoy) OVER (), false) AS is_largest_yoy_increase,
    COALESCE(yoy = MIN(yoy) OVER (), false) AS is_largest_yoy_decrease,
    is_complete_period, dq_status
  FROM ct
)"""
    columns = cols("date_key", "period_key", "year", "value_basis",
        *[(f"{stem}_nominal_sar_mn", f"{en}, {NOM}", f"{ar} {NOM_AR}") for _, _, stem, en, ar in _EXP_ITEMS],
        ("gdp_yoy_change_nominal_sar_mn", "GDP change vs previous year, SAR million", "التغير السنوي في الناتج"),
        ("gdp_yoy_growth_nominal_pct", "Nominal GDP growth vs previous year %", "النمو السنوي الاسمي"),
        ("trade_balance_nominal_sar_mn", "Trade balance = exports - imports, SAR million", "الميزان التجاري"),
        *[(col, en, ar) for col, _, _, en, ar in _EXP_SHARES],
        *[(f"contribution_{stem}_pp", f"{en}: contribution to nominal GDP growth, percentage points", f"مساهمة {ar} في النمو") for stem, _, _, en, ar in _EXP_CONTRIB],
        ("main_growth_driver", "Largest absolute contributor among consumption, capital formation, net exports", "المحرك الرئيسي للنمو"),
        ("main_growth_driver_detail", "Largest absolute contributor among government / private consumption, GFCF, inventories, exports, imports", "المحرك التفصيلي للنمو"),
        ("is_largest_yoy_increase", "Year with the highest GDP growth", "أعلى نمو"), ("is_largest_yoy_decrease", "Year with the lowest GDP growth", "أدنى نمو"),
        "is_complete_period", "dq_status")
    return ("Expenditure structure of GDP by year: levels, shares of GDP, GFCF splits, trade balance and contributions to nominal growth. | هيكل الإنفاق على الناتج المحلي الإجمالي", columns, body)

_INC_ITEMS = [  # (alias, code, stem, EN, AR)
    ("gdp", "IN_GDP", "gdp", "GDP", "الناتج المحلي الإجمالي"),
    ("coe", "IN_COE", "compensation_of_employees", "Compensation of employees", "تعويضات المشتغلين"),
    ("nos", "IN_NOS", "net_operating_surplus", "Net operating surplus", "فائض التشغيل الصافي"),
    ("cfc", "IN_CFC", "consumption_of_fixed_capital", "Consumption of fixed capital", "إهلاك رأس المال الثابت"),
    ("ntp", "IN_NTP", "net_taxes_on_production", "Net taxes on production", "صافي الضرائب على الإنتاج"),
]

def _inc_view():
    comps = _INC_ITEMS[1:]
    pivot = ",\n    ".join(f"MAX(CASE WHEN e.code = '{code}' THEN e.v END) AS {a}" for a, code, *_ in _INC_ITEMS)
    prev = ", ".join(f"p.{a} AS p_{a}" for a, *_ in _INC_ITEMS)
    cagr = ",\n    ".join(f"CASE WHEN COUNT({a}) = COUNT(*) AND COUNT(*) > 1 AND MIN_BY({a}, year) > 0 AND MAX_BY({a}, year) > 0 "
                           f"THEN ROUND((POWER(MAX_BY({a}, year) / MIN_BY({a}, year), 1.0 / (MAX(year) - MIN(year))) - 1) * 100, 4) END AS cagr_{a}" for a, *_ in _INC_ITEMS)
    yoy = ",\n    ".join(f"ROUND(try_divide({a} - p_{a}, p_{a}) * 100, 4) AS yoy_{a}" for a, *_ in _INC_ITEMS)
    levels = ",\n    ".join(f"{a} AS {stem}_nominal_sar_mn" for a, _, stem, *_ in _INC_ITEMS)
    shares = ",\n    ".join(f"ROUND(try_divide({a}, gdp) * 100, 4) AS {stem}_share_of_gdp_pct" for a, _, stem, *_ in comps)
    yoys = ",\n    ".join(f"yoy_{a} AS {stem}_yoy_growth_nominal_pct" for a, _, stem, *_ in _INC_ITEMS)
    cagrs = ",\n    ".join(f"cagr_{a} AS {stem}_cagr_full_period_nominal_pct" for a, _, stem, *_ in _INC_ITEMS)
    def fastest(prefix):
        cond = " AND ".join(f"{prefix}{a} IS NOT NULL" for a, *_ in comps)
        arr = ", ".join(f"named_struct('a', {prefix}{a}, 'n', '{en}')" for a, _, _, en, _ in comps)
        return f"CASE WHEN {cond} THEN array_max(array({arr})).n END"
    body = f"""
WITH
e AS (SELECT f.date_key, i.code, f.obs_value AS v, f.is_complete_period, f.dq_status
      FROM {{g}}.fact_gdp_income f JOIN {{g}}.dim_income_factor i ON f.income_factor_key = i.income_factor_key),
rng AS (SELECT MIN(date_key) AS lo, MAX(date_key) AS hi FROM {{g}}.fact_gdp_income),
w AS (
  SELECT d.date_key, d.period_key, d.year, d.prev_period_date_key,
    {pivot},
    COALESCE(BOOL_AND(e.is_complete_period), false) AS is_complete_period, COALESCE(MAX(e.dq_status), 'MISSING') AS dq_status
  FROM {{g}}.dim_date d CROSS JOIN rng LEFT JOIN e ON e.date_key = d.date_key
  WHERE d.period_type = 'annual' AND d.date_key BETWEEN rng.lo AND rng.hi
  GROUP BY d.date_key, d.period_key, d.year, d.prev_period_date_key
),
cg AS (SELECT {cagr} FROM w),
k AS (SELECT w.*, {prev} FROM w LEFT JOIN w p ON p.date_key = w.prev_period_date_key),
y AS (SELECT k.*, cg.*, {yoy} FROM k CROSS JOIN cg),
final AS (
  SELECT date_key, period_key, year, '{{vb}}' AS value_basis,
    {levels},
    {shares},
    ROUND(try_divide(coe, coe + nos) * 100, 4) AS labour_share_of_net_factor_income_pct,
    ROUND(try_divide(nos, coe + nos) * 100, 4) AS profit_share_of_net_factor_income_pct,
    {yoys},
    {fastest('yoy_')} AS fastest_growing_component_yoy,
    {cagrs},
    {fastest('cagr_')} AS fastest_growing_component_full_period,
    is_complete_period, dq_status
  FROM y
)"""
    columns = cols("date_key", "period_key", "year", "value_basis",
        *[(f"{stem}_nominal_sar_mn", f"{en}, {NOM}", f"{ar} {NOM_AR}") for _, _, stem, en, ar in _INC_ITEMS],
        *[(f"{stem}_share_of_gdp_pct", f"{en}, % of GDP", f"{ar} % من الناتج") for _, _, stem, en, ar in comps],
        ("labour_share_of_net_factor_income_pct", "Labour share = compensation / (compensation + net operating surplus) %", "حصة العمل"),
        ("profit_share_of_net_factor_income_pct", "Profit share = net operating surplus / (compensation + net operating surplus) %", "حصة الأرباح"),
        *[(f"{stem}_yoy_growth_nominal_pct", f"{en}: nominal growth vs previous year %", f"نمو {ar} السنوي الاسمي") for _, _, stem, en, ar in _INC_ITEMS],
        ("fastest_growing_component_yoy", "Income component with the highest YoY growth this year", "المكون الأسرع نمواً هذا العام"),
        *[(f"{stem}_cagr_full_period_nominal_pct", f"{en}: nominal CAGR over the full span (no gaps)", f"معدل النمو المركب لـ {ar}") for _, _, stem, en, ar in _INC_ITEMS],
        ("fastest_growing_component_full_period", "Income component with the highest CAGR over the full span", "المكون الأسرع نمواً خلال الفترة"),
        "is_complete_period", "dq_status")
    return ("Income structure of GDP by year: shares of compensation, operating surplus, fixed capital consumption and taxes; labour vs profit share; growth. | هيكل الدخل في الناتج المحلي الإجمالي", columns, body)

VW_RECONCILIATION = (
    "Annual GDP reconciliation: expenditure vs income vs sum of 4 quarters (activity), with differences; quarterly sum only when all 4 quarters exist. | مطابقة تقديرات الناتج المحلي الإجمالي",
    cols("date_key", "period_key", "year", "value_basis",
         ("expenditure_gdp_nominal_sar_mn", f"GDP, expenditure approach, {NOM}", "الناتج - نهج الإنفاق"),
         ("income_gdp_nominal_sar_mn", f"GDP, income approach, {NOM}", "الناتج - نهج الدخل"),
         ("quarters_with_gdp", "Quarters of the year with quarterly GDP present (0-4)", "عدد الأرباع المتوفرة"),
         ("quarterly_sum_gdp_nominal_sar_mn", f"Sum of 4 quarterly GDP values, {NOM}; NULL unless all 4 exist", "مجموع الأرباع"),
         ("diff_expenditure_minus_income_sar_mn", "Expenditure GDP - income GDP, SAR million", "الفرق بين الإنفاق والدخل"),
         ("diff_expenditure_minus_income_pct", "Difference as % of expenditure GDP", "الفرق %"),
         ("diff_expenditure_minus_quarterly_sum_sar_mn", "Expenditure GDP - sum of quarters, SAR million", "الفرق بين السنوي ومجموع الأرباع"),
         ("diff_expenditure_minus_quarterly_sum_pct", "Difference as % of expenditure GDP", "الفرق %"),
         ("tolerance_sar_mn", "Rounding tolerance used for agree / differ", "هامش التقريب"),
         ("expenditure_vs_income_status", "agree, differ or not_comparable", "حالة المطابقة"),
         ("expenditure_vs_quarterly_status", "agree, differ or not_comparable", "حالة المطابقة"),
         ("all_three_agree", "True if all three estimates agree within tolerance; NULL if any is missing", "تطابق التقديرات الثلاثة")),
    """
WITH
ex AS (SELECT f.date_key, f.obs_value AS v FROM {g}.fact_gdp_expenditure f JOIN {g}.dim_expenditure_item i ON f.expenditure_item_key = i.expenditure_item_key WHERE i.code = 'EX_GDP'),
inc AS (SELECT f.date_key, f.obs_value AS v FROM {g}.fact_gdp_income f JOIN {g}.dim_income_factor i ON f.income_factor_key = i.income_factor_key WHERE i.code = 'IN_GDP'),
qs AS (SELECT d.year * 100 AS date_key, COUNT(*) AS q_n, SUM(f.obs_value) AS q_sum
       FROM {g}.fact_gdp_activity_qtr f JOIN {g}.dim_economic_activity a ON f.economic_activity_key = a.economic_activity_key
       JOIN {g}.dim_date d ON f.date_key = d.date_key WHERE a.code = 'EA_GDP' GROUP BY d.year),
b AS (SELECT d.date_key, d.period_key, d.year, ex.v AS e, inc.v AS i, COALESCE(qs.q_n, 0) AS q_n, CASE WHEN qs.q_n = 4 THEN qs.q_sum END AS q
      FROM {g}.dim_date d LEFT JOIN ex ON ex.date_key = d.date_key LEFT JOIN inc ON inc.date_key = d.date_key LEFT JOIN qs ON qs.date_key = d.date_key
      WHERE d.period_type = 'annual'),
final AS (
  SELECT date_key, period_key, year, '{vb}' AS value_basis, e AS expenditure_gdp_nominal_sar_mn, i AS income_gdp_nominal_sar_mn,
    q_n AS quarters_with_gdp, q AS quarterly_sum_gdp_nominal_sar_mn,
    e - i AS diff_expenditure_minus_income_sar_mn, ROUND(try_divide(e - i, e) * 100, 6) AS diff_expenditure_minus_income_pct,
    e - q AS diff_expenditure_minus_quarterly_sum_sar_mn, ROUND(try_divide(e - q, e) * 100, 6) AS diff_expenditure_minus_quarterly_sum_pct,
    {tol} AS tolerance_sar_mn,
    CASE WHEN e IS NULL OR i IS NULL THEN 'not_comparable' WHEN abs(e - i) <= {tol} THEN 'agree' ELSE 'differ' END AS expenditure_vs_income_status,
    CASE WHEN e IS NULL OR q IS NULL THEN 'not_comparable' WHEN abs(e - q) <= {tol} THEN 'agree' ELSE 'differ' END AS expenditure_vs_quarterly_status,
    CASE WHEN e IS NULL OR i IS NULL OR q IS NULL THEN NULL ELSE abs(e - i) <= {tol} AND abs(e - q) <= {tol} AND abs(i - q) <= {tol} END AS all_three_agree
  FROM b
)""")

VW_DATA_QUALITY = (
    "Data quality per dataset, series and period: presence vs expected, dq_status, completeness %, latest period, silver blockers and refresh time. | جودة البيانات لكل سلسلة وفترة",
    cols(("dataset", "Silver dataset", "مجموعة البيانات"), ("gold_table", "Gold fact table", "جدول الحقائق"),
         ("series_code", "Item code", "رمز البند"), ("series_name_en", "Item name (English)", "اسم البند"), ("series_name_ar", "Item name (Arabic)", "اسم البند بالعربية"),
         ("is_aggregate", "Item is a total", "بند تجميعي"), "period_type", "date_key", "period_key", "year", "quarter",
         ("rows_present", "1 if the value exists, else 0", "موجود"), ("rows_expected", "Always 1", "متوقع"),
         ("dq_status", "PASS, PASS_WITH_WARNINGS, INCOMPLETE (value present, period incomplete), MISSING or FAILED_NOT_PUBLISHED", "حالة الجودة"),
         ("period_dq_status", "Quality status of the whole period", "حالة الفترة"),
         ("period_completeness_pct", "Series present in the period / expected %", "اكتمال الفترة"),
         ("series_completeness_pct", "Periods present for the series / expected %", "اكتمال السلسلة"),
         ("series_latest_period", "Latest period with a value for the series", "آخر فترة للسلسلة"),
         ("failed_checks", "Hard silver checks failed in the period", "الفحوصات الفاشلة"), ("warning_checks", "Warning checks failed in the period", "فحوصات التحذير"),
         ("dataset_completeness_pct", "Rows present / expected for the dataset %", "اكتمال المجموعة"),
         ("dataset_latest_period", "Latest published period of the dataset", "آخر فترة متاحة"),
         ("dataset_complete", "Silver marked the feed complete", "اكتمال البيانات"), ("dataset_gold_ready", "Silver marked the dataset gold ready", "الجاهزية"),
         ("dataset_blockers", "Silver checks blocking gold", "العوائق"), ("source_container", "Container silver read", "الحاوية المصدر"),
         ("silver_run_utc", "Silver run used (UTC)", "وقت تشغيل الطبقة الفضية"), ("last_refreshed_utc", "Gold refresh time (UTC)", "وقت آخر تحديث")),
    """
WITH
series AS (
  SELECT 'gdp_expenditure' AS dataset, 'fact_gdp_expenditure' AS gold_table, expenditure_item_key AS item_key, code, name_en, name_ar, is_aggregate FROM {g}.dim_expenditure_item
  UNION ALL SELECT 'gdp_income', 'fact_gdp_income', income_factor_key, code, name_en, name_ar, is_aggregate FROM {g}.dim_income_factor
  UNION ALL SELECT 'gdp_isic_qtr', 'fact_gdp_activity_qtr', economic_activity_key, code, name_en, name_ar, is_aggregate FROM {g}.dim_economic_activity
),
f AS (
  SELECT 'gdp_expenditure' AS dataset, date_key, expenditure_item_key AS item_key FROM {g}.fact_gdp_expenditure
  UNION ALL SELECT 'gdp_income', date_key, income_factor_key FROM {g}.fact_gdp_income
  UNION ALL SELECT 'gdp_isic_qtr', date_key, economic_activity_key FROM {g}.fact_gdp_activity_qtr
),
grid AS (
  SELECT s.*, q.date_key, q.period_key, q.dq_status AS period_dq_status, q.completeness_pct AS period_completeness_pct,
         q.failed_checks, q.warning_checks, q.is_published, (f.item_key IS NOT NULL) AS is_present
  FROM series s JOIN {g}.dq_period_status q ON q.dataset = s.dataset
  LEFT JOIN f ON f.dataset = s.dataset AND f.date_key = q.date_key AND f.item_key = s.item_key
),
final AS (
  SELECT x.dataset, x.gold_table, x.code AS series_code, x.name_en AS series_name_en, x.name_ar AS series_name_ar, x.is_aggregate,
    d.period_type, x.date_key, x.period_key, d.year, d.quarter,
    CASE WHEN x.is_present THEN 1 ELSE 0 END AS rows_present, 1 AS rows_expected,
    CASE WHEN NOT x.is_published THEN 'FAILED_NOT_PUBLISHED' WHEN x.is_present THEN x.period_dq_status ELSE 'MISSING' END AS dq_status,
    x.period_dq_status, x.period_completeness_pct,
    ROUND(100.0 * SUM(CASE WHEN x.is_present THEN 1 ELSE 0 END) OVER (PARTITION BY x.dataset, x.code) / COUNT(*) OVER (PARTITION BY x.dataset, x.code), 2) AS series_completeness_pct,
    MAX(CASE WHEN x.is_present THEN x.period_key END) OVER (PARTITION BY x.dataset, x.code) AS series_latest_period,
    x.failed_checks, x.warning_checks,
    ds.completeness_pct AS dataset_completeness_pct, ds.latest_period AS dataset_latest_period,
    ds.silver_complete AS dataset_complete, ds.silver_gold_ready AS dataset_gold_ready, ds.silver_blockers AS dataset_blockers,
    ds.source_container, ds.silver_run_utc, ds.last_refreshed_utc
  FROM grid x JOIN {g}.dim_date d ON x.date_key = d.date_key JOIN {g}.dq_dataset_status ds ON ds.dataset = x.dataset
)""")

def view_specs():
    return {"vw_gdp_growth": VW_GDP_GROWTH, "vw_sector_contribution": VW_SECTOR_CONTRIBUTION, "vw_expenditure_shares": _exp_view(),
            "vw_income_shares": _inc_view(), "vw_reconciliation": VW_RECONCILIATION, "vw_data_quality": VW_DATA_QUALITY}

def create_views(p):
    g = f"{p['catalog']}.{p['gold_schema']}"
    for name, (comment, columns, body) in view_specs().items():
        col_list = ",\n  ".join(f"{n} COMMENT '{esc(cm)}'" for n, cm in columns)
        query = body.format(g=g, vb=VALUE_BASIS, tol=TOLERANCE_SAR_MN) + "\nSELECT " + ", ".join(n for n, _ in columns) + " FROM final"
        spark.sql(f"CREATE OR REPLACE VIEW {g}.{name} (\n  {col_list}\n) COMMENT '{esc(comment)}' AS {query}")
        spark.sql(f"ALTER VIEW {g}.{name} SET TAGS ({tag_sql()})")

# COMMAND ----------

# DBTITLE 1,Tests
# Tests: row counts, key uniqueness, NULL / orphan keys, view logic, metadata. SQL tests return a violation count (0 = PASS).
# Value tests recompute results from silver in Python (a different path from the gold SQL).
def sql_tests(p):
    g, s, cat, gs, tol = f"{p['catalog']}.{p['gold_schema']}", f"{p['catalog']}.{p['silver_schema']}", p["catalog"], p["gold_schema"], SHARE_TOLERANCE_PP
    t = []
    for fact, spec in FACTS.items():
        k, dim, ds, (lo, hi) = spec["key"], spec["dim"], spec["silver"], spec["expected_range"]
        excluded = f"SELECT period_key FROM {g}.dq_period_status WHERE dataset = '{ds}' AND NOT is_published"
        t += [
            (f"{dim}: row count = silver", f"SELECT abs((SELECT count(*) FROM {g}.{dim}) - (SELECT count(*) FROM {s}.{dim}))"),
            (f"{dim}: key and code unique, not NULL", f"SELECT count(*) - count(DISTINCT {k}) + count_if({k} IS NULL) + count(*) - count(DISTINCT code) FROM {g}.{dim}"),
            (f"{fact}: row count = silver rows in published periods", f"SELECT abs((SELECT count(*) FROM {g}.{fact}) - (SELECT count(*) FROM {s}.{ds} WHERE period_key NOT IN ({excluded})))"),
            (f"{fact}: sum(obs_value) = silver", f"SELECT abs((SELECT sum(obs_value) FROM {g}.{fact}) - (SELECT sum(obs_value) FROM {s}.{ds} WHERE period_key NOT IN ({excluded})))"),
            (f"{fact}: no NULL keys", f"SELECT count_if(date_key IS NULL OR {k} IS NULL) FROM {g}.{fact}"),
            (f"{fact}: (date_key, {k}) unique", f"SELECT count(*) - count(DISTINCT date_key, {k}) FROM {g}.{fact}"),
            (f"{fact}: every date_key exists in dim_date", f"SELECT count(*) FROM {g}.{fact} f LEFT ANTI JOIN {g}.dim_date d ON f.date_key = d.date_key"),
            (f"{fact}: every {k} exists in {dim}", f"SELECT count(*) FROM {g}.{fact} f LEFT ANTI JOIN {g}.{dim} d ON f.{k} = d.{k}"),
            (f"{fact}: period range {lo}..{hi}", f"SELECT CASE WHEN min(d.period_key) = '{lo}' AND max(d.period_key) = '{hi}' THEN 0 ELSE 1 END FROM {g}.{fact} f JOIN {g}.dim_date d ON f.date_key = d.date_key"),
            (f"{fact}: all rows are {spec['grain']}", f"SELECT count(*) FROM {g}.{fact} f JOIN {g}.dim_date d ON f.date_key = d.date_key WHERE d.period_type <> '{spec['grain']}'"),
            (f"{fact}: is_complete_period matches members present",
             f"SELECT count(*) FROM (SELECT date_key, count(*) AS n, bool_and(is_complete_period) AS a, bool_or(is_complete_period) AS o FROM {g}.{fact} GROUP BY date_key) "
             f"WHERE a <> o OR a <> (n = (SELECT count(*) FROM {g}.{dim}))"),
            (f"{fact}: only publishable dq_status values", f"SELECT count_if(dq_status NOT IN ('PASS', 'PASS_WITH_WARNINGS', 'INCOMPLETE')) FROM {g}.{fact}"),
        ]
    fv = (f"SELECT a.code, f.date_key FROM {g}.fact_gdp_activity_qtr f JOIN {g}.dim_economic_activity a ON f.economic_activity_key = a.economic_activity_key")
    t += [
        ("dim_date: date_key unique, keys not NULL", f"SELECT count(*) - count(DISTINCT date_key) + count_if(date_key IS NULL OR period_key IS NULL) FROM {g}.dim_date"),
        ("dq_period_status: (dataset, date_key) unique", f"SELECT count(*) - count(DISTINCT dataset, date_key) FROM {g}.dq_period_status"),
        ("vw_expenditure_shares: C + GCF + NX = 100% of GDP", f"SELECT count_if(abs(final_consumption_share_of_gdp_pct + gross_capital_formation_share_of_gdp_pct + net_exports_share_of_gdp_pct - 100) > {tol}) FROM {g}.vw_expenditure_shares"),
        ("vw_expenditure_shares: government + private = 100% of consumption", f"SELECT count_if(abs(government_share_of_consumption_pct + private_share_of_consumption_pct - 100) > {tol}) FROM {g}.vw_expenditure_shares"),
        ("vw_expenditure_shares: GFCF government + non-government = 100%", f"SELECT count_if(abs(gfcf_government_share_of_gfcf_pct + gfcf_non_government_share_of_gfcf_pct - 100) > {tol}) FROM {g}.vw_expenditure_shares"),
        ("vw_expenditure_shares: oil + non-oil = 100% of non-government GFCF", f"SELECT count_if(abs(gfcf_oil_share_of_non_government_gfcf_pct + gfcf_non_oil_share_of_non_government_gfcf_pct - 100) > {tol}) FROM {g}.vw_expenditure_shares"),
        ("vw_expenditure_shares: no positive component share above 100% of parent", f"SELECT count_if(government_share_of_consumption_pct > 100 + {tol} OR private_share_of_consumption_pct > 100 + {tol} OR gfcf_government_share_of_gfcf_pct > 100 + {tol} OR gfcf_non_government_share_of_gfcf_pct > 100 + {tol}) FROM {g}.vw_expenditure_shares"),
        ("vw_expenditure_shares: contributions of C + GCF + NX = GDP growth", f"SELECT count_if(abs(contribution_final_consumption_pp + contribution_gross_capital_formation_pp + contribution_net_exports_pp - gdp_yoy_growth_nominal_pct) > {tol}) FROM {g}.vw_expenditure_shares"),
        ("vw_expenditure_shares: shares present for every year", f"SELECT count_if(final_consumption_share_of_gdp_pct IS NULL) FROM {g}.vw_expenditure_shares"),
        ("vw_income_shares: components = 100% of GDP", f"SELECT count_if(abs(compensation_of_employees_share_of_gdp_pct + net_operating_surplus_share_of_gdp_pct + consumption_of_fixed_capital_share_of_gdp_pct + net_taxes_on_production_share_of_gdp_pct - 100) > {tol}) FROM {g}.vw_income_shares"),
        ("vw_income_shares: labour + profit share = 100%", f"SELECT count_if(abs(labour_share_of_net_factor_income_pct + profit_share_of_net_factor_income_pct - 100) > {tol}) FROM {g}.vw_income_shares"),
        ("vw_income_shares: shares present for every year", f"SELECT count_if(compensation_of_employees_share_of_gdp_pct IS NULL) FROM {g}.vw_income_shares"),
        ("vw_sector_contribution: shares within 0..100% of parent", f"SELECT count_if(share_of_parent_pct < 0 OR share_of_parent_pct > 100 + {tol}) FROM {g}.vw_sector_contribution"),
        ("vw_sector_contribution: complete breakdowns sum to 100% of parent",
         f"SELECT count(*) FROM (SELECT breakdown, parent_code, date_key, sum(share_of_parent_pct) AS s, count(share_of_parent_pct) AS n, count(*) AS total "
         f"FROM {g}.vw_sector_contribution GROUP BY breakdown, parent_code, date_key) WHERE n = total AND abs(s - 100) > {tol}"),
        ("vw_sector_contribution: at least one complete sector breakdown is tested",
         f"SELECT CASE WHEN count(*) > 0 THEN 0 ELSE 1 END FROM (SELECT date_key FROM {g}.vw_sector_contribution WHERE breakdown = 'sector' GROUP BY date_key HAVING count(share_of_parent_pct) = count(*))"),
        ("vw_sector_contribution: annual values only from 4 quarters",
         f"WITH n AS (SELECT a.code, d.year, count(*) AS n FROM ({fv}) a JOIN {g}.dim_date d ON a.date_key = d.date_key GROUP BY a.code, d.year) "
         f"SELECT count(*) FROM {g}.vw_sector_contribution v LEFT JOIN n ON n.code = v.member_code AND n.year = v.year "
         f"WHERE v.period_type = 'annual' AND v.breakdown IN ('gdp_component', 'sector', 'sub_sector') AND v.value_nominal_sar_mn IS NOT NULL AND coalesce(n.n, 0) <> 4"),
        ("vw_sector_contribution: YoY only when the same quarter last year exists",
         f"SELECT count(*) FROM {g}.vw_sector_contribution v JOIN {g}.dim_date d ON v.date_key = d.date_key LEFT JOIN ({fv}) x ON x.code = v.member_code AND x.date_key = d.same_period_prev_year_date_key "
         f"WHERE v.period_type = 'quarterly' AND v.breakdown IN ('gdp_component', 'sector', 'sub_sector') AND v.yoy_growth_nominal_pct IS NOT NULL AND x.code IS NULL"),
        ("vw_sector_contribution: QoQ only when the previous quarter exists",
         f"SELECT count(*) FROM {g}.vw_sector_contribution v JOIN {g}.dim_date d ON v.date_key = d.date_key LEFT JOIN ({fv}) x ON x.code = v.member_code AND x.date_key = d.prev_period_date_key "
         f"WHERE v.breakdown IN ('gdp_component', 'sector', 'sub_sector') AND v.qoq_growth_nominal_pct IS NOT NULL AND x.code IS NULL"),
        ("vw_gdp_growth: quarterly YoY / QoQ only when the comparison quarter exists",
         f"SELECT count(*) FROM {g}.vw_gdp_growth v JOIN {g}.dim_date d ON v.date_key = d.date_key "
         f"LEFT JOIN ({fv}) y ON y.code = 'EA_GDP' AND y.date_key = d.same_period_prev_year_date_key LEFT JOIN ({fv}) q ON q.code = 'EA_GDP' AND q.date_key = d.prev_period_date_key "
         f"WHERE v.series = 'activity_gdp_qtr' AND ((v.yoy_growth_nominal_pct IS NOT NULL AND y.code IS NULL) OR (v.qoq_growth_nominal_pct IS NOT NULL AND q.code IS NULL))"),
        ("vw_gdp_growth: quarterly YoY present whenever both quarters exist",
         f"SELECT count(*) FROM {g}.vw_gdp_growth v JOIN {g}.dim_date d ON v.date_key = d.date_key JOIN ({fv}) y ON y.code = 'EA_GDP' AND y.date_key = d.same_period_prev_year_date_key "
         f"WHERE v.series = 'activity_gdp_qtr' AND v.gdp_nominal_sar_mn IS NOT NULL AND v.yoy_growth_nominal_pct IS NULL"),
        ("vw_reconciliation: quarterly sum only when 4 quarters exist", f"SELECT count_if(quarterly_sum_gdp_nominal_sar_mn IS NOT NULL AND quarters_with_gdp <> 4) FROM {g}.vw_reconciliation"),
        ("vw_reconciliation: comparable years agree within tolerance", f"SELECT count_if(expenditure_vs_income_status = 'differ' OR expenditure_vs_quarterly_status = 'differ') FROM {g}.vw_reconciliation"),
        ("no column is labelled 'real'", f"SELECT count(*) FROM {cat}.information_schema.columns WHERE table_schema = '{gs}' AND lower(column_name) RLIKE '(^|_)real(_|$)'"),
        ("every gold column has an EN | AR comment", f"SELECT count(*) FROM {cat}.information_schema.columns WHERE table_schema = '{gs}' AND (comment IS NULL OR comment NOT LIKE '%|%')"),
        ("every gold table and view has domain + layer tags",
         f"SELECT count(*) FROM {cat}.information_schema.tables t LEFT JOIN (SELECT table_name, count_if((tag_name = 'domain' AND tag_value = 'national_accounts') OR (tag_name = 'layer' AND tag_value = 'gold')) AS n "
         f"FROM {cat}.information_schema.table_tags WHERE schema_name = '{gs}' GROUP BY table_name) x ON x.table_name = t.table_name WHERE t.table_schema = '{gs}' AND coalesce(x.n, 0) < 2"),
    ]
    for v in ["vw_gdp_growth", "vw_sector_contribution", "vw_expenditure_shares", "vw_income_shares", "vw_reconciliation"]:
        t.append((f"{v}: value_basis is nominal on every row", f"SELECT count_if(value_basis NOT LIKE 'nominal%') FROM {g}.{v}"))
    return t

def value_tests(p):
    """Recompute key results directly from silver in Python and compare with the gold views."""
    g, out = f"{p['catalog']}.{p['gold_schema']}", []
    exp = {int(r["year"]): r["obs_value"] for r in silver_t(p, "gdp_expenditure").filter("item_code = 'EX_GDP'").collect()}
    yrs = sorted(exp)
    view = {r["year"]: r for r in spark.table(f"{g}.vw_gdp_growth").filter("series = 'expenditure_gdp'").collect()}
    yoy_err = max(abs(view[y]["yoy_growth_nominal_pct"] - (exp[y] / exp[y - 1] - 1) * 100) for y in yrs[1:])
    out.append(("vw_gdp_growth: expenditure YoY = Python recompute from silver", yoy_err, yoy_err < 1e-3))
    cagr = ((exp[yrs[-1]] / exp[yrs[0]]) ** (1 / (yrs[-1] - yrs[0])) - 1) * 100
    cagr_err = abs(view[yrs[-1]]["cagr_full_period_nominal_pct"] - cagr)
    out.append((f"vw_gdp_growth: expenditure CAGR {yrs[0]}-{yrs[-1]} = Python recompute ({cagr:.4f}%)", cagr_err, cagr_err < 1e-3))
    cagr5 = ((exp[yrs[-1]] / exp[yrs[-1] - 5]) ** (1 / 5) - 1) * 100
    cagr5_err = abs(view[yrs[-1]]["cagr_last_5y_nominal_pct"] - cagr5)
    out.append((f"vw_gdp_growth: expenditure 5-year CAGR = Python recompute ({cagr5:.4f}%)", cagr5_err, cagr5_err < 1e-3))
    q = {}
    for r in silver_t(p, "gdp_isic_qtr").filter("item_code = 'EA_GDP'").collect():
        q.setdefault(r["year"], []).append(r["obs_value"])
    expected = {y: sum(v) for y, v in q.items() if len(v) == 4}
    rec = {r["year"]: r["quarterly_sum_gdp_nominal_sar_mn"] for r in spark.table(f"{g}.vw_reconciliation").collect()}
    bad = [y for y in rec if rec[y] != expected.get(y)]
    out.append((f"vw_reconciliation: quarterly sums = Python recompute ({len(expected)} full years)", len(bad), not bad))
    s = {}
    for r in silver_t(p, "gdp_isic_qtr").filter("item_code IN ('EA_MIN', 'EA_GVA')").collect():
        s.setdefault(r["period_key"], {})[r["item_code"]] = r["obs_value"]
    want = {k: v["EA_MIN"] / v["EA_GVA"] * 100 for k, v in s.items() if len(v) == 2}
    got = {r["period_key"]: r["share_of_gva_pct"] for r in spark.table(f"{g}.vw_sector_contribution")
           .filter("breakdown = 'sector' AND member_code = 'EA_MIN' AND period_type = 'quarterly' AND share_of_gva_pct IS NOT NULL").collect()}
    err = max([abs(got.get(k, -1e9) - v) for k, v in want.items()] + [0.0]) if set(got) == set(want) else float("inf")
    out.append((f"vw_sector_contribution: mining share of GVA = Python recompute ({len(want)} quarters)", err, err < 1e-3))
    return out

def grant_tests(p):
    pr = p["powerbi_principal"]
    if not pr:
        return [("Power BI principal has read-only access to gold", "SKIPPED: powerbi_principal widget is empty", None)]
    rows = spark.sql(f"SHOW GRANTS `{pr}` ON SCHEMA {p['catalog']}.{p['gold_schema']}").collect()
    actions = {r["ActionType"] for r in rows}
    extra = sorted(actions - {"USE SCHEMA", "SELECT"})
    return [("Power BI principal has read-only access to gold", f"actions={sorted(actions)} extra={extra}", {"USE SCHEMA", "SELECT"} <= actions and not extra)]

def run_tests(p):
    results = []
    for name, q in sql_tests(p):
        v = spark.sql(q).first()[0]
        v = 0 if v is None else v
        results.append((name, "PASS" if v == 0 else "FAIL", str(v)))
    for name, detail, ok in value_tests(p) + grant_tests(p):
        results.append((name, "SKIPPED" if ok is None else ("PASS" if ok else "FAIL"), str(detail)))
    return spark.createDataFrame(results, "test string, status string, detail string")

# COMMAND ----------

# DBTITLE 1,Orchestrator
# Single entry point that rebuilds gold and runs the tests (the Power BI grant is a separate cell further down)
def run(p):
    setup_schema(p)
    runs_df, run_ids = latest_silver_runs(p)
    overwrite(p, "dq_period_status", build_period_status(p, run_ids))
    for spec in FACTS.values():
        overwrite(p, spec["dim"], build_item_dim(p, spec))
    overwrite(p, "dim_date", build_dim_date(p))
    for fact, spec in FACTS.items():
        overwrite(p, fact, build_fact(p, spec))
    overwrite(p, "dq_dataset_status", build_dataset_status(p, runs_df))
    create_views(p)
    return run_tests(p)

# COMMAND ----------

# DBTITLE 1,Run the gold build and tests
# Execute: rebuild gold and run all tests; fail the run if any test fails
params = get_params()
test_results = run(params)
display(test_results.orderBy(F.expr("CASE status WHEN 'FAIL' THEN 0 WHEN 'SKIPPED' THEN 1 ELSE 2 END"), "test"))
display(spark.table(gold(params, "dq_dataset_status")))
n_failed = test_results.filter("status = 'FAIL'").count()
if n_failed:
    raise AssertionError(f"{n_failed} gold test(s) failed")

# COMMAND ----------

# DBTITLE 1,Grant read-only access to Power BI (run manually)
# Grants read-only access to the Power BI principal. Run this cell only after filling the powerbi_principal widget.
# USE CATALOG only allows navigation (no data access); USE SCHEMA + SELECT are granted on gastatksa.gold only. Nothing is granted on silver.
def grant_powerbi(p):
    pr = p["powerbi_principal"]
    if not pr:
        return "Grant skipped: set the powerbi_principal widget to the Power BI group name or service principal application id."
    spark.sql(f"GRANT USE CATALOG ON CATALOG `{p['catalog']}` TO `{pr}`")
    spark.sql(f"GRANT USE SCHEMA, SELECT ON SCHEMA `{p['catalog']}`.`{p['gold_schema']}` TO `{pr}`")
    return f"Granted USE CATALOG, USE SCHEMA, SELECT (read only) to {pr}"

grant_params = get_params()
print(grant_powerbi(grant_params))
display(spark.createDataFrame(grant_tests(grant_params), "test string, detail string, passed boolean"))

# COMMAND ----------

# DBTITLE 1,Data dictionary (generated from Unity Catalog)
# One-page data dictionary of every gold table, view and column, read live from Unity Catalog metadata (EN | AR comments)
dict_params = get_params()
display(spark.sql(f"""
  SELECT t.table_name, t.table_type, t.comment AS table_comment, c.ordinal_position, c.column_name, c.full_data_type AS data_type,
         c.is_nullable, c.comment AS column_comment
  FROM {dict_params['catalog']}.information_schema.tables t
  JOIN {dict_params['catalog']}.information_schema.columns c
    ON c.table_schema = t.table_schema AND c.table_name = t.table_name
  WHERE t.table_schema = '{dict_params['gold_schema']}' AND c.table_schema = '{dict_params['gold_schema']}'
  ORDER BY CASE WHEN t.table_name LIKE 'dim%' THEN 1 WHEN t.table_name LIKE 'fact%' THEN 2 WHEN t.table_name LIKE 'vw%' THEN 3 ELSE 4 END,
           t.table_name, c.ordinal_position"""))

# COMMAND ----------

# DBTITLE 1,Power BI connection, relationships and model notes
# MAGIC %md
# MAGIC ## Power BI: connect, model, use
# MAGIC
# MAGIC **Connection (Azure Databricks connector)**
# MAGIC * Server hostname: `adb-7405606379760257.17.azuredatabricks.net`
# MAGIC * HTTP path: `/sql/1.0/warehouses/<warehouse-id>`. No SQL warehouse exists in this workspace yet. Create a small serverless SQL warehouse, then copy its HTTP path from *Connection details*.
# MAGIC * Power BI Desktop: Get data > Azure Databricks > hostname + HTTP path > **Import** mode (the data is only a few thousand rows) > sign in with Microsoft Entra ID (or the service principal for the Power BI service) > catalog `gastatksa`, schema `gold`.
# MAGIC * The Power BI identity needs only `USE CATALOG` on `gastatksa` and `USE SCHEMA` + `SELECT` on `gastatksa.gold` (see the grant cell).
# MAGIC
# MAGIC **Relationships (all many-to-one, single direction, dimension to fact)**
# MAGIC
# MAGIC | From (many) | To (one) |
# MAGIC | --- | --- |
# MAGIC | fact_gdp_expenditure[date_key] | dim_date[date_key] |
# MAGIC | fact_gdp_income[date_key] | dim_date[date_key] |
# MAGIC | fact_gdp_activity_qtr[date_key] | dim_date[date_key] |
# MAGIC | fact_gdp_expenditure[expenditure_item_key] | dim_expenditure_item[expenditure_item_key] |
# MAGIC | fact_gdp_income[income_factor_key] | dim_income_factor[income_factor_key] |
# MAGIC | fact_gdp_activity_qtr[economic_activity_key] | dim_economic_activity[economic_activity_key] |
# MAGIC | vw_* [date_key] (optional) | dim_date[date_key] |
# MAGIC
# MAGIC **Model notes**
# MAGIC * `dim_date` holds annual rows (date_key YYYY00) and quarterly rows (YYYYQQ). The annual facts join the annual rows and the quarterly fact joins the quarterly rows, so slicing by `year` works across all facts. Never sum across `period_type`.
# MAGIC * Never sum a fact over all items: filter `is_aggregate = false` (or pick one level) so totals are not double counted with their components. The views already do this.
# MAGIC * Every value is **nominal (current prices, SAR million)**. No growth is real growth.
# MAGIC * Sort `dim_date[period_label]` by `period_seq`, and sort dimension names by `sort_order`.
# MAGIC * `vw_sector_contribution`: filter on one `breakdown` before summing or ranking.
# MAGIC * Trust: show `dq_status`, `is_complete_period` and `vw_data_quality` next to any quarterly visual. The quarterly activity feed is incomplete (1,000 of 1,254 rows).