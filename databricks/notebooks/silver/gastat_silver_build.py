# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # GASTAT Silver Layer Build
# MAGIC
# MAGIC Builds `gastatksa.silver` from the ADF-landed GASTAT OData JSON (gdp_expenditure, gdp_income, gdp_isic_qtr). It applies an explicit schema contract, cleans and conforms labels to coded dimensions, runs hard quality gates (bad rows go to `_quarantine`, every result is logged to `_dq_audit`), records completeness in `_dataset_status`, and loads the data with an idempotent MERGE over a lookback window. Rows are never deleted. Rerun the whole notebook; all state comes from the widgets and the contract cell.

# COMMAND ----------

# DBTITLE 1,Parameters and data contract
# Parameters (widgets) and the data contract. Every input of the run is defined in this cell.
import json, uuid, datetime as dt
from functools import reduce
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType

spark.conf.set("spark.sql.session.timeZone", "UTC")

dbutils.widgets.text("catalog", "gastatksa", "Catalog")
dbutils.widgets.text("schema", "silver", "Silver schema")
dbutils.widgets.text("source_root", "", "Source root override (optional)")
dbutils.widgets.dropdown("allow_raw_fallback", "true", ["true", "false"], "Fall back to raw/gastat if bronze is empty")
dbutils.widgets.text("lookback_quarters", "4", "Lookback (quarters)")
dbutils.widgets.text("lookback_years", "2", "Lookback (years)")
dbutils.widgets.dropdown("full_refresh", "false", ["true", "false"], "Full refresh (apply revisions to all periods)")

ACCOUNT = "stgastsaudi12"
BRONZE_ROOT = f"abfss://bronze@{ACCOUNT}.dfs.core.windows.net/"
RAW_ROOT = f"abfss://raw@{ACCOUNT}.dfs.core.windows.net/gastat/"
SILVER_LOCATION = f"abfss://silver@{ACCOUNT}.dfs.core.windows.net/"
UNIT, PRICE_BASIS, TOLERANCE = "SAR million", "current prices", 1
DATASETS = ["gdp_expenditure", "gdp_income", "gdp_isic_qtr"]
KEY_COLS = ["indicator_code", "item_code", "period_key"]

# Source contract per dataset (field names exactly as delivered by the GASTAT OData API)
CONTRACT = {
    "gdp_expenditure": {
        "indicator_code": "DPV_NA_EXP_EFNA0101", "grain": "annual", "dim": "dim_expenditure_item",
        "label_en": "EXPENDITURE_ITEMS_ENGL", "label_ar": "EXPENDITURE_ITEMS_ARAB", "value": "OBSVALUE_OBSV",
        "fields": ["EXPENDITURE_ITEMS_ARAB", "EXPENDITURE_ITEMS_ENGL", "INDICATOR_NAME_ARAB", "INDICATOR_NAME_ENGL", "OBSVALUE_OBSV", "YEAR_QUARTER_TIME", "YEAR_TIME"],
        "indicator_name_en": "GDP at current prices by expenditure approach",
        "indicator_name_ar": "الناتج المحلي الإجمالي بالأسعار الجارية حسب الإنفاق"},
    "gdp_income": {
        "indicator_code": "DPV_NA20_EFNA0401", "grain": "annual", "dim": "dim_income_factor",
        "label_en": "FACTOR_INCOME_ENGL", "label_ar": "FACTOR_INCOME_ARAB", "value": "OBS_VALUE_OBSV",
        "fields": ["FACTOR_INCOME_ARAB", "FACTOR_INCOME_ENGL", "OBS_VALUE_OBSV", "YEAR_TIME"],
        "indicator_name_en": "GDP at current prices by income approach",
        "indicator_name_ar": "الناتج المحلي الإجمالي بالأسعار الجارية حسب الدخل"},
    "gdp_isic_qtr": {
        "indicator_code": "DPV_NA_ISIC_EFNA0101", "grain": "quarterly", "dim": "dim_economic_activity",
        "label_en": "ECONOMIC_ACTIVITY_ENGL", "label_ar": "ECONOMIC_ACTIVITY_ARAB", "value": "OBSVALUE_OBSV",
        "fields": ["ECONOMIC_ACTIVITY_ARAB", "ECONOMIC_ACTIVITY_ENGL", "FREQUENCY_TYPE_TIME", "INDICATOR_NAME_ARAB", "INDICATOR_NAME_ENGL", "OBSVALUE_OBSV", "QUARTER_TIME", "YEAR_TIME"],
        "indicator_name_en": "Quarterly GDP at current prices by economic activity",
        "indicator_name_ar": "الناتج المحلي الإجمالي الربعي بالأسعار الجارية حسب النشاط الاقتصادي"},
}

# Reference data: (item_code, label_en, label_ar, parent_code, hierarchy_level, is_aggregate, is_memo, sign_in_parent)
# Labels are matched after cleaning (trim, whitespace collapse, leading apostrophe removed, 'sevices' fixed), case-insensitive.
DIMS = {
    "dim_economic_activity": {"indicator_code": "DPV_NA_ISIC_EFNA0101", "negative_allowed": set(), "items": [
        ("EA_GDP", "Gross Domestic Product", "الناتج المحلي الإجمالي", None, 0, True, False, None),
        ("EA_GVA", "Gross Value Added", "القيمة المضافة الاجمالية", "EA_GDP", 1, True, False, 1),
        ("EA_NTP", "Net Taxes on Products", "صافي الضرائب على المنتجات", "EA_GDP", 1, False, False, 1),
        ("EA_AGR", "Agriculture, Forestry & Fishing", "الزراعة  الغابات وصيد الاسماك", "EA_GVA", 2, False, False, 1),
        ("EA_MIN", "Mining & Quarrying", "التعدين والتحجير", "EA_GVA", 2, True, False, 1),
        ("EA_MIN_OIL", "Crude Petroleum & Natural Gas", "الزيت الخام والغاز الطبيعي", "EA_MIN", 3, False, False, 1),
        ("EA_MIN_OTH", "Other Mining & Quarrying", "أنشطة تعدينية وتحجيرية أخرى", "EA_MIN", 3, False, False, 1),
        ("EA_MFG", "Manufacturing", "الصناعات التحويلية", "EA_GVA", 2, True, False, 1),
        ("EA_MFG_REF", "Petroleum Refining", "تكرير الزيت", "EA_MFG", 3, False, False, 1),
        ("EA_MFG_XREF", "Manufacturing excluding petroleum refining", "الصناعات التحويلية ماعدا تكرير الزيت", "EA_MFG", 3, False, False, 1),
        ("EA_UTL", "Electricity, Gas and Water", "الكهرباء، الغاز والماء", "EA_GVA", 2, False, False, 1),
        ("EA_CON", "Construction", "التشييد والبناء", "EA_GVA", 2, False, False, 1),
        ("EA_TRD", "Wholesale & Retail Trade, Restaurants & hotels", "تجارة الجملة والتجزئة والمطاعم والفنادق", "EA_GVA", 2, False, False, 1),
        ("EA_TRN", "Transport, Storage & Communication", "النقل والتخزين والاتصالات", "EA_GVA", 2, False, False, 1),
        ("EA_FIRE", "Finance, Insurance, Real Estate & Business Services", "خدمات المال والتأمين والعقارات وخدمات الاعمال", "EA_GVA", 2, True, False, 1),
        ("EA_FIRE_RE", "Real Estate", "الأنشطة العقارية", "EA_FIRE", 3, False, False, 1),
        ("EA_FIRE_FIN", "Finance, Insurance and Business services", "خدمات المال والتأمين وخدمات الأعمال", "EA_FIRE", 3, False, False, 1),
        ("EA_CSP", "Community, Social & Personal Services", "خدمات جماعية واجتماعية وشخصية", "EA_GVA", 2, False, False, 1),
        ("EA_GOV", "Government Services", "الخدمات الحكومية", "EA_GVA", 2, False, False, 1)]},
    "dim_expenditure_item": {"indicator_code": "DPV_NA_EXP_EFNA0101", "negative_allowed": {"EX_INV", "EX_NX"}, "items": [
        ("EX_GDP", "Gross Domestic Product", "الإنفاق على الناتج المحلي الإجمالي", None, 0, True, False, None),
        ("EX_GFCE", "Gross Final Consumption Expenditure", "إجمالي الاستهلاك النهائي", "EX_GDP", 1, True, False, 1),
        ("EX_GFCE_GOV", "Govt. Final Consumption Expenditure", "الإنفاق الاستهلاكي النهائي الحكومي", "EX_GFCE", 2, False, False, 1),
        ("EX_GFCE_PRV", "Private Final Consumption Expenditure", "الإنفاق الاستهلاكي النهائي الخاص", "EX_GFCE", 2, False, False, 1),
        ("EX_GCF", "Gross Capital Formation", "تكوين رأس المال الإجمالي", "EX_GDP", 1, True, False, 1),
        ("EX_GFCF", "Gross Fixed Capital Formation", "تكوين رأس المال الثابت الإجمالي", "EX_GCF", 2, True, False, 1),
        ("EX_GFCF_GOV", "Government", "حكومي", "EX_GFCF", 3, False, False, 1),
        ("EX_GFCF_NGOV", "Non-Government", "غير حكومي", "EX_GFCF", 3, True, False, 1),
        ("EX_GFCF_NGOV_OIL", "Oil", "نفطي", "EX_GFCF_NGOV", 4, False, False, 1),
        ("EX_GFCF_NGOV_NOIL", "Non-oil", "غير نفطي", "EX_GFCF_NGOV", 4, False, False, 1),
        ("EX_INV", "Change in Inventories", "التغير في المخزون", "EX_GCF", 2, False, False, 1),
        ("EX_NX", "Net exports", "صافي الصادرات", "EX_GDP", 1, True, False, 1),
        ("EX_X", "Exports of Goods & Services", "صادرات السلع والخدمات", "EX_NX", 2, False, False, 1),
        ("EX_M", "Imports of Goods & Services", "واردات السلع والخدمات", "EX_NX", 2, False, False, -1),
        # Memo item: alternative aggregate (C + GCF). No parent, so it is never summed into GDP.
        ("EX_DFD", "Domestic Final Demand", "الطلب المحلي النهائي", None, 1, True, True, None)]},
    "dim_income_factor": {"indicator_code": "DPV_NA20_EFNA0401", "negative_allowed": set(), "items": [
        ("IN_GDP", "Gross Domestic Product", "الناتج المحلي الإجمالي", None, 0, True, False, None),
        ("IN_COE", "Compensation of Employees", "تعويضات المشتغلين", "IN_GDP", 1, False, False, 1),
        ("IN_NTP", "Net Taxes on Production", "صافي الضرائب على الإنتاج", "IN_GDP", 1, False, False, 1),
        ("IN_CFC", "Consumption of Fixed Capital", "إهلاك رأس المال الثابت", "IN_GDP", 1, False, False, 1),
        ("IN_NOS", "Net Operating Surplus", "فائض التشغيل الصافي", "IN_GDP", 1, False, False, 1)]},
}

# Accounting identities: lhs = sum(coef * rhs) within tolerance. 'error' = hard fail; 'warning' = logged only.
SECTORS = ["EA_AGR", "EA_MIN", "EA_MFG", "EA_UTL", "EA_CON", "EA_TRD", "EA_TRN", "EA_FIRE", "EA_CSP", "EA_GOV"]
RULES = [
    ("isic_gdp_eq_gva_plus_net_taxes", "gdp_isic_qtr", "EA_GDP", {"EA_GVA": 1, "EA_NTP": 1}, "error", TOLERANCE),
    ("isic_mining_eq_crude_plus_other", "gdp_isic_qtr", "EA_MIN", {"EA_MIN_OIL": 1, "EA_MIN_OTH": 1}, "error", TOLERANCE),
    ("isic_manuf_eq_refining_plus_ex_refining", "gdp_isic_qtr", "EA_MFG", {"EA_MFG_REF": 1, "EA_MFG_XREF": 1}, "error", TOLERANCE),
    ("isic_fire_eq_real_estate_plus_finance", "gdp_isic_qtr", "EA_FIRE", {"EA_FIRE_RE": 1, "EA_FIRE_FIN": 1}, "error", TOLERANCE),
    ("isic_gva_eq_sum_of_sectors", "gdp_isic_qtr", "EA_GVA", {s: 1 for s in SECTORS}, "warning", 5),  # 10 rounded terms
    ("exp_gdp_eq_c_plus_gcf_plus_nx", "gdp_expenditure", "EX_GDP", {"EX_GFCE": 1, "EX_GCF": 1, "EX_NX": 1}, "error", TOLERANCE),
    ("exp_dfd_eq_c_plus_gcf", "gdp_expenditure", "EX_DFD", {"EX_GFCE": 1, "EX_GCF": 1}, "error", TOLERANCE),
    ("exp_gfcf_eq_gov_plus_nongov", "gdp_expenditure", "EX_GFCF", {"EX_GFCF_GOV": 1, "EX_GFCF_NGOV": 1}, "error", TOLERANCE),
    ("exp_c_eq_govt_plus_private", "gdp_expenditure", "EX_GFCE", {"EX_GFCE_GOV": 1, "EX_GFCE_PRV": 1}, "error", TOLERANCE),
    ("exp_gcf_eq_gfcf_plus_inventories", "gdp_expenditure", "EX_GCF", {"EX_GFCF": 1, "EX_INV": 1}, "error", TOLERANCE),
    ("exp_nx_eq_exports_minus_imports", "gdp_expenditure", "EX_NX", {"EX_X": 1, "EX_M": -1}, "error", TOLERANCE),
    ("exp_nongov_gfcf_eq_oil_plus_nonoil", "gdp_expenditure", "EX_GFCF_NGOV", {"EX_GFCF_NGOV_OIL": 1, "EX_GFCF_NGOV_NOIL": 1}, "error", TOLERANCE),
    ("inc_gdp_eq_sum_of_components", "gdp_income", "IN_GDP", {"IN_COE": 1, "IN_NTP": 1, "IN_CFC": 1, "IN_NOS": 1}, "error", TOLERANCE),
    ("cross_exp_gdp_eq_income_gdp", "cross", "EX_GDP", {"IN_GDP": 1}, "error", TOLERANCE),
    ("cross_4_quarters_eq_annual_gdp", "cross", "EX_GDP", {"EA_GDP_SUM_4Q": 1}, "error", TOLERANCE),
]

def get_params():
    """Read the widgets once per run and mint a new run id and timestamp."""
    g = dbutils.widgets.get
    return {"catalog": g("catalog"), "schema": g("schema"), "source_root": g("source_root").strip(),
            "allow_raw_fallback": g("allow_raw_fallback") == "true",
            "lookback_quarters": int(g("lookback_quarters")), "lookback_years": int(g("lookback_years")),
            "full_refresh": g("full_refresh") == "true", "run_id": str(uuid.uuid4()),
            "run_ts": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")}

def fq(p, table):
    return f"{p['catalog']}.{p['schema']}.{table}"

# COMMAND ----------

# DBTITLE 1,Table definitions, comments and tags
# DDL for every silver object, with EN + AR column comments, table comments and tags (idempotent)
FACT_COLS = [
    ("indicator_code", "STRING NOT NULL", "GASTAT indicator code | رمز المؤشر"),
    ("item_code", "STRING NOT NULL", "Stable item code from the dimension table | رمز البند الثابت من الجدول المرجعي"),
    ("period_key", "STRING NOT NULL", "Period key: YYYY for annual, YYYY-Qn for quarterly | مفتاح الفترة"),
    ("period_type", "STRING NOT NULL", "annual or quarterly | نوع الفترة: سنوي أو ربع سنوي"),
    ("year", "INT NOT NULL", "Calendar year | السنة"),
    ("quarter", "TINYINT", "Quarter 1-4; NULL for annual data | الربع (فارغ للبيانات السنوية)"),
    ("period_start_date", "DATE", "First day of the period | تاريخ بداية الفترة"),
    ("period_end_date", "DATE", "Last day of the period | تاريخ نهاية الفترة"),
    ("item_label_en", "STRING", "Item label in English (conformed) | اسم البند بالإنجليزية"),
    ("item_label_ar", "STRING", "Item label in Arabic (conformed) | اسم البند بالعربية"),
    ("indicator_name_en", "STRING", "Indicator name in English | اسم المؤشر بالإنجليزية"),
    ("indicator_name_ar", "STRING", "Indicator name in Arabic | اسم المؤشر بالعربية"),
    ("obs_value", "BIGINT NOT NULL", "Observed value in SAR million at current prices | القيمة بالمليون ريال سعودي بالأسعار الجارية"),
    ("unit", "STRING", "Unit of measure | وحدة القياس"),
    ("price_basis", "STRING", "Price basis | أساس الأسعار"),
    ("is_aggregate", "BOOLEAN", "True if the item is a total of other items; never sum it with its children | بند تجميعي لا يجمع مع مكوناته"),
    ("hierarchy_level", "INT", "Hierarchy level, 0 = top total | المستوى في التسلسل الهرمي"),
    ("parent_code", "STRING", "Parent item code | رمز البند الأب"),
    ("_record_hash", "STRING", "SHA-256 of value and attributes for change detection | بصمة السجل لاكتشاف التغييرات"),
    ("_source_file", "STRING", "Source file landed by ADF | مسار الملف المصدر"),
    ("_source_modified_at", "TIMESTAMP", "Source file modification time (UTC) | وقت تعديل الملف المصدر"),
    ("_ingested_at", "TIMESTAMP", "Silver load time (UTC) | وقت التحميل إلى الطبقة الفضية"),
    ("_run_id", "STRING", "Silver run id that last wrote the row | معرف التشغيل"),
    ("first_published_at", "TIMESTAMP", "First time the key was loaded | تاريخ أول تحميل"),
    ("last_revised_at", "TIMESTAMP", "Last time obs_value changed | تاريخ آخر مراجعة للقيمة"),
    ("revision_count", "INT", "Number of obs_value revisions applied | عدد مراجعات القيمة"),
]
DIM_COLS = [
    ("item_code", "STRING NOT NULL", "Stable item code | رمز البند الثابت"),
    ("indicator_code", "STRING NOT NULL", "GASTAT indicator code | رمز المؤشر"),
    ("label_en", "STRING NOT NULL", "Conformed English label | الاسم بالإنجليزية"),
    ("label_ar", "STRING", "Conformed Arabic label | الاسم بالعربية"),
    ("parent_code", "STRING", "Parent item code | رمز البند الأب"),
    ("hierarchy_level", "INT NOT NULL", "Hierarchy level, 0 = top total | المستوى في التسلسل الهرمي"),
    ("is_aggregate", "BOOLEAN NOT NULL", "Item is a total of other items | بند تجميعي"),
    ("is_memo", "BOOLEAN NOT NULL", "Memo item outside the additive hierarchy | بند للعلم خارج التجميع"),
    ("sign_in_parent", "TINYINT", "Sign when summed into the parent (+1 or -1) | الإشارة عند الجمع في البند الأب"),
    ("negative_allowed", "BOOLEAN NOT NULL", "Negative values are valid for this item | يسمح بالقيم السالبة"),
    ("sort_order", "INT", "Display order | ترتيب العرض"),
]
QUARANTINE_COLS = [
    ("_run_id", "STRING", "Run id | معرف التشغيل"), ("_quarantined_at", "TIMESTAMP", "Quarantine time (UTC) | وقت العزل"),
    ("dataset", "STRING", "Source dataset | مجموعة البيانات"), ("indicator_code", "STRING", "GASTAT indicator code | رمز المؤشر"),
    ("period_key", "STRING", "Derived period key, if derivable | مفتاح الفترة"), ("item_label_en_raw", "STRING", "Raw English label | الاسم الأصلي"),
    ("dq_errors", "ARRAY<STRING>", "Failed rules | القواعد التي فشلت"), ("record_json", "STRING", "Original record as JSON | السجل الأصلي"),
    ("_source_file", "STRING", "Source file | الملف المصدر"),
]
AUDIT_COLS = [
    ("run_id", "STRING", "Run id | معرف التشغيل"), ("run_ts", "TIMESTAMP", "Run start (UTC) | وقت التشغيل"),
    ("dataset", "STRING", "Dataset or cross | مجموعة البيانات"), ("check_name", "STRING", "Check name | اسم الفحص"),
    ("check_type", "STRING", "source, feed, row, completeness, identity, label, load | نوع الفحص"),
    ("severity", "STRING", "error = hard fail, blocker = blocks gold, warning, info | الخطورة"),
    ("status", "STRING", "PASS, FAIL, WARN or SKIPPED | النتيجة"), ("tested_count", "BIGINT", "Rows or periods tested | عدد المختبر"),
    ("failed_count", "BIGINT", "Rows or periods failing | عدد الإخفاقات"), ("metric_value", "DOUBLE", "Metric value | قيمة المقياس"),
    ("threshold", "DOUBLE", "Threshold or tolerance | الحد المسموح"), ("details", "STRING", "JSON details | التفاصيل"),
]
STATUS_COLS = [
    ("run_id", "STRING", "Run id | معرف التشغيل"), ("run_ts", "TIMESTAMP", "Run start (UTC) | وقت التشغيل"),
    ("dataset", "STRING", "Dataset | مجموعة البيانات"), ("indicator_code", "STRING", "GASTAT indicator code | رمز المؤشر"),
    ("source_file", "STRING", "File read | الملف المقروء"), ("source_container", "STRING", "Container read | الحاوية"),
    ("rows_in_feed", "BIGINT", "Records in the file | عدد السجلات في الملف"), ("rows_valid", "BIGINT", "Rows passing row gates | السجلات السليمة"),
    ("rows_quarantined", "BIGINT", "Rows quarantined | السجلات المعزولة"), ("expected_rows", "BIGINT", "members x periods | العدد المتوقع"),
    ("missing_rows", "BIGINT", "Expected minus present | السجلات الناقصة"), ("complete", "BOOLEAN", "Feed is complete | البيانات مكتملة"),
    ("gates_passed", "BOOLEAN", "No hard-fail check failed | اجتياز فحوصات الجودة"), ("merged", "BOOLEAN", "MERGE applied | تم الدمج"),
    ("rows_inserted", "BIGINT", "Rows inserted | السجلات المضافة"), ("rows_updated", "BIGINT", "Rows updated | السجلات المحدثة"),
    ("gold_ready", "BOOLEAN", "Safe to publish to gold | جاهز لطبقة الذهب"), ("blockers", "ARRAY<STRING>", "Checks blocking gold | العوائق"),
]
TABLE_DOCS = {
    "gdp_expenditure": ("GDP at current prices by expenditure approach, annual, SAR million (GASTAT DPV_NA_EXP_EFNA0101). | الناتج المحلي الإجمالي بالأسعار الجارية حسب الإنفاق، سنوي، بالمليون ريال سعودي", FACT_COLS),
    "gdp_income": ("GDP at current prices by income approach, annual, SAR million (GASTAT DPV_NA20_EFNA0401). | الناتج المحلي الإجمالي بالأسعار الجارية حسب الدخل، سنوي، بالمليون ريال سعودي", FACT_COLS),
    "gdp_isic_qtr": ("Quarterly GDP at current prices by economic activity, SAR million (GASTAT DPV_NA_ISIC_EFNA0101). Check _dataset_status.complete before use. | الناتج المحلي الإجمالي الربعي بالأسعار الجارية حسب النشاط الاقتصادي، بالمليون ريال سعودي", FACT_COLS),
    "dim_economic_activity": ("Economic activity reference: stable codes and hierarchy. | جدول مرجعي للأنشطة الاقتصادية مع الرموز والتسلسل الهرمي", DIM_COLS),
    "dim_expenditure_item": ("Expenditure item reference: stable codes and hierarchy. | جدول مرجعي لبنود الإنفاق مع الرموز والتسلسل الهرمي", DIM_COLS),
    "dim_income_factor": ("Income factor reference: stable codes and hierarchy. | جدول مرجعي لعناصر الدخل مع الرموز والتسلسل الهرمي", DIM_COLS),
    "_quarantine": ("Rows rejected by the silver quality gates, with the failed rules. | السجلات المرفوضة من فحوصات الجودة", QUARANTINE_COLS),
    "_dq_audit": ("Result of every silver quality check, one row per check per run. | نتائج فحوصات جودة البيانات لكل تشغيل", AUDIT_COLS),
    "_dataset_status": ("Per-run dataset completeness and gold readiness. | حالة اكتمال البيانات والجاهزية لطبقة الذهب", STATUS_COLS),
}
FACT_DATA_COLS = [c for c, _, _ in FACT_COLS if c not in ("first_published_at", "last_revised_at", "revision_count")]

def esc(s):
    return s.replace("\\", "\\\\").replace("'", "\\'")

def add_check_constraint(table, name, expr):
    props = {r["key"] for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    if f"delta.constraints.{name}" not in props:
        spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expr})")

def setup_objects(p):
    """Create schema and tables if missing; refresh table comments and tags on every run."""
    schema = f"{p['catalog']}.{p['schema']}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} MANAGED LOCATION '{SILVER_LOCATION}'")
    spark.sql(f"COMMENT ON SCHEMA {schema} IS '{esc('GASTAT national accounts, silver layer (cleaned, conformed, validated). | الحسابات القومية - الطبقة الفضية')}'")
    spark.sql(f"ALTER SCHEMA {schema} SET TAGS ('domain' = 'national_accounts', 'source' = 'GASTAT', 'layer' = 'silver')")
    for t, (comment, cols) in TABLE_DOCS.items():
        col_ddl = ",\n  ".join(f"{n} {tp} COMMENT '{esc(cm)}'" for n, tp, cm in cols)
        if t in DATASETS:
            extra, cluster = f",\n  CONSTRAINT pk_{t} PRIMARY KEY (indicator_code, item_code, period_key)", "CLUSTER BY (item_code, year) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
        elif t in DIMS:
            extra, cluster = f",\n  CONSTRAINT pk_{t} PRIMARY KEY (item_code)", ""
        else:
            extra, cluster = "", ""
        spark.sql(f"CREATE TABLE IF NOT EXISTS {fq(p, t)} (\n  {col_ddl}{extra}) {cluster}")
        spark.sql(f"COMMENT ON TABLE {fq(p, t)} IS '{esc(comment)}'")
        tags = "'domain' = 'national_accounts', 'source' = 'GASTAT', 'layer' = 'silver', 'pii' = 'false'"
        if t in DATASETS:
            tags += f", 'unit' = '{UNIT}', 'price_basis' = '{PRICE_BASIS}', 'frequency' = '{CONTRACT[t]['grain']}', 'indicator_code' = '{CONTRACT[t]['indicator_code']}'"
            add_check_constraint(fq(p, t), "valid_period",
                "(period_type = 'annual' AND quarter IS NULL) OR (period_type = 'quarterly' AND quarter BETWEEN 1 AND 4)")
        spark.sql(f"ALTER TABLE {fq(p, t)} SET TAGS ({tags})")

# COMMAND ----------

# DBTITLE 1,Source discovery and explicit-schema read
# Locate the ADF-landed file per dataset and read it with an explicit schema (no inference)
def list_json_files(path, depth=0, max_depth=5):
    try:
        entries = dbutils.fs.ls(path)
    except Exception:  # path does not exist
        return []
    out = []
    for e in entries:
        if e.isDir() and depth < max_depth and not e.name.startswith(("_", ".")):
            out += list_json_files(e.path, depth + 1, max_depth)
        elif e.name.lower().endswith(".json"):
            out.append(e)
    return out

def resolve_source(dataset, p):
    """Bronze first (bronze/<ds>/ or bronze/gastat/<ds>/); raw/gastat/<ds>/ only if fallback is enabled. Latest file wins."""
    roots = [p["source_root"].rstrip("/") + "/"] if p["source_root"] else \
            [BRONZE_ROOT, BRONZE_ROOT + "gastat/"] + ([RAW_ROOT] if p["allow_raw_fallback"] else [])
    tried = []
    for root in roots:
        tried.append(root + dataset + "/")
        files = list_json_files(root + dataset + "/")
        if files:
            latest = max(files, key=lambda f: f.modificationTime)
            container = root.split("://")[1].split("@")[0]
            return {"path": latest.path, "container": container, "files_found": len(files),
                    "fallback": container != "bronze", "tried": tried}
    raise FileNotFoundError(f"No JSON file found for {dataset}. Tried: {tried}")

def read_feed(dataset, src):
    """Strip the UTF-8 BOM, parse the OData envelope, explode `value`, parse each record with an explicit StructType.
    All fields are read as STRING and typed later with try_cast, so bad values are quarantined instead of silently nulled."""
    rec_schema = StructType([StructField(f, StringType(), True) for f in CONTRACT[dataset]["fields"]])
    env_schema = "struct<value: array<string>>"
    doc = (spark.read.text(src["path"], wholetext=True)
           .select(F.col("value").startswith("\ufeff").alias("has_bom"),
                   F.regexp_replace("value", "^\ufeff", "").alias("doc"),
                   F.col("_metadata.file_path").alias("_source_file"),
                   F.col("_metadata.file_modification_time").alias("_source_modified_at")))
    envelope = doc.select("has_bom", F.json_object_keys("doc").alias("keys"),
                          F.size(F.from_json("doc", env_schema)["value"]).alias("n"))
    records = (doc.select("_source_file", "_source_modified_at",
                          F.posexplode(F.from_json("doc", env_schema)["value"]).alias("_pos", "_rec"))
               .withColumn("r", F.from_json("_rec", rec_schema)))
    return envelope, records

# COMMAND ----------

# DBTITLE 1,Cleaning, conforming and row-level gates
# Label cleaning, dimension mapping, typing, period derivation and row-level quality gates
def clean_en(c):
    c = F.trim(F.regexp_replace(c, r"[\s\u00A0]+", " "))
    c = F.regexp_replace(c, r"^['\u2018\u2019`]+\s*", "")      # leading apostrophe
    c = F.regexp_replace(c, r"\s+,", ",")                         # 'Finance , Insurance' -> 'Finance, Insurance'
    c = F.regexp_replace(c, r"(?i)\bsevices\b", "services")       # source typo
    return F.nullif(F.trim(c), F.lit(""))

def clean_ar(c):
    c = F.trim(F.regexp_replace(c, r"[\s\u00A0]+", " "))
    return F.nullif(F.trim(F.regexp_replace(c, r"^['\u2018\u2019`]+\s*", "")), F.lit(""))

def dim_frame(dim_name):
    d = DIMS[dim_name]
    rows = [(code, d["indicator_code"], en, ar, parent, lvl, agg, memo, sign, code in d["negative_allowed"], i + 1)
            for i, (code, en, ar, parent, lvl, agg, memo, sign) in enumerate(d["items"])]
    df = spark.createDataFrame(rows, "item_code string, indicator_code string, label_en string, label_ar string, parent_code string, "
                                     "hierarchy_level int, is_aggregate boolean, is_memo boolean, sign_in_parent tinyint, negative_allowed boolean, sort_order int")
    return df.withColumn("label_en", clean_en(F.col("label_en"))).withColumn("label_ar", clean_ar(F.col("label_ar")))

def conform(dataset, records, dim_df):
    """Typed, cleaned, coded rows plus a `dq_errors` array (empty = valid)."""
    c = CONTRACT[dataset]
    r = lambda f: F.col(f"r.{f}")
    quarterly = c["grain"] == "quarterly"
    q_raw = F.upper(F.trim(r("QUARTER_TIME"))) if quarterly else F.lit(None).cast("string")
    dim = F.broadcast(dim_df.select(F.lower("label_en").alias("_label_key"), "item_code", F.col("label_en").alias("item_label_en"),
                                    F.col("label_ar").alias("item_label_ar"), "parent_code", "hierarchy_level", "is_aggregate", "negative_allowed"))
    df = (records
          .withColumn("label_en_raw", r(c["label_en"]))
          .withColumn("label_en_clean", clean_en(r(c["label_en"])))
          .withColumn("label_ar_clean", clean_ar(r(c["label_ar"])))
          .withColumn("year", F.expr("try_cast(trim(r.YEAR_TIME) AS INT)"))
          .withColumn("quarter", F.when(q_raw.rlike("^Q[1-4]$"), F.substring(q_raw, 2, 1).cast("tinyint")) if quarterly else F.lit(None).cast("tinyint"))
          .withColumn("obs_value", F.expr(f"try_cast(trim(r.{c['value']}) AS BIGINT)"))
          .withColumn("period_type", F.lit(c["grain"]))
          .withColumn("period_key", F.concat(F.col("year").cast("string"), F.lit("-Q"), F.col("quarter").cast("string")) if quarterly
                      else F.col("year").cast("string"))
          .withColumn("source_indicator_name", r("INDICATOR_NAME_ENGL") if "INDICATOR_NAME_ENGL" in c["fields"] else F.lit(None).cast("string"))
          .join(dim, F.lower(F.col("label_en_clean")) == F.col("_label_key"), "left"))
    rules = [
        ("null_key", F.col("label_en_clean").isNull() | r("YEAR_TIME").isNull()),
        ("invalid_year", r("YEAR_TIME").isNotNull() & (F.col("year").isNull() | ~F.col("year").between(1900, dt.date.today().year + 1))),
        ("invalid_value", F.col("obs_value").isNull()),
        ("unmapped_label", F.col("label_en_clean").isNotNull() & F.col("item_code").isNull()),
        ("negative_not_allowed", (F.col("obs_value") < 0) & ~F.coalesce(F.col("negative_allowed"), F.lit(False))),
    ]
    if quarterly:
        rules += [("invalid_quarter", F.col("quarter").isNull()),
                  ("invalid_frequency", F.coalesce(F.upper(F.trim(r("FREQUENCY_TYPE_TIME"))), F.lit("")) != "QRT")]
    if "YEAR_QUARTER_TIME" in c["fields"]:   # annual rows carry 'YYYY-' (empty quarter part)
        rules += [("invalid_quarter", r("YEAR_QUARTER_TIME").isNotNull() & (F.trim(r("YEAR_QUARTER_TIME")) != F.concat(F.trim(r("YEAR_TIME")), F.lit("-"))))]
    return df.withColumn("dq_errors", F.array_distinct(F.array_compact(F.array(*[F.when(cond, F.lit(n)) for n, cond in rules]))))

def row_gates(dataset, flagged, n_feed):
    """Split valid / quarantined rows, resolve duplicate keys, and return audit rows."""
    w = Window.partitionBy("item_code", "period_key")
    valid0 = (flagged.filter(F.size("dq_errors") == 0)
              .withColumn("_dup_n", F.count(F.lit(1)).over(w))
              .withColumn("_dup_vals", F.size(F.collect_set("obs_value").over(w)))
              .withColumn("_rn", F.row_number().over(w.orderBy(F.col("_source_modified_at").desc(), F.col("_pos").desc()))))
    conflicts = valid0.filter("_dup_n > 1 AND _dup_vals > 1").withColumn("dq_errors", F.array(F.lit("duplicate_key_conflict")))
    valid = valid0.filter("_dup_vals = 1 AND _rn = 1").drop("_dup_n", "_dup_vals", "_rn")
    bad = flagged.filter(F.size("dq_errors") > 0).unionByName(conflicts.drop("_dup_n", "_dup_vals", "_rn"))
    identical_dups = valid0.filter("_dup_vals = 1 AND _rn > 1").count()
    counts = {r_[0]: r_[1] for r_ in bad.select(F.explode("dq_errors").alias("e")).groupBy("e").count().collect()}
    rule_names = ["null_key", "invalid_year", "invalid_value", "invalid_quarter", "unmapped_label", "negative_not_allowed", "duplicate_key_conflict"]
    if CONTRACT[dataset]["grain"] == "quarterly":
        rule_names.append("invalid_frequency")
    rows = []
    for name in rule_names:
        details = None
        if name == "unmapped_label" and counts.get(name):
            details = {"unmapped_labels": [x[0] for x in bad.filter(F.array_contains("dq_errors", name)).select("label_en_clean").distinct().collect()]}
        rows.append(audit(dataset, name, "row", "error", "FAIL" if counts.get(name) else "PASS", tested=n_feed, failed=counts.get(name, 0), details=details))
    rows.append(audit(dataset, "duplicate_identical_dropped", "row", "warning", "WARN" if identical_dups else "PASS", tested=n_feed, failed=identical_dups))
    # Label drift: source Arabic label differs from the reference Arabic label (mapping is by English label)
    ar_drift = valid.filter(~F.col("label_ar_clean").eqNullSafe(F.col("item_label_ar"))).select("label_en_clean", "label_ar_clean", "item_label_ar").distinct().collect()
    rows.append(audit(dataset, "arabic_label_matches_reference", "label", "warning", "WARN" if ar_drift else "PASS", failed=len(ar_drift),
                      details=[r_.asDict() for r_ in ar_drift][:20] or None))
    if "INDICATOR_NAME_ENGL" in CONTRACT[dataset]["fields"]:   # confirms price basis from the payload
        wrong_basis = valid.filter(~F.lower("source_indicator_name").contains("current prices")).count()
        rows.append(audit(dataset, "price_basis_is_current_prices", "label", "warning", "WARN" if wrong_basis else "PASS", tested=n_feed, failed=wrong_basis))
    return valid, bad, rows

def to_fact(dataset, valid, p):
    c = CONTRACT[dataset]
    quarterly = c["grain"] == "quarterly"
    start = F.make_date(F.col("year"), ((F.col("quarter").cast("int") - 1) * 3 + 1) if quarterly else F.lit(1), F.lit(1))
    hash_cols = ["obs_value", "item_label_en", "item_label_ar", "indicator_name_en", "indicator_name_ar", "unit", "price_basis", "is_aggregate", "hierarchy_level", "parent_code"]
    df = (valid.select(
            F.lit(c["indicator_code"]).alias("indicator_code"), "item_code", "period_key", "period_type", "year", "quarter",
            start.alias("period_start_date"), F.last_day(F.add_months(start, 2 if quarterly else 11)).alias("period_end_date"),
            "item_label_en", "item_label_ar", F.lit(c["indicator_name_en"]).alias("indicator_name_en"), F.lit(c["indicator_name_ar"]).alias("indicator_name_ar"),
            "obs_value", F.lit(UNIT).alias("unit"), F.lit(PRICE_BASIS).alias("price_basis"), "is_aggregate", "hierarchy_level", "parent_code",
            "_source_file", "_source_modified_at")
          .withColumn("_record_hash", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(h).cast("string"), F.lit("<null>")) for h in hash_cols]), 256))
          .withColumn("_ingested_at", F.lit(p["run_ts"]).cast("timestamp"))
          .withColumn("_run_id", F.lit(p["run_id"])))
    return df.select(*FACT_DATA_COLS)

# COMMAND ----------

# DBTITLE 1,Feed, completeness and identity checks
# Dataset-level checks: feed envelope and schema contract, completeness (members x periods), accounting identities
def audit(dataset, check, ctype, severity, status, tested=None, failed=None, metric=None, threshold=None, details=None):
    return {"dataset": dataset, "check_name": check, "check_type": ctype, "severity": severity, "status": status,
            "tested_count": None if tested is None else int(tested), "failed_count": None if failed is None else int(failed),
            "metric_value": None if metric is None else float(metric), "threshold": None if threshold is None else float(threshold),
            "details": None if details is None else json.dumps(details, ensure_ascii=False, default=str)}

def feed_checks(dataset, envelope, records):
    e = envelope.first()
    n = e["n"] if e is not None and e["n"] is not None and e["n"] >= 0 else None
    keys = list(e["keys"] or []) if e is not None else []
    next_links = [k for k in keys if k.lower().endswith("nextlink")]
    capped = bool(n) and n % 1000 == 0
    observed = {x[0] for x in records.select(F.explode(F.json_object_keys("_rec"))).distinct().collect()}
    expected = set(CONTRACT[dataset]["fields"])
    missing, extra = sorted(expected - observed), sorted(observed - expected)
    rows = [
        audit(dataset, "envelope_parse", "feed", "error", "PASS" if n is not None else "FAIL", tested=1, metric=n, details={"envelope_keys": keys}),
        audit(dataset, "utf8_bom_stripped", "feed", "info", "PASS", details={"had_bom": bool(e and e["has_bom"])}),
        audit(dataset, "paging_next_link_absent", "feed", "blocker", "FAIL" if next_links else "PASS", details={"next_link_keys": next_links}),
        audit(dataset, "api_page_cap_not_hit", "feed", "blocker", "FAIL" if capped else "PASS", tested=n, metric=n, threshold=1000,
              details={"records": n, "note": "Record count is an exact multiple of 1000: the ADF copy fetched only the first API page. Fix ADF paging ($top/$skip or next link)." if capped else None}),
        audit(dataset, "schema_contract_fields_present", "feed", "error", "FAIL" if missing else "PASS", failed=len(missing), details={"missing_fields": missing}),
        audit(dataset, "schema_contract_no_new_fields", "feed", "warning", "WARN" if extra else "PASS", failed=len(extra), details={"new_fields": extra}),
    ]
    return rows, (n or 0)

def period_grid(grain, lo, hi):
    ids = spark.range(lo, hi + 1)
    if grain == "quarterly":   # id = year * 4 + quarter - 1
        return ids.select(F.concat(F.floor(F.col("id") / 4).cast("string"), F.lit("-Q"), (F.col("id") % 4 + 1).cast("string")).alias("period_key"))
    return ids.select(F.col("id").cast("string").alias("period_key"))

def completeness(dataset, fact, dim_df):
    """Expected grid = every reference member x every period between the first and last period in the feed."""
    grain = CONTRACT[dataset]["grain"]
    idx = (F.col("year") * 4 + F.col("quarter") - 1) if grain == "quarterly" else F.col("year")
    span = fact.agg(F.min(idx).alias("lo"), F.max(idx).alias("hi"), F.count("*").alias("actual")).first()
    if span["actual"] == 0:
        return [audit(dataset, "completeness_members_x_periods", "completeness", "blocker", "FAIL", tested=0, failed=0, details={"note": "no valid rows"})], {"expected": 0, "missing": 0, "complete": False}
    grid = dim_df.select("item_code").crossJoin(period_grid(grain, int(span["lo"]), int(span["hi"])))
    missing = grid.join(fact.select("item_code", "period_key"), ["item_code", "period_key"], "left_anti")
    by_item = {x["item_code"]: x["count"] for x in missing.groupBy("item_code").count().collect()}
    n_members, n_periods = dim_df.count(), int(span["hi"]) - int(span["lo"]) + 1
    expected, n_missing = n_members * n_periods, sum(by_item.values())
    first, last = period_grid(grain, int(span["lo"]), int(span["lo"])).first()[0], period_grid(grain, int(span["hi"]), int(span["hi"])).first()[0]
    row = audit(dataset, "completeness_members_x_periods", "completeness", "blocker", "FAIL" if n_missing else "PASS",
                tested=expected, failed=n_missing, metric=round(span["actual"] / expected, 4), threshold=1.0,
                details={"members": n_members, "periods": n_periods, "period_range": f"{first}..{last}", "expected_rows": expected,
                         "actual_rows": span["actual"], "missing_by_item": dict(sorted(by_item.items(), key=lambda kv: -kv[1]))})
    return [row], {"expected": expected, "missing": n_missing, "complete": n_missing == 0}

def cross_long(facts):
    """Annual GDP from each approach plus the sum of quarterly GDP for years with all 4 quarters present."""
    exp = facts["gdp_expenditure"].filter("item_code = 'EX_GDP'").select("period_key", "item_code", "obs_value")
    inc = facts["gdp_income"].filter("item_code = 'IN_GDP'").select("period_key", "item_code", "obs_value")
    q4 = (facts["gdp_isic_qtr"].filter("item_code = 'EA_GDP'").groupBy("year")
          .agg(F.count("*").alias("n"), F.sum("obs_value").alias("obs_value")).filter("n = 4")
          .select(F.col("year").cast("string").alias("period_key"), F.lit("EA_GDP_SUM_4Q").alias("item_code"), "obs_value"))
    return exp.unionByName(inc).unionByName(q4)

def identity_check(rule, long_df):
    """Test lhs - sum(coef * rhs) per period, only where every term is present; the rest are reported as untestable."""
    name, dataset, lhs, rhs, severity, tol = rule
    terms = {lhs: 1, **{k: -v for k, v in rhs.items()}}
    coef = F.create_map(*[x for k, v in terms.items() for x in (F.lit(k), F.lit(v))])
    per = (long_df.filter(F.col("item_code").isin(list(terms))).groupBy("period_key")
           .agg(F.sum(F.col("obs_value") * coef[F.col("item_code")]).alias("diff"), F.countDistinct("item_code").alias("n"))
           .filter(F.col("n") == len(terms)).withColumn("fail", F.abs("diff") > tol))
    s = per.agg(F.count("*").alias("tested"), F.coalesce(F.sum(F.col("fail").cast("int")), F.lit(0)).alias("failed"), F.max(F.abs("diff")).alias("max_abs")).first()
    total = long_df.select("period_key").distinct().count()
    failing = [(x["period_key"], x["diff"]) for x in per.filter("fail").orderBy("period_key").limit(20).collect()]
    status = "SKIPPED" if s["tested"] == 0 else ("FAIL" if s["failed"] else "PASS")
    return audit(dataset, name, "identity", severity, status, tested=s["tested"], failed=s["failed"], metric=s["max_abs"], threshold=tol,
                 details={"formula": f"{lhs} = " + " + ".join(f"{'-' if v < 0 else ''}{k}" for k, v in rhs.items()),
                          "untestable_periods": total - s["tested"], "failing_periods": failing})

# COMMAND ----------

# DBTITLE 1,Writers and idempotent MERGE
# Writers: dimensions, quarantine, audit, status, and the lookback-window MERGE (never deletes)
AUDIT_FIELDS = ["dataset", "check_name", "check_type", "severity", "status", "tested_count", "failed_count", "metric_value", "threshold", "details"]

def write_dim(dim_name, dim_df, p):
    dim_df.select(*[c for c, _, _ in DIM_COLS]).write.insertInto(fq(p, dim_name), overwrite=True)   # keeps comments and tags

def write_quarantine(dataset, bad, p):
    (bad.select(F.lit(p["run_id"]).alias("_run_id"), F.lit(p["run_ts"]).cast("timestamp").alias("_quarantined_at"), F.lit(dataset).alias("dataset"),
                F.lit(CONTRACT[dataset]["indicator_code"]).alias("indicator_code"), "period_key", F.col("label_en_raw").alias("item_label_en_raw"),
                "dq_errors", F.col("_rec").alias("record_json"), "_source_file")
        .write.mode("append").saveAsTable(fq(p, "_quarantine")))

def write_audit(rows, p):
    df = spark.createDataFrame([tuple(r[k] for k in AUDIT_FIELDS) for r in rows],
        "dataset string, check_name string, check_type string, severity string, status string, tested_count long, failed_count long, metric_value double, threshold double, details string")
    df.select(F.lit(p["run_id"]).alias("run_id"), F.lit(p["run_ts"]).cast("timestamp").alias("run_ts"), *AUDIT_FIELDS).write.mode("append").saveAsTable(fq(p, "_dq_audit"))

def write_status(status, p):
    cols = [c for c, _, _ in STATUS_COLS if c not in ("run_id", "run_ts")]
    df = spark.createDataFrame([tuple(s[c] for c in cols) for s in status.values()],
        "dataset string, indicator_code string, source_file string, source_container string, rows_in_feed long, rows_valid long, rows_quarantined long, "
        "expected_rows long, missing_rows long, complete boolean, gates_passed boolean, merged boolean, rows_inserted long, rows_updated long, gold_ready boolean, blockers array<string>")
    df.select(F.lit(p["run_id"]).alias("run_id"), F.lit(p["run_ts"]).cast("timestamp").alias("run_ts"), *cols).write.mode("append").saveAsTable(fq(p, "_dataset_status"))
    return spark.table(fq(p, "_dataset_status")).filter(F.col("run_id") == p["run_id"])

def merge_fact(dataset, fact, p):
    """Insert new keys for any period; update changed rows only inside the lookback window (or everywhere on full refresh).
    Keys missing from the feed are kept (no WHEN NOT MATCHED BY SOURCE clause)."""
    tgt = fq(p, dataset)
    quarterly = CONTRACT[dataset]["grain"] == "quarterly"
    idx = (F.col("year") * 4 + F.col("quarter") - 1) if quarterly else F.col("year")
    lookback = p["lookback_quarters"] if quarterly else p["lookback_years"]
    max_idx = fact.agg(F.max(idx)).first()[0]
    src = fact.withColumn("_in_window", F.lit(True) if p["full_refresh"] else idx > F.lit(max_idx - lookback))
    outside = (src.alias("s").join(spark.table(tgt).alias("t"), KEY_COLS)
               .filter("NOT s._in_window AND s._record_hash <> t._record_hash").count())
    view = f"stg_{dataset}_{p['run_id'][:8]}"
    src.createOrReplaceTempView(view)
    upd = ",\n      ".join(f"t.{c} = s.{c}" for c in FACT_DATA_COLS if c not in KEY_COLS)
    ins_cols = FACT_DATA_COLS + ["first_published_at", "last_revised_at", "revision_count"]
    ins_vals = [f"s.{c}" for c in FACT_DATA_COLS] + ["s._ingested_at", "s._ingested_at", "0"]
    spark.sql(f"""
    MERGE INTO {tgt} t
    USING {view} s
    ON t.indicator_code = s.indicator_code AND t.item_code = s.item_code AND t.period_key = s.period_key
    WHEN MATCHED AND s._in_window AND t._record_hash <> s._record_hash THEN UPDATE SET
      {upd},
      t.last_revised_at = CASE WHEN t.obs_value <> s.obs_value THEN s._ingested_at ELSE t.last_revised_at END,
      t.revision_count = t.revision_count + CASE WHEN t.obs_value <> s.obs_value THEN 1 ELSE 0 END
    WHEN NOT MATCHED THEN INSERT ({", ".join(ins_cols)}) VALUES ({", ".join(ins_vals)})""")
    spark.catalog.dropTempView(view)
    h = spark.sql(f"DESCRIBE HISTORY {tgt} LIMIT 1").first()
    m = h["operationMetrics"] if h["operation"] == "MERGE" else {}
    inserted, updated = int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0))
    retained = spark.table(tgt).join(fact.select(*KEY_COLS), KEY_COLS, "left_anti").count()
    total = spark.table(tgt).count()
    rows = [audit(dataset, "merge_applied", "load", "info", "PASS", tested=fact.count(), metric=total,
                  details={"inserted": inserted, "updated": updated, "table_rows": total, "lookback": lookback, "full_refresh": p["full_refresh"],
                           "window_start": None if p["full_refresh"] else period_grid("quarterly" if quarterly else "annual", max_idx - lookback + 1, max_idx - lookback + 1).first()[0]}),
            audit(dataset, "revisions_outside_lookback_not_applied", "load", "warning", "WARN" if outside else "PASS", failed=outside),
            audit(dataset, "rows_retained_not_in_feed", "load", "info", "PASS", metric=retained)]
    return rows, inserted, updated

# COMMAND ----------

# DBTITLE 1,Orchestrator
# One function runs the whole silver build. Audit and quarantine are always written; MERGE runs only if no hard gate failed.
def run(p):
    setup_objects(p)
    audit_rows, facts, status, dims = [], {}, {}, {}
    for dim_name in DIMS:
        dims[dim_name] = dim_frame(dim_name)
        write_dim(dim_name, dims[dim_name], p)

    for ds in DATASETS:
        c = CONTRACT[ds]
        src = resolve_source(ds, p)
        audit_rows.append(audit(ds, "source_is_bronze", "source", "blocker", "WARN" if src["fallback"] else "PASS",
                                details={**src, "note": "Bronze had no file; read the raw landing zone instead. ADF must land into bronze." if src["fallback"] else None}))
        envelope, records = read_feed(ds, src)
        rows, n_feed = feed_checks(ds, envelope, records)
        audit_rows += rows
        valid, bad, rows = row_gates(ds, conform(ds, records, dims[c["dim"]]), n_feed)
        audit_rows += rows
        write_quarantine(ds, bad, p)
        facts[ds] = to_fact(ds, valid, p)
        rows, comp = completeness(ds, facts[ds], dims[c["dim"]])
        audit_rows += rows
        status[ds] = {"dataset": ds, "indicator_code": c["indicator_code"], "source_file": src["path"], "source_container": src["container"],
                      "rows_in_feed": n_feed, "rows_valid": facts[ds].count(), "rows_quarantined": bad.count(),
                      "expected_rows": comp["expected"], "missing_rows": comp["missing"], "complete": None, "gates_passed": None,
                      "merged": False, "rows_inserted": 0, "rows_updated": 0, "gold_ready": False, "blockers": []}

    long_cross = cross_long(facts)
    for rule in RULES:
        long_df = long_cross if rule[1] == "cross" else facts[rule[1]].select("period_key", "item_code", "obs_value")
        audit_rows.append(identity_check(rule, long_df))

    hard_fails = [r for r in audit_rows if r["severity"] == "error" and r["status"] == "FAIL"]
    for ds, s in status.items():
        mine = [r for r in audit_rows if r["dataset"] in (ds, "cross")]
        s["complete"] = not any(r["check_type"] in ("feed", "completeness") and r["severity"] == "blocker" and r["status"] == "FAIL" for r in mine)
        s["gates_passed"] = not hard_fails
        s["blockers"] = sorted({r["check_name"] for r in mine if (r["severity"] in ("error", "blocker") and r["status"] in ("FAIL", "WARN"))
                                or (r["check_type"] == "identity" and r["severity"] == "error" and r["status"] == "SKIPPED")})
    write_audit(audit_rows, p)

    if hard_fails:
        write_status(status, p)
        raise RuntimeError(f"Silver hard gates failed; nothing merged. Failed checks: {[(r['dataset'], r['check_name']) for r in hard_fails]}")

    load_rows = []
    for ds in DATASETS:
        rows, ins, upd = merge_fact(ds, facts[ds], p)
        load_rows += rows
        s = status[ds]
        s.update(merged=True, rows_inserted=ins, rows_updated=upd, gold_ready=s["complete"] and not s["blockers"])
        spark.sql(f"ALTER TABLE {fq(p, ds)} SET TBLPROPERTIES ('gastat.complete' = '{str(s['complete']).lower()}', "
                  f"'gastat.gold_ready' = '{str(s['gold_ready']).lower()}', 'gastat.last_run_id' = '{p['run_id']}')")
    write_audit(load_rows, p)
    return write_status(status, p)

# COMMAND ----------

# DBTITLE 1,Run the silver build
# Execute; the audit for this run is shown even if a hard gate fails
params = get_params()
try:
    display(run(params))
finally:
    display(spark.table(fq(params, "_dq_audit")).filter(F.col("run_id") == params["run_id"])
            .orderBy(F.expr("CASE status WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 WHEN 'SKIPPED' THEN 2 ELSE 3 END"), "dataset", "check_type", "check_name"))

# COMMAND ----------

# DBTITLE 1,Run notes and gold blockers
# MAGIC %md
# MAGIC ## Run notes (2026-11-10)
# MAGIC
# MAGIC * Bronze container `abfss://bronze@stgastsaudi12.dfs.core.windows.net/` is **empty**. The files are in `raw/gastat/<dataset>/data.json`, so the run used the raw fallback and logged `source_is_bronze = WARN` (blocker). Set `allow_raw_fallback = false` once ADF lands into bronze.
# MAGIC * `gdp_isic_qtr` is **incomplete**: 1,000 of 1,254 rows (19 members x 66 quarters, 2010-Q1..2026-Q2), and the GDP total is present in only 20 quarters. `api_page_cap_not_hit` and `completeness_members_x_periods` fail, `complete = false` in `_dataset_status` and in the `gastat.complete` table property. **ADF paging must be fixed** (`$top`/`$skip` loop until an empty page).
# MAGIC * All hard gates pass: no quarantined rows, all labels mapped, negatives only in Change in Inventories / Net exports, and every identity within +/-1 where it can be tested.
# MAGIC * Rerun is idempotent: the second run inserted and updated 0 rows.
# MAGIC * Gold should read only datasets with `gold_ready = true` in the latest `_dataset_status` run. Today that is none of them, because of the bronze fallback (and, for ISIC, the truncation).