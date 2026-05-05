# Databricks notebook source
# MAGIC %md
# MAGIC # Contact Center Centralized FY2025
# MAGIC
# MAGIC This notebook computes Contact Center risk metrics for FY2025.
# MAGIC
# MAGIC **Metrics Computed:**
# MAGIC - **1.1** — Total number of unrated or unscored customers
# MAGIC - **SD2/ABAC 1.2 PEP** — PEP (Politically Exposed Persons) analysis
# MAGIC - **1.2** — Tier 1/2 High Risk Customers
# MAGIC - **1.3** — High Risk Customers (excluding Tier 1/2)
# MAGIC - **1.4** — Medium Risk Customers
# MAGIC - **1.5** — Low Risk Customers (including unscored)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: JDBC Connections & Credentials

# COMMAND ----------

# ------------------------------------------------------------------
# Cell 1: JDBC connection strings and authentication credentials
# ------------------------------------------------------------------
# Define JDBC URLs for various SQL Server pools used by the pipeline.
# Credentials are fetched from Databricks secrets (Azure AD Service Principal).
# ------------------------------------------------------------------

czJdbcUrl = "jdbc:sqlserver://p3001-eastus2-asql-2.database.windows.net:1433;database=eda-akora2-aaecz-corporatepoolprd;loginTimeout=10;"
srzJdbcURL = "jdbc:sqlserver://p3001-eastus2-asql-3.database.windows.net:1433;database=eda-akora2-aaed1-srzpoolprd;loginTimeout=10"
azJdbcURL = "jdbc:sqlserver://p3006-eastus2-asql-1.database.windows.net:1433;database=eda-akora-aaaz-CAGAML00BI0001ClusterPRD;loginTimeout=10"

izJdbcUrl = "jdbc:sqlserver://p3004-eastus2-asql-32.database.windows.net:1433;database=eda-akora-aaicz-icz00001poolprd;loginTimeout=10"

jdbcUsername = dbutils.secrets.get(scope="aaaz-base", key="SP_ADB_AAAZ_CAGAML00BI0001_PRD_AppID")
jdbcPassword = dbutils.secrets.get(scope="aaaz-base", key="SP_ADB_AAAZ_CAGAML00BI0001_PRD_PWD")

connectionProperties = {
    "AADSecurePrincipalID" : jdbcUsername,
    "AADSecurePrincipalSecret" : jdbcPassword,
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "authentication" : "ActiveDirectoryServicePrincipal",
    "fetchsize" : "10"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build NACO (National Account Centre Operations) Universe
# MAGIC
# MAGIC Create the Contact Centre AU (Assessment Unit) table for the reporting date **20251031**.
# MAGIC
# MAGIC The universe combines:
# MAGIC - **Personal customers** (customr_type = 0) joined with `cif_personal_fy25`
# MAGIC - **Non-personal customers** (customr_type = 1) joined with `cif_non_personal_fy25`
# MAGIC
# MAGIC Filters applied:
# MAGIC - Bank number = 4
# MAGIC - Application ID in ('ACS', 'VSA')
# MAGIC - Effective date on or before 20251031
# MAGIC - Customer status = '00' (active)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE rafy2025_centralized.contact_centre_au_20251031
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT DISTINCT acc.customr_num, acc.customr_type
# MAGIC FROM ra_fy_2025.cif_accounts_fy25 acc
# MAGIC JOIN ra_fy_2025.cif_personal_fy25 pers ON acc.customr_num = pers.customr_num AND acc.customr_bank_num = pers.customr_bank_num AND acc.customr_type = pers.customr_type
# MAGIC WHERE acc.customr_bank_num = 4
# MAGIC AND acc.customr_type = 0
# MAGIC AND acc.aplictn_id in ('ACS', 'VSA')
# MAGIC AND SUBSTRING(acc.ifw_effective_date, 1, 8) <= '20251031'
# MAGIC AND pers.customr_status = '00'
# MAGIC UNION
# MAGIC SELECT DISTINCT acc.customr_num, acc.customr_type
# MAGIC FROM ra_fy_2025.cif_accounts_fy25 acc
# MAGIC JOIN ra_fy_2025.cif_non_personal_fy25 npers ON acc.customr_num = npers.customr_num AND acc.customr_bank_num = npers.customr_bank_num AND acc.customr_type = npers.customr_type
# MAGIC WHERE acc.customr_bank_num = 4
# MAGIC AND acc.customr_type = 1
# MAGIC AND acc.aplictn_id in ('ACS', 'VSA')
# MAGIC AND SUBSTRING(acc.ifw_effective_date, 1, 8) <= '20251031'
# MAGIC --AND SUBSTRING(acc.ifw_effective_date, 1, 8) > '20241031' or SUBSTRING(acc.ifw_effective_date, 1, 8) is NULL
# MAGIC AND npers.customr_status = '00'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Preparation: Load NACO Universe & Build CIF Keys

# COMMAND ----------

# ------------------------------------------------------------------
# Cell 15: Import PySpark SQL functions
# ------------------------------------------------------------------
from pyspark.sql.functions import *

# COMMAND ----------

# ------------------------------------------------------------------
# Cell 16: Load the NACO universe table created in the SQL cell above
# ------------------------------------------------------------------
naco = spark.table("rafy2025_centralized.contact_centre_au_20251031")

# COMMAND ----------

# ------------------------------------------------------------------
# Cell 17: Derive standardized CIF keys from the NACO universe
#   - cust_no  : 9-digit zero-padded customer number
#   - cust_type: 'N' for non-personal (customr_type=1), 'P' for personal
# ------------------------------------------------------------------
cif = naco.withColumn('cust_no', lpad(col('customr_num'), 9, '0')).withColumn('cust_type', when(col('customr_type') == '1', 'N').otherwise('P'))

# COMMAND ----------

# ------------------------------------------------------------------
# Cell 18: Read the latest customer record from CAEDW via JDBC
#   - Uses row_number() to get the most recent record per cust_id
#   - Pulls cust_id, cust_no, cust_type_mn
# ------------------------------------------------------------------
cust = '''(select cust_id, cust_no, cust_type_mn from (select *, row_number() over (partition by cust_id order by to_dt desc) as row_num from caedw.cust) c where c.row_num = 1)t'''
df_cust = spark.read.jdbc(url = czJdbcUrl, table = cust, properties = connectionProperties).cache()

# COMMAND ----------

# ------------------------------------------------------------------
# Cell 19: Join CIF keys with CAEDW customer data to enrich the
#          NACO universe with cust_id and cust_type_mn.
#   - Left join ensures all NACO records are retained
#   - Drop duplicate cust_no column from df_cust after join
# ------------------------------------------------------------------
niu_naco = cif.join(df_cust, (cif.cust_no == df_cust.cust_no) & (cif.cust_type == df_cust.cust_type_mn), how = 'left').drop(df_cust.cust_no)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric 1.1 — Total Number of Unrated or Unscored Customers
# MAGIC
# MAGIC Identifies NACO customers that do **not** appear in the scored/rated customer table.
# MAGIC Uses a `left_anti` join to find unscored CIF customers.

# COMMAND ----------

# ------------------------------------------------------------------
# Cell 21: Metric 1.1 — Unrated / Unscored Customers
#   1. Load the scored customer table (CRR_Scorable_Cust_CA)
#   2. Filter to CIF-prefixed entity IDs only
#   3. Derive cust_no and cust_type_mn from v_entity_id
#   4. Left-anti join with NACO universe to find unscored customers
#   5. Print the count
# ------------------------------------------------------------------

# The 1.1 logic by filtering the scored table by CIF prefix in the v_entity_id
scored_src = spark.table("rafy2025_centralized.CRR_Scorable_Cust_CA")
scored_keys = scored_src.filter(col('v_entity_id').startswith('CIF'))
scored_keys = scored_keys.withColumn('cust_no', substring(col('v_entity_id'), -9, 9)).withColumn('cust_type_mn', when(substring(col("v_entity_id"), 8, 1) == "1", "N").otherwise("P"))

print("scored CIF distinct customers (cust_no) =", scored_keys.count())

metric_1_1 = (
    niu_naco
    .join(scored_keys, on = ["cust_no", "cust_type_mn"], how="left_anti")
    .dropDuplicates()
    )

metric_1_1_count = metric_1_1.count()

print("Metric 1.1 (CIF unscored found in AU) =", metric_1_1_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric 1.2 — Tier 1/2 High Risk Customers
# MAGIC
# MAGIC Counts distinct customers in the NACO universe whose risk rating
# MAGIC is **Tier 1** or **Tier 2** (the highest risk tiers).

# COMMAND ----------

# ------------------------------------------------------------------
# Cell 28: Load rated customers and prepare CIF keys for Tier 1/2
# ------------------------------------------------------------------

# Tier 1/2 High risk customers
hrc1 = spark.table('rafy2025_centralized.customer_scorable_rated_ca')
hrc_cif1 = hrc1.filter(col('v_entity_id').startswith('CIF'))
hrc_cif1 = hrc_cif1.withColumn('cust_no', substring(col('v_entity_id'), -9, 9)).withColumn('cust_type_mn', when(substring(col("v_entity_id"), 8, 1) == "1", "N").otherwise("P"))
display(hrc_cif1)

# COMMAND ----------

# ------------------------------------------------------------------
# Cell 29: Metric 1.2 — Count distinct Tier 1/2 customers in NACO
# ------------------------------------------------------------------
contact_center_hrc1 = hrc_cif1.join(niu_naco, on = ['cust_no', 'cust_type_mn'], how = 'inner').filter(col('risk_rating').isin("Tier 1", "Tier 2"))
contact_center_hrc1.agg(countDistinct('cust_intrl_id').alias('1.2')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric 1.3 — High Risk Customers (Excluding Tier 1/2)
# MAGIC
# MAGIC Counts distinct customers in the NACO universe with a **High** risk
# MAGIC rating (but not Tier 1 or Tier 2).

# COMMAND ----------

# ------------------------------------------------------------------
# Cell 31: Load rated customers and prepare CIF keys for High risk
# ------------------------------------------------------------------

# 1.3 High Risk Customers (exclude tier 1/2)
hrc = spark.table('rafy2025_centralized.customer_scorable_rated_ca')
hrc_cif = hrc.filter(col('v_entity_id').startswith('CIF'))
hrc_cif = hrc_cif.withColumn('cust_no', substring(col('v_entity_id'), -9, 9)).withColumn('cust_type_mn', when(substring(col("v_entity_id"), 8, 1) == "1", "N").otherwise("P"))
display(hrc_cif)

# COMMAND ----------

# ------------------------------------------------------------------
# Cell 32: Metric 1.3 — Count distinct High risk customers in NACO
# ------------------------------------------------------------------
contact_center_hrc = hrc_cif.join(niu_naco, on = ['cust_no', 'cust_type_mn'], how = 'inner').filter(col('risk_rating') == "High")
contact_center_hrc.agg(countDistinct('cust_intrl_id').alias('1.3')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric 1.4 — Medium Risk Customers
# MAGIC
# MAGIC Counts distinct customers in the NACO universe with a **Medium** risk rating.

# COMMAND ----------

# ------------------------------------------------------------------
# Cell 34: Load rated customers and prepare CIF keys for Medium risk
# ------------------------------------------------------------------

# 1.4 Medium Risk Customers
hrc2 = spark.table('rafy2025_centralized.customer_scorable_rated_ca')
hrc_cif2 = hrc2.filter(col('v_entity_id').startswith('CIF'))
hrc_cif2 = hrc_cif2.withColumn('cust_no', substring(col('v_entity_id'), -9, 9)).withColumn('cust_type_mn', when(substring(col("v_entity_id"), 8, 1) == "1", "N").otherwise("P"))
display(hrc_cif2)

# COMMAND ----------

# ------------------------------------------------------------------
# Cell 35: Metric 1.4 — Count distinct Medium risk customers in NACO
# ------------------------------------------------------------------
contact_center_hrc2 = hrc_cif2.join(niu_naco, on = ['cust_no', 'cust_type_mn'], how = 'inner').filter(col('risk_rating') == "Medium")
contact_center_hrc2.agg(countDistinct('cust_intrl_id').alias('1.4')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric 1.5 — Low Risk Customers (Including Unscored)
# MAGIC
# MAGIC Combines two populations:
# MAGIC 1. **Low-rated** customers from the rated table (risk_rating = 'Low')
# MAGIC 2. **Unscored/unrated** customers from the unrated table
# MAGIC
# MAGIC These are unioned together and then counted.

# COMMAND ----------

# ------------------------------------------------------------------
# Cell 37: Load both unrated and rated customer tables, prepare CIF keys
# ------------------------------------------------------------------

# 1.5 Low Risk Customers
hrc3_unscored = spark.table('rafy2025_centralized.customer_scorable_unrated_ca')
hrc_cif3_unscored = hrc3_unscored.filter(col('v_entity_id').startswith('CIF'))
hrc_cif3_unscored = hrc_cif3_unscored.withColumn('cust_no', substring(col('v_entity_id'), -9, 9)).withColumn('cust_type_mn', when(substring(col("v_entity_id"), 8, 1) == "1", "N").otherwise("P"))
hrc3 = spark.table('rafy2025_centralized.customer_scorable_rated_ca')
hrc_cif3 = hrc3.filter(col('v_entity_id').startswith('CIF'))
hrc_cif3 = hrc_cif3.withColumn('cust_no', substring(col('v_entity_id'), -9, 9)).withColumn('cust_type_mn', when(substring(col("v_entity_id"), 8, 1) == "1", "N").otherwise("P"))
display(hrc_cif3_unscored)

# COMMAND ----------

# ------------------------------------------------------------------
# Cell 38: Metric 1.5 — Low risk + unscored customers in NACO
#   - Filter rated customers to 'Low' risk rating
#   - Join unscored customers with NACO universe
#   - Union both sets (allowMissingColumns handles schema differences)
#   - Count distinct cust_intrl_id
# ------------------------------------------------------------------
contact_center_hrc3_low = hrc_cif3.join(niu_naco, on = ['cust_no', 'cust_type_mn'], how = 'inner').filter(col('risk_rating') == 'Low')
contact_center_hrc3_unscored = hrc_cif3_unscored.join(niu_naco, on = ['cust_no', 'cust_type_mn'], how = 'inner')
contact_center_hrc3 = contact_center_hrc3_low.unionByName(contact_center_hrc3_unscored, allowMissingColumns=True)
contact_center_hrc3.agg(countDistinct('cust_intrl_id').alias('1.5')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric 6.5b — Total Number of Customers at End of Review Period
# MAGIC
# MAGIC Two approaches exist for counting the AU universe:
# MAGIC - **Method A** (larger): `COUNT(DISTINCT customr_num, customr_type)` — counts each customer-type combination separately
# MAGIC - **Method B** (smaller, used by business): `COUNT(DISTINCT customr_num)` — deduplicates across personal/non-personal
# MAGIC
# MAGIC The business reports use **Method B**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- 6.5b Method A: COUNT by (customr_num, customr_type) — LARGER
# MAGIC -- This is the same universe as the NACO table above.
# MAGIC -- Result: ~16,851,015
# MAGIC -- ============================================================
# MAGIC select count(*) as 6_5b from
# MAGIC (
# MAGIC SELECT DISTINCT acc.customr_num, acc.customr_type
# MAGIC FROM ra_fy_2025.cif_accounts_fy25 acc
# MAGIC JOIN ra_fy_2025.cif_personal_fy25 pers ON acc.customr_num = pers.customr_num AND acc.customr_bank_num = pers.customr_bank_num AND acc.customr_type = pers.customr_type
# MAGIC WHERE acc.customr_bank_num = 4
# MAGIC AND acc.customr_type = 0
# MAGIC AND acc.aplictn_id in ('ACS', 'VSA')
# MAGIC AND SUBSTRING(acc.ifw_effective_date, 1, 8) <= '20251031'
# MAGIC AND pers.customr_status = '00'
# MAGIC UNION
# MAGIC SELECT DISTINCT acc.customr_num, acc.customr_type
# MAGIC FROM ra_fy_2025.cif_accounts_fy25 acc
# MAGIC JOIN ra_fy_2025.cif_non_personal_fy25 npers ON acc.customr_num = npers.customr_num AND acc.customr_bank_num = npers.customr_bank_num AND acc.customr_type = npers.customr_type
# MAGIC WHERE acc.customr_bank_num = 4
# MAGIC AND acc.customr_type = 1
# MAGIC AND acc.aplictn_id in ('ACS', 'VSA')
# MAGIC AND SUBSTRING(acc.ifw_effective_date, 1, 8) <= '20251031'
# MAGIC --AND SUBSTRING(acc.ifw_effective_date, 1, 8) > '20241031' or SUBSTRING(acc.ifw_effective_date, 1, 8) is NULL
# MAGIC AND npers.customr_status = '00'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- 6.5b Method B: COUNT by customr_num ONLY — SMALLER
# MAGIC -- This is what is reported to business.
# MAGIC -- Result: ~16,784,275
# MAGIC -- ============================================================
# MAGIC SELECT COUNT(*) as SD_1 FROM
# MAGIC (
# MAGIC SELECT DISTINCT acc.customr_num
# MAGIC FROM ra_fy_2025.cif_accounts_fy25 acc
# MAGIC JOIN ra_fy_2025.cif_personal_fy25 pers ON acc.customr_num = pers.customr_num AND acc.customr_bank_num = pers.customr_bank_num AND acc.customr_type = pers.customr_type
# MAGIC WHERE acc.customr_bank_num = 4
# MAGIC AND acc.customr_type = 0
# MAGIC AND acc.aplictn_id in ('ACS', 'VSA')
# MAGIC AND SUBSTRING(acc.ifw_effective_date, 1, 8) <= '20251031'
# MAGIC AND pers.customr_status = '00'
# MAGIC UNION
# MAGIC SELECT DISTINCT acc.customr_num
# MAGIC FROM ra_fy_2025.cif_accounts_fy25 acc
# MAGIC JOIN ra_fy_2025.cif_non_personal_fy25 npers ON acc.customr_num = npers.customr_num AND acc.customr_bank_num = npers.customr_bank_num AND acc.customr_type = npers.customr_type
# MAGIC WHERE acc.customr_bank_num = 4
# MAGIC AND acc.customr_type = 1
# MAGIC AND acc.aplictn_id in ('ACS', 'VSA')
# MAGIC AND SUBSTRING(acc.ifw_effective_date, 1, 8) <= '20251031'
# MAGIC --AND SUBSTRING(acc.ifw_effective_date, 1, 8) > '20241031' or SUBSTRING(acc.ifw_effective_date, 1, 8) is NULL
# MAGIC AND npers.customr_status = '00'
# MAGIC )
# MAGIC /*16784275*/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Debug: Reconciliation of Metrics 1.1–1.5 vs 6.5b
# MAGIC
# MAGIC **Known gap**: Sum(1.1–1.5) = 16,737,792 vs 6.5b = 16,784,275 → **variance = 46,483**
# MAGIC
# MAGIC Potential causes:
# MAGIC 1. Metrics 1.1–1.5 count `countDistinct(cust_intrl_id)` — customers with **NULL** `cust_intrl_id` (failed CAEDW join) are excluded
# MAGIC 2. 6.5b Method B counts `DISTINCT customr_num` — deduplicates across personal/non-personal types
# MAGIC 3. Customers may exist in the NACO universe but not match any scoring/rating table bucket

# COMMAND ----------

# ------------------------------------------------------------------
# Debug Step 1: Check how many NACO records have NULL cust_intrl_id
# after the CAEDW join. These would be invisible to metrics 1.1-1.5
# since they all count by cust_intrl_id.
# ------------------------------------------------------------------
null_cust_id_count = niu_naco.filter(col('cust_id').isNull()).select('cust_no', 'cust_type').dropDuplicates().count()
total_niu_naco_count = niu_naco.select('cust_no', 'cust_type').dropDuplicates().count()
print(f"NACO records with NULL cust_id after CAEDW join: {null_cust_id_count}")
print(f"Total distinct NACO records (cust_no + cust_type): {total_niu_naco_count}")

# COMMAND ----------

# ------------------------------------------------------------------
# Debug Step 2: Check how many distinct customr_num appear in BOTH
# personal and non-personal (i.e. duplicated across types).
# This is the diff between Method A and Method B of 6.5b.
# ------------------------------------------------------------------
naco_by_num = naco.select('customr_num').distinct().count()
naco_by_num_type = naco.select('customr_num', 'customr_type').distinct().count()
dual_type_customers = naco_by_num_type - naco_by_num
print(f"Distinct customr_num (Method B / business): {naco_by_num}")
print(f"Distinct customr_num + customr_type (Method A): {naco_by_num_type}")
print(f"Customers appearing in BOTH personal & non-personal: {dual_type_customers}")

# COMMAND ----------

# ------------------------------------------------------------------
# Debug Step 3: Compare the sum of metrics 1.1-1.5 with 6.5b
# Collect all metric counts and print a reconciliation summary.
# ------------------------------------------------------------------
m_1_1 = metric_1_1_count  # already computed above

m_1_2 = contact_center_hrc1.select('cust_intrl_id').distinct().count()
m_1_3 = contact_center_hrc.select('cust_intrl_id').distinct().count()
m_1_4 = contact_center_hrc2.select('cust_intrl_id').distinct().count()
m_1_5 = contact_center_hrc3.select('cust_intrl_id').distinct().count()

metric_sum = m_1_1 + m_1_2 + m_1_3 + m_1_4 + m_1_5

print("=== Reconciliation Summary ===")
print(f"  1.1 (Unscored):       {m_1_1:>12,}")
print(f"  1.2 (Tier 1/2):       {m_1_2:>12,}")
print(f"  1.3 (High):           {m_1_3:>12,}")
print(f"  1.4 (Medium):         {m_1_4:>12,}")
print(f"  1.5 (Low + unscored): {m_1_5:>12,}")
print(f"  ────────────────────────────")
print(f"  Sum(1.1–1.5):         {metric_sum:>12,}")
print(f"  6.5b (Method B):      {naco_by_num:>12,}")
print(f"  Variance:             {naco_by_num - metric_sum:>12,}")
print(f"  NULL cust_id records: {null_cust_id_count:>12,}")

# COMMAND ----------

# ------------------------------------------------------------------
# Debug Step 4: Find NACO customers NOT captured by ANY of 1.1–1.5
# These are the "missing" customers that explain the variance.
# ------------------------------------------------------------------
from functools import reduce

# Collect all cust_intrl_ids from each metric bucket
all_metric_ids = reduce(
    lambda a, b: a.union(b),
    [
        metric_1_1.select(col('cust_no'), col('cust_type').alias('cust_type_mn')),
        contact_center_hrc1.select('cust_no', 'cust_type_mn'),
        contact_center_hrc.select('cust_no', 'cust_type_mn'),
        contact_center_hrc2.select('cust_no', 'cust_type_mn'),
        contact_center_hrc3.select('cust_no', 'cust_type_mn'),
    ]
).dropDuplicates()

# Left-anti join with niu_naco to find who's missing
missing_customers = niu_naco.join(all_metric_ids, on=['cust_no', 'cust_type_mn'], how='left_anti')
print(f"Customers in NACO but NOT in any metric bucket: {missing_customers.count()}")
display(missing_customers.select('cust_no', 'cust_type_mn', 'cust_id').limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## SD2/ABAC — PEP (Politically Exposed Persons) 2025
# MAGIC
# MAGIC Joins the PEP list with the NACO universe to identify how many
# MAGIC contact-centre customers are flagged as PEPs, broken down by PEP type.

# COMMAND ----------

# ------------------------------------------------------------------
# SD2 PEP 2025
#   1. Load the exploded PEP list for 2025
#   2. Filter to CIF-prefixed entities
#   3. Derive cust_no and cust_type_mn
#   4. Inner join with NACO universe
#   5. Count distinct cust_intrl_id (total and non-null PEP_TYPE)
#   6. Summarize by PEP_TYPE
# ------------------------------------------------------------------

# SD2 PEP 2025
pep = spark.table("ra_adido_2025.pep_list_2025_exploded")
pep_cif = pep.filter(col('ENTITY').startswith('CIF'))
pep_cif = pep_cif.withColumn('cust_no', substring(col('ENTITY'), -9, 9)).withColumn('cust_type_mn', when(substring(col("ENTITY"), 8, 1) == "1", "N").otherwise("P"))
display(pep_cif)

naco_pep = pep_cif.join(niu_naco, on = ['cust_no', 'cust_type_mn'], how = 'inner')
naco_pep.agg(countDistinct('cust_intrl_id').alias('SD2')).display()
naco_pep.filter(col("PEP_TYPE").isNotNull()).agg(countDistinct('cust_intrl_id').alias('SD2')).display()

sd2_summary = naco_pep.groupBy("PEP_TYPE").count().orderBy("count", ascending=False)
sd2_summary_notNull = naco_pep.filter(col("PEP_TYPE").isNotNull()).groupBy("PEP_TYPE").count().orderBy("count", ascending=False)

display(sd2_summary)
display(sd2_summary_notNull)
