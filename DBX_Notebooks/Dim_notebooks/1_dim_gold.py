# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType, TimestampType,FloatType
from pyspark.sql.functions import *
from pyspark.sql import *

catalog_name = 'ecommerce'

# COMMAND ----------

df_gold_brand = spark.table(f"{catalog_name}.silver.slv_brands")
df_gold_category = spark.table(f"{catalog_name}.silver.slv_category")
df_gold_product = spark.table(f"{catalog_name}.silver.slv_products")

# COMMAND ----------

df_gold_product.createOrReplaceTempView("v_products")
df_gold_category.createOrReplaceTempView("v_category")
df_gold_brand.createOrReplaceTempView("v_brands")

# COMMAND ----------

spark.sql(f"Use catalog {catalog_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.gld_dim_products AS
# MAGIC
# MAGIC WITH brand_category as (
# MAGIC   select 
# MAGIC   a.brand_name,
# MAGIC   a.brand_code,
# MAGIC   b.category_code as category_code,
# MAGIC   b.category_name as catagory_name
# MAGIC   from v_brands as a
# MAGIC   inner join v_category as b 
# MAGIC   on a.catagory_code = b.category_code
# MAGIC )
# MAGIC SELECT
# MAGIC   p.product_id,
# MAGIC   p.sku,
# MAGIC   p.category_code,
# MAGIC   COALESCE(bc.catagory_name, 'Not Available') AS category_name,
# MAGIC   p.brand_code,
# MAGIC   COALESCE(bc.brand_name, 'Not Available')   AS brand_name,
# MAGIC   p.color,
# MAGIC   p.size,
# MAGIC   p.material,
# MAGIC   p.weight_grams,
# MAGIC   p.length_cm,
# MAGIC   p.width_cm,
# MAGIC   p.height_cm,
# MAGIC   p.rating_count,
# MAGIC   p.file_name,
# MAGIC   p.ingest_timestamp
# MAGIC FROM v_products p
# MAGIC LEFT JOIN brand_category bc
# MAGIC   ON p.brand_code = bc.brand_code;

# COMMAND ----------

india_region = {
    "MH": "West", "GJ": "West", "RJ": "West",
    "KA": "South", "TN": "South", "TS": "South", "AP": "South", "KL": "South",
    "UP": "North", "WB": "North", "DL": "North"
}
# Australia states
australia_region = {
    "VIC": "SouthEast", "WA": "West", "NSW": "East", "QLD": "NorthEast"
}

# United Kingdom states
uk_region = {
    "ENG": "England", "WLS": "Wales", "NIR": "Northern Ireland", "SCT": "Scotland"
}

# United States states
us_region = {
    "MA": "NorthEast", "FL": "South", "NJ": "NorthEast", "CA": "West", 
    "NY": "NorthEast", "TX": "South"
}

# UAE states
uae_region = {
    "AUH": "Abu Dhabi", "DU": "Dubai", "SHJ": "Sharjah"
}

# Singapore states
singapore_region = {
    "SG": "Singapore"
}

# Canada states
canada_region = {
    "BC": "West", "AB": "West", "ON": "East", "QC": "East", "NS": "East", "IL": "Other"
}

# Combine into a master dictionary
country_state_map = {
    "India": india_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "United Arab Emirates": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}  

# COMMAND ----------

rows = []
for country, states in country_state_map.items():
    for state_code, region in states.items():
        rows.append(Row(country=country, state=state_code, region=region))
rows[:10]        

# COMMAND ----------

df_region_mapping = spark.createDataFrame(rows)

# COMMAND ----------

df_silver = spark.table(f'{catalog_name}.silver.slv_customers')

# COMMAND ----------

df_gold = df_silver.join(df_region_mapping, on=['country', 'state'], how='left')

df_gold = df_gold.fillna({'region': 'Other'})
df_gold.display()

# COMMAND ----------

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_customers")

# COMMAND ----------

df_silver = spark.table(f'{catalog_name}.silver.slv_calendar')

# COMMAND ----------

df_gold = df_silver.withColumn("date_id", date_format(col("date"), "yyyyMMdd").cast("int"))

# Add month name (e.g., 'January', 'February', etc.)
df_gold = df_gold.withColumn("month_name", date_format(col("date"), "MMMM"))

# Add is_weekend column
df_gold = df_gold.withColumn(
    "is_weekend",
    when(col("day_name").isin("Saturday", "Sunday"), 1).otherwise(0)
)

display(df_gold.limit(5))

# COMMAND ----------

desired_columns_order = ["date_id", "date", "year", "month_name", "day_name", "is_weekend", "quarter", "week", "_ingested_at", "_source_file"]

df_gold = df_gold.select(desired_columns_order)

display(df_gold.limit(5))

# COMMAND ----------

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_date")

# COMMAND ----------

