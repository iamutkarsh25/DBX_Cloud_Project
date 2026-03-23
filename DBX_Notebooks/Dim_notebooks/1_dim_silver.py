# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType, TimestampType,FloatType
from pyspark.sql.functions import *

catalog_name = 'ecommerce'

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_brands")
df_bronze.display(10)

# COMMAND ----------

df_silver = df_bronze.withColumn("brand_name",trim(col('brand_name')))
df_silver.display(10)


# COMMAND ----------

df_silver = df_silver.withColumn("brand_code",regexp_replace(col("brand_code"),r'[^A-Za-z0-9]',''))
df_silver.display(10)

# COMMAND ----------

anamolies = {
    "GROCERY": "GRCY",
    "BOOKS": "BKS",
    "TOYS": "TOY"
}

df_silver = df_silver.replace(anamolies,subset = "catagory_code")
df_silver.select("catagory_code").distinct().display()

# COMMAND ----------

df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema",True)\
    .saveAsTable(f"{catalog_name}.silver.slv_brands")


# COMMAND ----------

# MAGIC %md
# MAGIC Category

# COMMAND ----------

df_category = spark.table(f"{catalog_name}.bronze.brz_category")
df_category.display(10)

# COMMAND ----------

df_grp = df_category.groupBy("category_code").count().filter(col("count")>1)
df_grp.display()

# COMMAND ----------

df_silver = df_category.dropDuplicates(["category_code"])
df_silver = df_silver.withColumn("category_code",upper(col("category_code")))
df_silver.write.format("delta")\
    .option("mergeSchema",True)\
    .mode("overwrite")\
    .saveAsTable(f"{catalog_name}.silver.slv_category")


# COMMAND ----------

df_product = spark.read.table(f"{catalog_name}.bronze.brz_products")

row_count = df_product.count()
column_count = len(df_product.columns)
print(f"Row count: {row_count}, Column count: {column_count}")

# COMMAND ----------

df_product.display()

# COMMAND ----------

df_silver = df_product.withColumn("weight_grams",regexp_replace(col("weight_grams"),"g","").cast(IntegerType()))\
    .withColumn("length_cm",regexp_replace(col("length_cm"),",","."))
df_silver.select("weight_grams","length_cm").display()

# COMMAND ----------

df_silver = df_silver.withColumn(
    "category_code",
    upper(col("category_code"))
).withColumn(
    "brand_code",
    upper(col("brand_code"))
)
df_silver.display(2)

# COMMAND ----------

df_silver = df_silver.withColumn("material",when(col("material")=="Coton","Cotton")
                                 .when(col("material")=="Ruber","Rubber")
                                 .when(col("material")=="Alumium","Aluminium")
                                 .otherwise(col("material"))
                                 )

df_silver = df_silver.withColumn("rating_count",
                                 when(col("rating_count").isNotNull(),abs(col("rating_count")))
                                 .otherwise(lit(0))
                                 )
df_silver.display()

# COMMAND ----------

df_silver.write.format("delta")\
    .option("mergeScehma",True)\
    .mode("overwrite")\
    .saveAsTable(f"{catalog_name}.silver.slv_products")

# COMMAND ----------

# MAGIC %md
# MAGIC Customers

# COMMAND ----------

df_customer = spark.table(f"{catalog_name}.bronze.brz_customers")
df_silver = df_customer.dropna(subset = ["customer_id"])\
    .fillna("N/A", subset= ["phone"])

df_silver.write.format("delta")\
    .option("mergeScehma",True)\
    .mode("overwrite")\
    .saveAsTable(f"{catalog_name}.silver.slv_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC Calender/Date

# COMMAND ----------

df_date = spark.table(f"{catalog_name}.bronze.brz_calendar")
# df_date.display()
df_silver = df_date.withColumn("date",to_date(df_date["date"],"dd-mm-yyyy"))
df_silver = df_silver.dropDuplicates(["date"])\
    .withColumn("day_name",initcap(col("day_name")))\
    .withColumn("week_of_year",abs(col("week_of_year")))

df_silver.display()

# COMMAND ----------

df_silver= df_silver.withColumn("quarter",concat_ws("",lit("Q"),"quarter",lit("-"),"year"))
df_silver = df_silver.withColumn("week_of_year", concat_ws("-", concat(lit("Week"), col("week_of_year"), lit("-"), col("year"))))
df_silver.display()

# COMMAND ----------

df_silver = df_silver.withColumnRenamed("week_of_year","week")
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_calendar")