from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark =  SparkSession.builder.appName("Consumer_Complaints_ETL").getOrCreate()
df= spark.read.format("csv").option("header","true").option("inferSchema","true").load(r"D:\Spark_Project\Consumer Complaint Database\customer_complaint_dataset.csv")
df.show()

#Data De-duplication - Remove duplicate rows to ensure data quality
print(f"Before de-duplication: {df.count()} records")
df_clean= df.dropDuplicates()
print(f"After de-duplication: {df_clean.count()} records")

#Drop Rows Having All Null Values
df_clean= df_clean.dropna('all')

#Check Null Values in Each Column - Count missing or empty values column-wise
null_summary = df_clean.select([
    F.count(F.when(F.col(c).isNull() | (F.col(c) == ''), c)).alias(c)
    for c in df_clean.columns
])
null_summary.show()
df_clean = df_clean.fillna({
    "State": "Not Mentioned State",
    "ZIP code": "Not Provided" ,
    "Sub-product": "Sub-Product Information Missing",
    "Sub-issue": "Sub-Issue Not Mentioned",
    "Company public response": "No Public Response Provided from company",
})

df_clean.show(5)

null_summary = df_clean.select([
    F.count(F.when(F.col(c).isNull() | (F.col(c) == ''), c)).alias(c)
    for c in df_clean.columns
])
null_summary.show()

# Cache DataFrame for Performance Optimization
df_clean.cache()

# Repartition Data for Better Parallelism
print("Before repartition:", df_clean.rdd.getNumPartitions())
df_clean = df_clean.repartition(3)
print("After repartition:", df_clean.rdd.getNumPartitions())

#Save Cleaned Data as Parquet File
df_clean.write.mode("overwrite").parquet(
   r"C:\Users\user\Downloads\aggregates\Clean_data"
)

# Create the Template For Sql Operations
df_clean.createOrReplaceTempView("consumer_complaints")

# #which states have the highest number of complaints.
agg_by_state_sql1 = spark.sql("""
    SELECT
        State,
        COUNT(*) AS Total_Complaints
    FROM consumer_complaints
    GROUP BY State
    ORDER BY Total_Complaints DESC
""")
agg_by_state_sql1.show()
# To save agg.1 Data Into Parquet File
agg_by_state_sql1.coalesce(1).write.mode("overwrite").parquet(
  r"C:\Users\user\Downloads\aggregates\agg_by_state"
)
# To save agg.1 Data Into Csv Format
agg_by_state_sql1.write.mode("overwrite").option("header", True).option("inferSchema","true").csv(r"D:\Spark_Files_csv\agg_by_state")

#Analyze company responses based on timeliness.
agg_by_company_sql2= spark.sql("""SELECT 
    Company,
    SUM(CASE WHEN `Timely response?` = 'Yes' THEN 1 ELSE 0 END) AS timely_count,
    SUM(CASE WHEN `Timely response?` = 'No' THEN 1 ELSE 0 END) AS delayed_count
FROM consumer_complaints
GROUP BY Company
ORDER BY timely_count DESC;
""")
agg_by_company_sql2.show()
# To save agg.2 Data Into Parquet File
agg_by_company_sql2.coalesce(1).write.mode("overwrite").parquet(
   r"C:\Users\user\Downloads\aggregates\company_response"
)
# To save agg.2 Data Into Csv Format
agg_by_company_sql2.write.mode("overwrite").option("header", True).option("inferSchema","true").csv(r"D:\Spark_Files_csv\company_response")

#Find unique products each company deals with.
Unique_Products = spark.sql("""
    SELECT
        Company,
        COLLECT_SET(Product) AS Unique_Products
    FROM consumer_complaints
    GROUP BY Company
""")
Unique_Products.show()
# To save agg.3 Data Into Parquet File
Unique_Products.coalesce(1).write.mode("overwrite").parquet(
   r"C:\Users\user\Downloads\aggregates\agg_by_Product"
)

#Identify companies receiving the most consumer complaints.
agg_by_Company_sql4 = spark.sql("""
    SELECT Company,
        COUNT(*) AS Max_consumer_complaint_received
        FROM consumer_complaints
    GROUP BY Company;
""")
agg_by_Company_sql4.show()
# To save agg.4 Data Into Parquet File
agg_by_Company_sql4.coalesce(1).write.mode("overwrite").parquet(
   r"C:\Users\user\Downloads\aggregates\Max_consumer_complaint_received"
)
# To save agg.4 Data Into Csv Format
agg_by_Company_sql4.write.mode("overwrite").option("header", True).option("inferSchema","true").csv(r"D:\Spark_Files_csv\Max_consumer_complaint_received")

# Analyze Maximum Issues raised under each product category.
agg_by_product_sql5 = spark.sql("""
    SELECT 
        Product AS Grouping_Product,
        MAX(Issue) AS Max_Issues,
        COUNT(*) AS Total_Issues
    FROM consumer_complaints
    GROUP BY Product
    ORDER BY Total_Issues DESC;
""")

agg_by_product_sql5.show(20)
# To save agg.5 Data Into Parquet File
agg_by_product_sql5.write.mode("overwrite").parquet(
   r"C:\Users\user\Downloads\aggregates\Total_issues"
)
# To save agg.5 Data Into Csv Format
agg_by_product_sql5.write.mode("overwrite").option("header", True).option("inferSchema","true").csv(r"D:\Spark_Files_csv\Total_issues")





