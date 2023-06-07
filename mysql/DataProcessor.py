import configparser

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count, sum, lit, avg, round
from pyspark.sql.types import DecimalType

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("../conf/db.properties")

    MYSQL_DRIVER = config.get("DB", "MYSQL_DRIVER")
    DB_URL = config.get("DB", "DB_URL")
    DB_USER = config.get("DB", "DB_USER")

    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("SparkByExamples.com") \
        .config("spark.jars", "mysql-connector-j-8.0.33.jar") \
        .getOrCreate()

    leaseDetails = spark.read \
        .format("jdbc") \
        .option("driver", MYSQL_DRIVER) \
        .option("url", DB_URL) \
        .option("dbtable", "leaseDetails") \
        .option("user", DB_USER) \
        .option("numPartitions", 5) \
        .option("fetchsize", 20) \
        .load()

    leaseSales = spark.read \
        .format("jdbc") \
        .option("driver", MYSQL_DRIVER) \
        .option("url", DB_URL) \
        .option("dbtable", "leaseSales") \
        .option("user", DB_USER) \
        .option("numPartitions", 5) \
        .option("fetchsize", 20) \
        .load()

    leaseTrans = spark.read \
        .format("jdbc") \
        .option("driver", MYSQL_DRIVER) \
        .option("url", DB_URL) \
        .option("dbtable", "leaseTrans") \
        .option("user", DB_USER) \
        .option("numPartitions", 5) \
        .option("fetchsize", 20) \
        .load()

    leaseSales1 = leaseSales.withColumn(
        "Source",
        when(col("Source_main") != "NULL", col("Source_main"))
        .when(col("Source_sub") != "NULL", col("Source_sub"))
        .otherwise(0)
    )

    leaseSales2 = leaseSales1.groupby(col("L_ID"), col("year")).agg(count(col("L_ID")).alias("ct"),
                                                                    sum(col("Source")).alias("Source"),
                                                                    (12 * sum(col("Source")) / count(
                                                                        col("L_ID"))).alias("pro-rated"))

    leaseTrans1 = leaseTrans.withColumn(
        "Yearly_Rent", (col("Monthly_Rent") * lit(12))
    )

    o1 = leaseDetails.filter("lease_status = 'Current'") \
        .join(leaseTrans1, leaseTrans1["L_ID"] == leaseDetails["L_ID"]) \
        .join(leaseSales2, leaseDetails["L_ID"] == leaseSales1["L_ID"]) \
        .select(leaseDetails["L_ID"].alias("tenant_ID"), leaseDetails["lease_type"], leaseDetails["Category"],
                leaseTrans1["Area_PerSqFT"], round(leaseTrans1["Yearly_Rent"], 2).alias("Annual_Rent"), leaseTrans1["Lease_From"],
                leaseTrans1["Lease_To"],
                leaseSales2["year"], leaseSales2["ct"].alias("No_of_Months"),
                round(leaseSales2["Source"], 2).alias("Sales_in_Months"), round(leaseSales2["pro-rated"], 2).alias("Pro_Rated_Annual_Sales"))

    o2 = leaseTrans1.join(leaseDetails, leaseTrans1["L_ID"] == leaseDetails["L_ID"]) \
        .join(leaseSales1, leaseDetails["L_ID"] == leaseSales1["L_ID"]) \
        .groupby(leaseDetails["Category"]).agg(
        round(avg(leaseTrans1["Area_PerSqFT"]), 2).alias("Area_SqFt_per_Category"),
        round(avg("Yearly_Rent"), 2).cast(DecimalType(18, 2)).alias("Rent_per_Category"),
        round(avg("Source"), 2).cast(DecimalType(18, 2)).alias("Sales_per_Category"))

    o1.write \
        .mode("APPEND") \
        .format("jdbc") \
        .option("driver", MYSQL_DRIVER) \
        .option("url", DB_URL) \
        .option("dbtable", "CurrentTenantYearlySales") \
        .option("user", DB_USER) \
        .save()

    o2.write \
        .mode("APPEND")\
        .format("jdbc") \
        .option("driver", MYSQL_DRIVER) \
        .option("url", DB_URL) \
        .option("dbtable", "CategoryPerformance") \
        .option("user", DB_USER) \
        .save()
