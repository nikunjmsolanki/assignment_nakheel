from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit, count, sum, avg, round
from pyspark.sql.types import DecimalType

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("NikunjAssignment") \
        .enableHiveSupport() \
        .getOrCreate()

    leaseSales = spark.sql("select * from assignment.leaseSales").withColumn(
        "Source",
        when(col("Source_main") != "NULL", col("Source_main"))
        .when(col("Source_sub") != "NULL", col("Source_sub"))
        .otherwise(0)
    )

    leaseSales1 = leaseSales.groupby(col("L_ID"), col("year")).agg(count(col("L_ID")).alias("ct"),
                                                                   sum(col("Source")).alias("Source"),
                                                                   (12 * sum(col("Source")) / count(col("L_ID"))).alias(
                                                                       "pro-rated"))

    leaseDetails = spark.sql("select * from assignment.leaseDetail")

    leaseTrans1 = spark.sql("select * from assignment.leaseTrans").withColumn(
        "Yearly_Rent", (col("Monthly_Rent") * lit(12))
    )

    o1 = leaseDetails.filter("lease_status = 'Current'") \
        .join(leaseTrans1, leaseTrans1["L_ID"] == leaseDetails["L_ID"]) \
        .join(leaseSales1, leaseDetails["L_ID"] == leaseSales1["L_ID"]) \
        .select(leaseDetails["L_ID"].alias("tenant_ID"), leaseDetails["lease_type"], leaseDetails["Category"],
                leaseTrans1["Area_PerSqFT"], round(leaseTrans1["Yearly_Rent"], 2).alias("Annual_Rent"),
                leaseTrans1["Lease_From"],
                leaseTrans1["Lease_To"],
                leaseSales1["year"], leaseSales1["ct"].alias("No_of_Months"),
                round(leaseSales1["Source"], 2).alias("Sales_in_Months"),
                round(leaseSales1["pro-rated"], 2).alias("Pro_Rated_Annual_Sales"))

    o2 = leaseTrans1.join(leaseDetails, leaseTrans1["L_ID"] == leaseDetails["L_ID"]) \
        .join(leaseSales1, leaseDetails["L_ID"] == leaseSales1["L_ID"]) \
        .groupby(leaseDetails["Category"]).agg(
        round(avg(leaseTrans1["Area_PerSqFT"]), 2).alias("Area_SqFt_per_Category"),
        round(avg("Yearly_Rent"), 2).cast(DecimalType(18, 2)).alias("Rent_per_Category"),
        round(avg("Source"), 2).cast(DecimalType(18, 2)).alias("Sales_per_Category"))

    o1.write.mode("APPEND").saveAsTable("assignment.CurrentTenantYearlySales")
    o2.write.mode("APPEND").saveAsTable("assignment.CategoryPerformance")

    spark.sql("select * from assignment.CurrentTenantYearlySales").show(1000)
    spark.sql("select * from assignment.CategoryPerformance").show(1000)
