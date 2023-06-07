from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("NikunjAssignment") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS assignment")

    leaseDetail = spark.read.csv("../inputFiles/LeaseDetails/*.csv", header=True)
    leaseSales = spark.read.csv("../inputFiles/LeaseSales/*.csv", header=True)
    leaseTrans = spark.read.csv("../inputFiles/LeaseTrans/*.csv", header=True)

    leaseDetail.write\
        .mode("APPEND").saveAsTable("assignment.leaseDetail")
    leaseSales.write\
        .mode("APPEND").saveAsTable("assignment.leaseSales")
    leaseTrans.write\
        .mode("APPEND").saveAsTable("assignment.leaseTrans")


