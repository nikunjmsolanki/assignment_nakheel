from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("NikunjAssignment") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS assignment")

    leaseDetail = spark.read.csv("/Users/nikunj/Downloads/DE_Assignment/Data/LeaseDetails/*.csv", header=True)
    leaseSales = spark.read.csv("/Users/nikunj/Downloads/DE_Assignment/Data/LeaseSales/*.csv", header=True)
    leaseTrans = spark.read.csv("/Users/nikunj/Downloads/DE_Assignment/Data/LeaseTrans/*.csv", header=True)

    leaseDetail.write\
        .option("url", "jdbc:hive2://localhost:10000/assignment") \
        .option('driver', 'org.apache.hive.jdbc.HiveDriver') \
        .mode("APPEND").saveAsTable("assignment.leaseDetail")
    leaseSales.write\
        .mode("APPEND").saveAsTable("assignment.leaseSales")
    leaseTrans.write\
        .mode("APPEND").saveAsTable("assignment.leaseTrans")


