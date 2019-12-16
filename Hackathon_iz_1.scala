package org.inceptez.spark.sqlhacthon

import org.apache.spark.sql.SQLContext;;
import org.apache.spark._;
import org.apache.spark.sql.SparkSession
import java.util.Date
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.functions._
import org.inceptez.hack
import org.inceptez.hack.allmethods

case class InsuranceData(IssuerId: Int, IssuerId2: Int, BusinessDate: String, StateCode: String, SourceName: String, NetworkName: String, NetworkURL: String, custnum: String, MarketCoverage: String, DentalOnlyPlan: String);

object IZTest {
  def main(args: Array[String]) {

    //Output Directory Configuration
    val inDirPath = "hdfs://localhost:54310/user/hduser/sparkhack2/"
    val outDirPath = "hdfs://localhost:54310/user/hduser/sparkhack2/Ram/"

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("LoadInsuranceDataApplication")
    val sc = new SparkContext(sparkconf);

    sc.setLogLevel("ERROR");

    //************************** Task 1 -Reading insuranceinfo1.csv **********************/
    println("******************* Task 1 - Creating RDD by reading insuranceinfo1 CSV File ******************")
    //Point - 1
    val insuranceRdd = sc.textFile(inDirPath + "insuranceinfo1.csv");

    //Point - 2
    val headerData_RDD = insuranceRdd.first();
    val withOutHeaderData_RDD = insuranceRdd.filter { row => row != headerData_RDD };

    //Point - 3 and 4
    val withOutEmptyRowData_RDD = withOutHeaderData_RDD.filter { row => !row.isEmpty() };

    //Point - 5
    val splitData_RDD = withOutEmptyRowData_RDD.map(row => row.split(",", -1))

    //Point - 6
    val validData_RDD = splitData_RDD.filter(row => !row(0).trim().isEmpty() && !row(1).trim().isEmpty()
      && !row(2).trim().isEmpty() && !row(3).trim().isEmpty()
      && !row(4).trim().isEmpty() && !row(5).trim().isEmpty()
      && !row(6).trim().isEmpty() && !row(7).trim().isEmpty()
      && !row(8).trim().isEmpty() && !row(9).trim().isEmpty())

    //Point - 7
    val finalTyped_RDD = validData_RDD.map(rowArray => {

      InsuranceData(rowArray(0).trim().toInt, rowArray(1).trim().toInt, rowArray(2).trim(), rowArray(3).trim(), rowArray(4).trim(), rowArray(5).trim(), rowArray(6).trim(), rowArray(7).trim(), rowArray(8).trim(), rowArray(9).trim())
    })

    val finalUnTyped_RDD = validData_RDD.map(rowArray => {

      rowArray(0) + "," + rowArray(1) + "," + rowArray(2) + "," + rowArray(3) + "," + rowArray(4) + "," + rowArray(5) + "," + rowArray(6) + "," + rowArray(7) + "," + rowArray(8) + "," + rowArray(9)
    })

    //Point - 8
    println("Total rows from the insuranceinfo1 file - " + withOutHeaderData_RDD.count().toString())
    println("Total valid data rows in insuranceinfo1 file - " + finalTyped_RDD.count().toString())
    println("Total rows removed in clean up process in insuranceinfo1  file - " + (withOutHeaderData_RDD.count() - finalTyped_RDD.count()).toString())

    //Point-9
    val rejectData = withOutEmptyRowData_RDD.subtract(finalUnTyped_RDD)

    //********************** Task 1 - Reading insuranceinfo2.csv *******************************

    println("******************* Task 1 -Creating RDD by reading insuranceinfo2 CSV File ******************")
    //Point - 1
    val insuranceRDD_1 = sc.textFile(inDirPath + "insuranceinfo2.csv");

    //Point - 2
    val headerData_RDD_1 = insuranceRDD_1.first();
    val withOutHeaderData_RDD_1 = insuranceRDD_1.filter { row => row != headerData_RDD_1 };

    //Point - 3 and 4
    val withOutEmptyRowData_RDD_1 = withOutHeaderData_RDD_1.filter { row => !row.isEmpty() };

    //Point - 5
    val splitData_RDD_1 = withOutEmptyRowData_RDD_1.map(row => row.split(",", -1))

    //Point - 6
    val validData_RDD_1 = splitData_RDD_1.filter(row => !row(0).trim().isEmpty() && !row(1).trim().isEmpty()
      && !row(2).trim().isEmpty() && !row(3).trim().isEmpty()
      && !row(4).trim().isEmpty() && !row(5).trim().isEmpty()
      && !row(6).trim().isEmpty() && !row(7).trim().isEmpty()
      && !row(8).trim().isEmpty() && !row(9).trim().isEmpty())

    //Point - 7
    val finalTyped_RDD_1 = validData_RDD_1.map(rowArray => {

      InsuranceData(rowArray(0).trim().toInt, rowArray(1).trim().toInt, rowArray(2).trim(), rowArray(3).trim(), rowArray(4).trim(), rowArray(5).trim(), rowArray(6).trim(), rowArray(7).trim(), rowArray(8).trim(), rowArray(9).trim())
    })

    val finalUnTyped_RDD_1 = validData_RDD_1.map(rowArray => {

      rowArray(0) + "," + rowArray(1) + "," + rowArray(2) + "," + rowArray(3) + "," + rowArray(4) + "," + rowArray(5) + "," + rowArray(6) + "," + rowArray(7) + "," + rowArray(8) + "," + rowArray(9)
    })

    //Point - 8
    println("Total rows from the insuranceinfo2 file - " + withOutHeaderData_RDD_1.count().toString())
    println("Total valid data rows in insuranceinfo2 file - " + finalTyped_RDD_1.count().toString())
    println("Total rows removed in clean up process in insuranceinfo2 file - " + (withOutHeaderData_RDD_1.count() - finalTyped_RDD_1.count()).toString())

    //Point - 9
    val rejectData_1 = withOutEmptyRowData_RDD_1.subtract(finalUnTyped_RDD_1)

    // ********************* Task-2 ***************************************/

    //Point -12
    val insureddatamerged = finalUnTyped_RDD.union(finalUnTyped_RDD_1)

    //Point -13
    insureddatamerged.cache()

    //Point - 14
    println("*************************Task 2 - Data Merging Validation*********************************")
    println("Total valid data rows in insuranceinfo1 file - " + finalTyped_RDD.count().toString())
    println("Total valid data rows in insuranceinfo2 file - " + finalTyped_RDD_1.count().toString())
    println("Total merged valid data rows - " + insureddatamerged.count().toString())

    //Point - 15
    val withOutDuplicates = insureddatamerged.distinct();
    println("Total No of duplicate rows  - " + (insureddatamerged.count() - withOutDuplicates.count()).toString())

    //Point - 16
    val insuredatarepart = withOutDuplicates.repartition(8)

    //TODO: Need to change this implementation 
    //Point-17
    val data_RDD_2019_10_01 = insuredatarepart.filter { x => x.contains(",2019-10-02,") }
    val data_RDD_2019_10_02 = insuredatarepart.filter { x => x.contains(",2019-10-02,") }

    //Point-18
    rejectData.coalesce(1).saveAsTextFile(outDirPath + "rejectDataFile1")
    rejectData_1.coalesce(1).saveAsTextFile(outDirPath + "rejectDataFile2")
    insureddatamerged.coalesce(1).saveAsTextFile(outDirPath + "MergedData")
    data_RDD_2019_10_01.coalesce(1).saveAsTextFile(outDirPath + "20191001")
    data_RDD_2019_10_02.coalesce(1).saveAsTextFile(outDirPath + "20191002")

    // ********************* Task-3 ***************************************/
    println("******************* Task 3 - Data Frame Operations ******************")
    //Point -19 and 20
    var insuranceSchema = StructType(Array(StructField("IssuerId", IntegerType, true),
      StructField("IssuerId2", IntegerType, true),
      StructField("BusinessDate", DateType, true),
      StructField("StateCode", StringType, true),
      StructField("SourceName", StringType, true),
      StructField("NetworkName", StringType, true),
      StructField("NetworkURL", StringType, true),
      StructField("custnum", StringType, true),
      StructField("MarketCoverage", StringType, true),
      StructField("DentalOnlyPlan", StringType, true)))

    val spark = SparkSession.builder().appName("IZ Test app").master("local[*]").getOrCreate();
    val insureDataRowRdd = insuredatarepart.map { x => Row(x) }
    val insuredatarepartDF = spark.createDataFrame(insureDataRowRdd, insuranceSchema);

    //Point - 21
    val insuranceInfo1DF = spark.read.option("header", "true").option("delimiter", ",").option("escape", ",").option("mode", "dropmalformed").schema(insuranceSchema).csv(inDirPath + "insuranceinfo1.csv")
    val insuranceInfo2DF = spark.read.option("header", "true").option("delimiter", ",").option("escape", ",").option("mode", "dropmalformed").schema(insuranceSchema).csv(inDirPath + "insuranceinfo2.csv")
    //insuranceInfo1DF.show(10);

    //Point - 22
    val insuranceInfo1DF_ColRenamed = insuranceInfo1DF.withColumnRenamed("SourceName", "srcnm").withColumnRenamed("StateCode", "stcd");
    val insuranceInfo2DF_ColRenamed = insuranceInfo2DF.withColumnRenamed("SourceName", "srcnm").withColumnRenamed("StateCode", "stcd")

    val insuranceInfo1DF_ColAdded = insuranceInfo1DF_ColRenamed.withColumn("issueridcomposite", concat(col("IssuerId").cast("String"), lit(" "), col("IssuerId2").cast("String")));
    val insuranceInfo2DF_ColAdded = insuranceInfo2DF_ColRenamed.withColumn("issueridcomposite", concat(col("IssuerId").cast("String"), lit(" "), col("IssuerId2").cast("String")));

    val insuranceInfo1DF_ColDrop = insuranceInfo1DF_ColAdded.drop(col("DentalOnlyPlan"))
    val insuranceInfo2DF_ColDrop = insuranceInfo2DF_ColAdded.drop(col("DentalOnlyPlan"))

    val insuranceInfo1DF_ColAdded_1 = insuranceInfo1DF_ColDrop.withColumn("sysdt", current_date()).withColumn("systs", current_timestamp());
    val insuranceInfo2DF_ColAdded_1 = insuranceInfo2DF_ColDrop.withColumn("sysdt", current_date()).withColumn("systs", current_timestamp());

    //Point - 23
    val insuranceInfo1DF_Valid = insuranceInfo1DF_ColAdded_1.na.drop("any");
    val insuranceInfo2DF_Valid = insuranceInfo2DF_ColAdded_1.na.drop("any");

    //Point - 24 and 25
    val obj = new allmethods();

    def remspecialchar(inputString: String): String = {

      inputString.replaceAll("[^a-zA-Z0-9]", " ")

    }

    //Point - 26
    val dsludf = udf(remspecialchar(_: String): String)
    val insuranceInfo1DF_Final = insuranceInfo1DF_Valid.withColumn("NetworkName", dsludf(col("NetworkName")))
    val insuranceInfo2DF_Final = insuranceInfo2DF_Valid.withColumn("NetworkName", dsludf(col("NetworkName")))

    //Point - 27
    insuranceInfo1DF_Final.write.format("json").mode(SaveMode.Overwrite).save(outDirPath + "processedInfo1JSON")
    insuranceInfo2DF_Final.write.format("json").mode(SaveMode.Overwrite).save(outDirPath + "processedInfo2JSON")

    //Point - 28
    insuranceInfo1DF_Final.write.format("csv").option("header", "true").option("delimiter", "~").mode(SaveMode.Overwrite).save(outDirPath + "processedInfo1CSV")
    insuranceInfo2DF_Final.write.format("csv").option("header", "true").option("delimiter", "~").mode(SaveMode.Overwrite).save(outDirPath + "processedInfo2CSV")

    //Point - 29 Todo

    // ********************* Task-4 ***************************************/
    println("******************* Task 4 - Handling RDDs and Tempviews ******************")
    //Point - 30
    val custs_states_RDD = sc.textFile(inDirPath + "custs_states.csv");

    //Point - 31
    val custs_states_Split_RDD = custs_states_RDD.map { x => x.split(",") }
    val custs_Info_RDD = custs_states_Split_RDD.filter(col => col.size == 5)
    val state_Code_RDD = custs_states_Split_RDD.filter(col => col.size == 2)

    val custfilter_RDD = custs_Info_RDD.map(col => {
      col(0) + "," + col(1) + "," + col(2) + "," + col(3) + "," + col(4)
    })

    val statesfilter_RDD = state_Code_RDD.map(col => {
      col(0) + "," + col(1)
    })

    //Point - 32
    val custstatesDF = spark.read.option("delimiter", ",").option("escape", ",").csv(inDirPath + "custs_states.csv").toDF("Col1", "Col2", "Col3", "Col4", "Col5")

    //Point - 33
    val custfilterDF = custstatesDF.na.drop(Array("Col3", "Col4", "Col5")).withColumnRenamed("Col1", "custid").withColumnRenamed("Col2", "FirstName").withColumnRenamed("Col3", "LastName").withColumnRenamed("Col4", "age").withColumnRenamed("Col5", "profession")
    val statesfilterDF = custstatesDF.filter("Col3 is null and Col4 is null and Col5 is null").drop("Col3", "Col4", "Col5").withColumnRenamed("Col1", "stated").withColumnRenamed("Col2", "statedesc")

    //Point-34
    custfilterDF.createOrReplaceTempView("custView")
    statesfilterDF.createOrReplaceTempView("statesView")

    //Point - 35
    insuranceInfo1DF_Valid.createOrReplaceTempView("insureInfo1View")
    insuranceInfo2DF_Valid.createOrReplaceTempView("insureInfo2View")

    //Point - 36
    spark.sqlContext.udf.register("removeSpecialCharacter", remspecialchar(_: String): String)

    //Point - 37
    val finalinsuranceInfo1DF = spark.sql("select insureInfo1View.*,removeSpecialCharacter(NetworkName) as cleannetworkname,current_date() as curdt,current_timestamp() as curts,year(BusinessDate) as yr,month(BusinessDate) as mth,case when locate('http' , NetworkURL) > 0 THEN 'http' else 'noprotocol' end as protocol,statesview.statedesc,custview.age,custview.profession from insureInfo1View  inner join statesView on insureInfo1View.stcd = statesView.stated inner join custView on insureInfo1View.custnum = custView.custid  ")
    val finalinsuranceInfo2DF = spark.sql("select insureInfo2View.*,removeSpecialCharacter(NetworkName) as cleannetworkname,current_date() as curdt,current_timestamp() as curts,year(BusinessDate) as yr,month(BusinessDate) as mth,case when locate('http' , NetworkURL) > 0 THEN 'http' else 'noprotocol' end as protocol,statesview.statedesc,custview.age,custview.profession from insureInfo2View  inner join statesView on insureInfo2View.stcd = statesView.stated inner join custView on insureInfo2View.custnum = custView.custid  ")

    //Point - 38
    finalinsuranceInfo1DF.write.mode(SaveMode.Overwrite).option("compression","none").parquet(outDirPath + "parquetOutputInfo1")
    finalinsuranceInfo2DF.write.mode(SaveMode.Overwrite).option("compression","none").parquet(outDirPath + "parquetOutputInfo2")

    //Point - 39
    val insureInfo1DF = finalinsuranceInfo1DF.groupBy("statedesc", "protocol", "profession").agg(avg("age").as("AverageAge"), count("age").as("Count"))
    val insureInfo2DF = finalinsuranceInfo1DF.groupBy("statedesc", "protocol", "profession").agg(avg("age").as("AverageAge"), count("age").as("Count"))
    insureInfo1DF.union(insureInfo2DF).show()

    //Point - 40
    val mysqlURL = "jdbc:mysql://localhost/custdb";
    val mysqltbl = "insureInfo"
    val mysqlUserName = "root"
    val mysqlPwd = "root"

    /*
    insureInfo1DF.write.format("jdbc").option("driver","com.mysql.jdbc.Driver")
    .option("url",mysqlURL)
    .option("dbtable", mysqltbl)
    .option("user", mysqlUserName)
    .option("password", mysqlPwd).mode(SaveMode.Append).save();*/
  }
}