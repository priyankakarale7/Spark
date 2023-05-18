import SparkSessionTest.spark
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
object CSV_using_groupBy extends App{



object process_crimeFile extends App{
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("ColumnCountDistinctCount")
    .getOrCreate()

  println("Read CSV file as a DataFrame")
  val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("C:\\Users\\INTEL\\Downloads\\atlcrime.csv");

  val res_schema = StructType(Array(
    StructField("col_name",StringType,true),
    StructField("count",LongType,true),
    StructField("distinct_count",LongType,true)))
  
  // Create an empty Seq to collect the rows
  var collectedRows = Seq.empty[Row]
  
  // Iterate over each column
  for (columnName <- df.columns) {
    
    val columnCount = df.select(columnName).count()
   
    val uniqueCountDF = df.groupBy().agg(countDistinct(columnName).alias("unique_count"))
    
    val row = Row(columnName, columnCount, uniqueCountDF)
   
    // Add the row to the collected rows
    collectedRows = collectedRows :+ row
  }

  // Create a DataFrame from the collected rows and the schema
  val rowRDD = spark.sparkContext.parallelize(collectedRows)
  val res_df = spark.createDataFrame(rowRDD, res_schema)

  res_df.show()



}

}

+--------------------+------------+
|               crime|count(crime)|
+--------------------+------------+
|LARCENY-FROM VEHICLE|       77345|
| LARCENY-NON VEHICLE|       64697|
|     BURGLARY-NONRES|        8505|
|  ROBBERY-PEDESTRIAN|       14446|
|          AUTO THEFT|       38168|
|            HOMICIDE|         728|
|                RAPE|         990|
|  ROBBERY-COMMERCIAL|        1855|
|   ROBBERY-RESIDENCE|        1880|
|  BURGLARY-RESIDENCE|       42941|
|         AGG ASSAULT|       19133|
+--------------------+------------+

