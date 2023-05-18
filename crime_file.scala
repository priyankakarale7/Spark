import SparkSessionTest.spark
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object process_crimeFile extends App{
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("ColumnCountDistinctCount")
    .getOrCreate()

  println("Read CSV file as a DataFrame")
  val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("C:\\Users\\INTEL\\Downloads\\atlcrime.csv");

  df.show()


  val resultSchema = List(("column_name", "string"), ("count", "long"), ("distinct_count", "long"))

  val res_schema = StructType(Array(
    StructField("col_name",StringType,true),
    StructField("count",LongType,true),
    StructField("distinct_count",LongType,true)))

  // Create an empty DataFrame
  //val resultDF = spark.createDataFrame(spark.sparkContext.parallelize("", resultSchema))

  // Create an empty Seq to collect the rows
  var collectedRows = Seq.empty[Row]

  df.columns.foreach(println)
  // Iterate over each column
  for (columnName <- df.columns) {

    // Get the count and distinct count of the column
    val columnCount = df.select(columnName).count()
    //val distinctCount = df.select(columnName).groupBy(columnName).count()
    val distinctCount = df.select(columnName).distinct().count()

    // Create a row with the column name, count, and distinct count
    val row = Row(columnName, columnCount, distinctCount)

    //println(row)
    // Append the row to the result DataFrame
    //resultDF.union(spark.createDataFrame(Seq(row), resultSchema))

    // Add the row to the collected rows
    collectedRows = collectedRows :+ row
  }

  // Create a DataFrame from the collected rows and the schema
  val rowRDD = spark.sparkContext.parallelize(collectedRows)
  val res_df = spark.createDataFrame(rowRDD, res_schema)

  res_df.show()



}
