import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD



class CSV_proccessor {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate();

  val df = spark.read.format("csv").option("header", true).load("C:\\Users\\INTEL\\Downloads\\employees1.csv");

  df.show()

  df.createOrReplaceTempView("employee_data");

  val unique_employee = df.groupBy("empId")


  unique_employee.max("empSal")

  val total_expenditure: DataFrame = spark.sql("select sum(empSal) from employee_data")
  total_expenditure.show()

  spark.sparkContext.setLogLevel("ERROR")

  println("spark read csv files from a directory into RDD")
  val rddFromFile = spark.sparkContext.textFile("C:/tmp/files/text01.csv")
  println(rddFromFile.getClass)

  val rdd = rddFromFile.map(f=>{
    f.split(",")
  })

  println("Iterate RDD")
  rdd.foreach(f=>{
    println("Col1:"+f(0)+",Col2:"+f(1))
  })
  println(rdd)

  println("Get data Using collect")
  rdd.collect().foreach(f=>{
    println("Col1:"+f(0)+",Col2:"+f(1))
  })


}

object ReadMultipleCSVFiles extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("ReadMultipleCSV_files")
    .getOrCreate()


  println("read all csv files from a directory to single RDD")
  val rdd2 = spark.sparkContext.textFile("C:/tmp/files/*")
  rdd2.foreach(f=>{
    println(f)
  })

  println("read csv files base on wildcard character")
  val rdd3 = spark.sparkContext.textFile("C:/tmp/files/text*.csv")
  rdd3.foreach(f=>{
    println(f)
  })

  println("read multiple csv files into a RDD")
  val rdd4 = spark.sparkContext.textFile("C:/tmp/files/text01.csv,C:/tmp/files/text02.csv")
  rdd4.foreach(f=>{
    println(f)
  })

}
