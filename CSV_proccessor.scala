import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD

import java.time.LocalDate



/*
1. Read Employee from csv file with 4 column {empId, empName, empSal, empDob, empDoj}

2. Remove the duplicate records

3. Sort it based on empDoj

4. Iterate and print the data


1)Read the file

2)display distinct records

3)sorted records using DOJ and DOB

4) highest , lowest, avg salary

5) employee above avg and below avg salary
 */


case class Employee(empId: Int, empName: String, empSal: Double, empDob: LocalDate, empDoj: LocalDate)

class CSV_proccessor {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate();

  println("Read CSV file as a DataFrame")
  val df = spark.read.format("csv").option("header", true).option("InferSchema", true).load("C:\\Users\\INTEL\\Downloads\\employees1.csv");

  df.show()

  df.createOrReplaceTempView("employee_data");

  //display distinct records
  println("Unique Data of Employee")
  val unique = df.dropDuplicates()
  unique.show()


  //sorted records using DOJ and DOB
  val sorted = unique.sort("empDoj", "empDob")

  sorted.show()

  //val unique_employee = df.groupBy("empId")



  //unique_employee.max("empSal")

  // highest , lowest, avg salary
  val total_sal: DataFrame = spark.sql("select sum(empSal) from employee_data")
  total_sal.show()
  val tot = total_sal.toString().toInt
  println(tot)
  val total_uni_emp = unique.count()
  //val avg = total_sal["sum(empSal)"]/ total_uni_emp;
  println(total_sal.collect().mkString("Array(", ", ", ")").toInt)



  println("spark read csv files from a directory into RDD")
  val rddFromFile = spark.sparkContext.textFile("C:\\Users\\INTEL\\Downloads\\employees1.csv")
  println(rddFromFile.getClass)

  val rdd = rddFromFile.map(f=>{
    f.split(",")
  })

  val employees = rdd.map(row => Employee(
    row(0).toInt,
    row(1),
    row(2).toDouble,
    LocalDate.parse(row(3)),
    LocalDate.parse(row(4))
  ))

  println("Iterate RDD")
  rdd.foreach(println)

