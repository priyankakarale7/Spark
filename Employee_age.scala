package Joins

import org.apache.spark.sql.SparkSession

class Employee_age {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Employee Data")
    .getOrCreate();

  println("Read CSV file as a DataFrame")
  val df = spark.read.format("csv").option("header", true).option("InferSchema", true).load("C:\\Users\\INTEL\\Downloads\\employees1.csv");

  println("Unique Data of Employee")
  val unique = df.dropDuplicates()
  unique.show()

  unique.createOrReplaceTempView("employee_data");


  val joinDF = spark.sql("select a.empId,  a.empName,  cast((datediff(to_date('19-05-2023', 'dd-MM-yyyy'), a.empDob)/365.25) as int) as age from employee_data a")
  joinDF.show(false)


}
object Employee_age extends App{
  val em = new Employee_age
}