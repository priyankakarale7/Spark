import org.apache.spark.sql.{DataFrame, SparkSession}

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


}
