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


//  val total_sal: DataFrame = spark.sql("select sum(empSal) from employee_data")
//  total_sal.show()
// val tot = total_sal.first();
//  println(tot)
//  val total_uni_emp = unique.count()
  //val avg = tot / total_uni_emp;
 // println(total_sal.collect().mkString("Array(", ", ", ")").toInt)


  val averageSalary = unique.select(avg("empSal")).first().getDouble(0)

  val maxSalary = unique.select(max("empSal")).first().getInt(0)
  val highestSalaryRecords = unique.filter(col("empSal") === maxSalary)

  val minSalary = unique.select(min("empSal")).first().getInt(0)
  val lowestSalaryRecords = unique.filter(col("empSal") === minSalary)

  println("Records with Highest Salary:")
  highestSalaryRecords.show()

  println("Records with Lowest Salary:")
  lowestSalaryRecords.show()

  println(s"Average Salary: $averageSalary")



//Using RDD
  println()
  println("spark read csv files from a directory into RDD")
  val rddFromFile = spark.sparkContext.textFile("C:\\Users\\INTEL\\Downloads\\employees1.csv")

  @transient val header = rddFromFile.first()

  println(header)

  val emp_data = rddFromFile.filter(row => row!= header).persist()

  emp_data.foreach(println)

  val uniqueRDD = emp_data.distinct()

  uniqueRDD.foreach(println)

  def processLine(line: String): String = {
    line.split(",")(2)  // Assuming salary is the 3th field (index 2)
  }

  /*
  val rdd = uniqueRDD.map(line => {
    val fields = line.split(",")
    val salary = fields(2).toInt  // Assuming salary is the 3th field (index 5)
    (salary, line)
  })
   */

  val keyValueRDD = uniqueRDD.map(line => (processLine(line), line))

  val total_Salary = keyValueRDD.map(_._1.toLong).sum()
  val num_Records = keyValueRDD.count()
  val avg_Salary = total_Salary / num_Records

  println(s"Average Salary: $averageSalary")

}
