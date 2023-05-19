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

Output:

+-----+---------------+------+----------+----------+
|empId|        empName|empSal|    empDob|    empDoj|
+-----+---------------+------+----------+----------+
|    4|   Samantha Lee| 90000|1978-12-25|2005-07-01|
|    5| Michael Jordan|100000|1975-05-25|2000-07-15|
|   17|    Steven Chen| 95000|1985-02-01|2011-06-01|
|   11|      James Lee| 70000|1990-08-21|2014-06-01|
|    9|     David Chen| 90000|1983-06-30|2009-11-01|
|    3|    Bob Johnson| 75000|1982-03-10|2008-12-01|
|   14| Michelle Brown| 67000|1980-07-23|2004-09-01|
|   19|   Daniel Brown| 70000|1990-10-18|2013-12-01|
|   22|      Emily Kim| 90000|1984-12-25|2008-08-01|
|    1|       John Doe| 50000|1990-01-01|2015-01-01|
|   13| Robert Johnson| 72000|1994-11-12|2019-05-01|
|   10|       Sara Kim| 95000|1981-01-10|2007-08-01|
|    2|     Jane Smith| 60000|1985-06-15|2010-05-01|
|   16|      Karen Lee| 86000|1988-03-15|2012-10-01|
|   23|Thomas Anderson| 77000|1993-09-12|2019-01-01|
|    8|     Emma Davis| 75000|1987-12-07|2013-01-01|
|    6|    Kate Wilson| 80000|1989-02-12|2012-10-01|
|   24|  Nicole Taylor| 67000|1981-03-30|2006-07-01|
|   15|     Adam Smith| 72000|1991-06-10|2014-07-01|
|   21|   Ryan Johnson| 84000|1987-11-02|2016-04-01|
+-----+---------------+------+----------+----------+
only showing top 20 rows

+-----+---------------+---+
|empId|empName        |age|
+-----+---------------+---+
|4    |Samantha Lee   |44 |
|5    |Michael Jordan |47 |
|17   |Steven Chen    |38 |
|11   |James Lee      |32 |
|9    |David Chen     |39 |
|3    |Bob Johnson    |41 |
|14   |Michelle Brown |42 |
|19   |Daniel Brown   |32 |
|22   |Emily Kim      |38 |
|1    |John Doe       |33 |
|13   |Robert Johnson |28 |
|10   |Sara Kim       |42 |
|2    |Jane Smith     |37 |
|16   |Karen Lee      |35 |
|23   |Thomas Anderson|29 |
|8    |Emma Davis     |35 |
|6    |Kate Wilson    |34 |
|24   |Nicole Taylor  |42 |
|15   |Adam Smith     |31 |
|21   |Ryan Johnson   |35 |
+-----+---------------+---+
only showing top 20 rows
