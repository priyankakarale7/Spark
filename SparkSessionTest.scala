
//
import org.apache.spark.sql.SparkSession

object SparkSessionTest extends App{
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate();
/*
  println("First SparkContext: 1")
  println("APP Name :"+spark.sparkContext.appName);
  println("Deploy Mode :"+spark.sparkContext.deployMode);
  println("Master :"+spark.sparkContext.master);
*/
  val sparkSession2 = SparkSession.builder()
    .master("local[1]")
    .appName(" Spark ")
    .getOrCreate();

  /*
  println("Second SparkContext: 2")
  println("App Name :"+sparkSession2.sparkContext.appName);
  println("Deploy Mode :"+sparkSession2.sparkContext.deployMode);
  println("Master :"+sparkSession2.sparkContext.master);

  */

  //Create RDD from parallelize
  val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
  val col = Seq("language","users_count");
  val rdd = spark.sparkContext.parallelize(dataSeq)

  //rdd to dataframe
  import spark.implicits._
  val dfFromRDD1 = rdd.toDF("Language","Fees")
  dfFromRDD1.show();
  dfFromRDD1.printSchema()



  //Create RDD from external Data source
  val rdd2 = spark.sparkContext.textFile("C:\\Users\\INTEL\\OneDrive\\Desktop\\random_txt.txt")

//  rdd.foreach(println)
//
//  rdd2.foreach(println)


  //transformations on RDD’s are flatMap(), map(), reduceByKey(), filter(), sortByKey()

  // Some actions on RDD’s are count(),  collect(),  first(),  max(),  reduce()

  // Create a dataframe


  val data = Seq(("James","","Smith","1991-04-01",'M',3000),
  ("Michael","Rose","","2000-05-19","M",4000),
  ("Robert","","Williams","1978-09-05","M",4000),
  ("Maria","Anne","Jones","1967-12-01","F",4000),
  ("Jen","Mary","Brown","1980-02-17","F",-1)
  )

  val columns = Seq("firstname","middlename","lastname","dob","gender","salary")

//val df = rd.toDF();
//  df.printSchema();
////  df.show()
 val obj = new CSV_proccessor;


}
