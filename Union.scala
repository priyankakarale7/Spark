import org.apache.spark.sql.SparkSession


class Union {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Spark-Union")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val Data = Seq(("James","Sales","NY",90000,34,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000)
    )
    val df = Data.toDF("employee_name","department","state","salary","age","bonus")
    df.printSchema()
    df.show()

    val Data2 = Seq(("James","Sales","NY",90000,34,10000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000)
    )
    val df2 = Data2.toDF("employee_name","department","state","salary","age","bonus")
    df2.show(false)

  println("Using Union")
    val unionDF = df.union(df2)
    unionDF.show(false)
  println("After removing duplicates")
    unionDF.distinct().show(false)

  println("Using UnionAll")
    val unionAllDF = df.unionAll(df2)
    unionAllDF.show(false)

}

object Union extends App{
  val un = new Union
}