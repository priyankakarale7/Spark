import SparkSessionTest.spark
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


class DataFrames {

  val simpleData = Seq(Row("James ","","Smith","36636","M",3000),
    Row("Michael ","Rose","","40288","M",4000),
    Row("Robert ","","Williams","42114","M",4000),
    Row("Maria ","Anne","Jones","39192","F",4000),
    Row("Jen","Mary","Brown","","F",-1)
  )

  val simpleSchema = StructType(Array(
    StructField("firstname",StringType,true),
    StructField("middlename",StringType,true),
    StructField("lastname",StringType,true),
    StructField("id", StringType, true),
    StructField("gender", StringType, true),
    StructField("salary", IntegerType, true)
  ))

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(simpleData),simpleSchema)
  df.printSchema()
  df.show()



  val structureData = Seq(
    Row(Row("James ","","Smith"),"36636","M",3100),
    Row(Row("Michael ","Rose",""),"40288","M",4300),
    Row(Row("Robert ","","Williams"),"42114","M",1400),
    Row(Row("Maria ","Anne","Jones"),"39192","F",5500),
    Row(Row("Jen","Mary","Brown"),"","F",-1)
  )

  val structureSchema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("id",StringType)
    .add("gender",StringType)
    .add("salary",IntegerType)

  val df2 = spark.createDataFrame(
    spark.sparkContext.parallelize(structureData),structureSchema)
  df2.printSchema()
  df2.show()



  val updatedDF = df2.withColumn("OtherInfo",
    struct(  col("id").as("identifier"),
      col("gender").as("gender"),
      col("salary").as("salary"),
  )).drop("id","gender","salary")
  updatedDF.printSchema()
  updatedDF.show(false)

}


