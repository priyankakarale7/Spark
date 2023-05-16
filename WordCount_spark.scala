import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.ListMap
import scala.io.Source


class WordCount_spark {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Read Text File")
    .getOrCreate();

  println(spark.sparkContext.appName);

  val book = spark.read.text("C:\\Users\\INTEL\\OneDrive\\Desktop\\random_txt.txt")

//  val counts = book.
//    flatMap(lines => lines.split("[\\d\\s-,;.!?)(\"]+"))
//    .map(word => (word,1))
  //  .reduceByKey(_+_)
//  val counts = rd.flatMap({
//    lines => lines.
//      //
//  })
//    .map(word => (word,1))
//      //.filter(word => word.matches("[a-zA-Z]+")


  //counts.show();
}
object WordCount_spark extends App{
  val df = new WordCount_spark;
}

case class Reads_Book(filename : String){
  var wordCount = Map[String,Int]();
  var ans = Seq[(String, Int)]();

  private def read(): Unit = {
    println("Reads Book");
    val file = Source.fromFile(filename)
    val lines = file.getLines.toList

    try {
      val words = getWordsByMap(lines)
      wordCount =words.groupBy(identity).mapValues(_.size).view.toMap
      ans = wordCount.toSeq
        .sortWith(_._1 < _._1) // alphabatically sorting a - z
        .sortWith(_._2 > _._2) // sorting by frequency in desc order



    }finally{
      file.close();
    }


  }

  private def getWordsByMap(lines: List[String]): List[String] = {
    lines.flatMap(_.toLowerCase.split("[\\d\\s-,;.!?)(\"]+")
      .filter(word => word.matches("[a-zA-Z]+"))
    )

  }


  def printAllWords(): Unit = wordCount.foreach(i => println(i._1 + " - " + i._2 +" Times"))

  def printAllWords(list : Map[String,Int]) : Unit = list.foreach(i => println(i._1 + " - " + i._2 +" Times"))

  def printInSortedOrder(list : Seq[(String, Int)]) : Unit = list.foreach(println);

  def getTop (top : Int) : Unit = {
    printAllWords(wordCount.take(top));
  }

  def getBottom (bottom : Int) : Unit = {
    printAllWords(wordCount.takeRight(bottom));
  }

  def printWordCount( word : String): Unit = {
    try{
      println(word + " " + wordCount(word) + " Times")
    }catch{
      case e : Exception => println("Word"  + word + "Not Found");
    }
  }


  def printMostUsedWord(): Unit ={
    val value = wordCount.maxBy(_._2);

    val word = value._1;
    val count = value._2;
    println("Most Used Word : "+ word + "-"+ count +" Times")
  }

  def printLeastUsedWord(): Unit ={
    val value = wordCount.minBy(_._2);

    val word = value._1;
    val count = value._2;
    println("Least Used Word : "+ word + "-"+ count +" Times")
  }

  def largestWord() : String = wordCount.maxBy(_._1.length)._1

  def SmallestWord() : String = wordCount.minBy(_._1.length)._1

  read();

}


object Reads_Book extends App{
  val rb = new Reads_Book("/Users/a614734/Downloads/hunger_game.txt");
  println(rb.printMostUsedWord)
  println(rb.printLeastUsedWord)
  //  rb.printAllWords()

  rb.read();
  //println(rb.printInSortedOrder(rb.ans));
  println(rb.SmallestWord())


}
