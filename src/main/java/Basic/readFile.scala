package Basic

import scala.io.Source

/**
  * Created by jingwang on 3/29/16.
  */
object readFile {

  val filename = "src_cat_features_20160310.csv"
  var featureId = scala.collection.mutable.Map[String,String]()
  var sum =0



  def main(args: Array[String]): Unit = {

      for (line <- Source.fromFile("/Users/jingwang/IdeaProjects/LearnScala/textFiles/src_cat_features_20160310.csv").getLines().drop(1)) {
         var k =line.split(",")(0)
        var v=line.split(",",-1)(1)
        //var k=1.toString
        featureId += (k -> v)


      }
    val value = featureId("148675684215048704")
    println(value)

  }






}
