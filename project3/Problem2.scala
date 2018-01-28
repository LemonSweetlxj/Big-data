package comp9313.ass3
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import scala.io.Source

object Problem2
{
  
  def main(args: Array[String]) 
  {
       
    val inputFile = args(0)
    val outputFolder= args(1)
    val conf = new SparkConf().setAppName("Problem2").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    

    //split every line, and regard the first number as key and the second number as value 
    val originalEdge = input.map(line => line.toString.split("\\s+"))
    val pair = originalEdge.map { x => (x(0),x(1)) }
    //swap the key and value(reverse the edge)
    val updateEdge = pair.map(_.swap)
    //reduce by key,and sort the vertex in ascending order
    val oneSource = updateEdge.reduceByKey(_+ "," +_)
    val oneSourceInt = oneSource.map{case (k,v) =>(k.toInt,v)}    
    val order = oneSourceInt.sortBy(_._1, true)
       
    //write the final result to  the required format
    val finalWrite = order.map(line =>line._1.toString() + '\t' + line._2.toString())
    finalWrite.saveAsTextFile(outputFolder)       
  
  }
}
