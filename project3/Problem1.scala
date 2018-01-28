package comp9313.ass3
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._

object Problem1
{
  
  def main(args: Array[String]) 
  {
   

    
    val inputFile = args(0)
    val outputFolder= args(1)
    val topOfK = args(2).toInt
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)
    
    val input = sc.textFile(inputFile)  
    //split the word and get the distinct word in each row
    //write the word to 1
    val words = input.flatMap(line => line.toLowerCase.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+").distinct.filter(_.nonEmpty))
                .map(word => (word, 1))
    
    //reduce the word by key and get the format(word, total number)
    val count = words.reduceByKey(_+_)
    
    //change the place of word and number, sort the result in descending order,
    //and get the format (total number,word)
    val sortedResult = count.map{case(key,value) =>(value,key)}.sortByKey(false,1)
    //after sorting, swap the place again and get top K
    val exchange = sortedResult.map{case(key,value) =>(value,key)}
    val topK = exchange.take(topOfK)

    //write the final result to required format
    val finalWrite = topK.map(line => line._1 + '\t' + line._2)
    val data = sc.parallelize(finalWrite)
    data.saveAsTextFile(outputFolder)    
 
  }
}
