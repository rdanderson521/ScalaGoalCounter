import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

// https://stackoverflow.com/questions/24299427/how-do-i-convert-csv-file-to-rdd
class SimpleCSVHeader(header:Array[String]) extends Serializable {
  val index = header.zipWithIndex.toMap
  def apply(array:Array[String], key:String):String = array(index(key))
}

object GoalCounter {

  // args should be the user name and directory
  def main(args: Array[String])
  {
    val hdfsUrl = "hdfs://namenode:8020/user/";
    val username = "rdanderson521"
    val inputPath = hdfsUrl + username + "/goals/input/"
    val outputPath = hdfsUrl + username + "/goals/output/team-goals"

    val conf = new SparkConf().setAppName("GoalCounter")
    val sc = new SparkContext(conf)

    val data = sc.textFile(inputPath)

    val splitData = data.map(line => line.split(",").map(elem => elem.trim) )

    val header = new SimpleCSVHeader(splitData.take(1)(0) )

    // removes header lines
    val filteredData = splitData.filter(line => header(line,"HomeTeam") != "HomeTeam")

    val homeTeamGoals = filteredData.map(line => (header(line,"HomeTeam"), header(line,"FTHG").toInt))
    val awayTeamGoals = filteredData.map(line => (header(line,"AwayTeam"), header(line,"FTAG").toInt))

    val mergedGoals = homeTeamGoals.union(awayTeamGoals)

    val teamGoals = mergedGoals.reduceByKey( (a,b) => a + b )

    teamGoals.saveAsTextFile(outputPath)
  }

}
