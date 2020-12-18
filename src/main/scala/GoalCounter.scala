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
    // default directory parameters
    val hdfsUrl = "hdfs://namenode:8020/user/";
    val username = "rdanderson521"
    var inputPath = hdfsUrl + username + "/goals/input/"
    var outputPath = hdfsUrl + username + "/goals/output/team-goals"

    // overwrites default directory if args supplied
    if (args.length == 2){
      inputPath = args(0)
      outputPath = args(1)
    }

    // sets up spark context
    val conf = new SparkConf().setAppName("GoalCounter")
    val sc = new SparkContext(conf)

    // reads all files from input directory
    val data = sc.textFile(inputPath)

    // splits the csv and removes trailing whitespace
    val splitData = data.map(line => line.split(",").map(elem => elem.trim) )

    // sets the header
    val header = new SimpleCSVHeader(splitData.take(1)(0) )

    // removes header lines by checking if the text in the "HomeTeam" column is equal to "HomeTeam"
    val filteredData = splitData.filter(line => header(line,"HomeTeam") != "HomeTeam")

    // maps the tuples with team names and their goals
    val homeTeamGoals = filteredData.map(line => (header(line,"HomeTeam"), header(line,"FTHG").toInt))
    val awayTeamGoals = filteredData.map(line => (header(line,"AwayTeam"), header(line,"FTAG").toInt))

    // merges away and home team tuples
    val mergedGoals = homeTeamGoals.union(awayTeamGoals)

    // reduces the tuples by key, adding the values of the teams with the same name
    val teamGoals = mergedGoals.reduceByKey( (a,b) => a + b )

    // saves the output to the output dir
    teamGoals.saveAsTextFile(outputPath)
  }

}
