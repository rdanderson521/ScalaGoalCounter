
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext


object GoalCounter {

  // args should be the paths of the data in the hdfs system
  def main(args: Array[String])
  {
    val hdfsUrl = "hdfs://namenode:8020/user/";
    val username = "rdanderson521";
    val outputPath = "/wc/output/goals-output"

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    if (args.length >= 1)
    {
      var data = sqlContext.read.format("csv").option("header", "true").load(hdfsUrl+username+args(0))
      for( i <- 1 to args.length - 1) {
        val tempData = sqlContext.read.format("csv").option("header", "true").load(hdfsUrl+username+args(i))
        val mergedData = data.union(tempData)
        data = mergedData
      }

      val homeGoalsMapped = data.rdd.map(row => (row.getString(row.fieldIndex("HomeTeam")) -> row.getString(row.fieldIndex("FTHG")) ))
      val awayGoalsMapped = data.rdd.map(row => (row.getString(row.fieldIndex("AwayTeam")) -> row.getString(row.fieldIndex("FTAG")) ))

      val mergedGoalMapped = homeGoalsMapped.union(awayGoalsMapped)

      val teamGoals = mergedGoalMapped.reduceByKey( (a,b) => a + b )

      teamGoals.saveAsTextFile(hdfsUrl+username+outputPath)
    }
  }

}
