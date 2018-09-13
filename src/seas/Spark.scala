package seas

/**
 * Created by dpetzoldt on 5/12/15.
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import seas.meta.Meta$

/**
 * Provides the Spark context used by process steps such as [[seas.proc.Source]].
 *
 */
object Spark  {

  var sc:SparkContext = _
  var sql:SQLContext = _

  def +=(sc:SparkContext)={
    this.sc=sc
    this
  }
  def +=(sql:SQLContext)={
    this.sql=sql
    this
  }

  /*
   * Returns the Spark context. Created if
   */
  def context(local:Boolean=true)={
    if (sc == null)
      if (local)
        sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("SEAS"))
      else
        sc = new SparkContext(new SparkConf()
          .setMaster("spark://bipower:7077")
          .setAppName("SEAS")
          .set("spark.executor.memory","10G")
          .set("spark.cores.max","100")
          .setAppName("SEAS"))
    sc
  }



  def sqlContext={
    if (sql == null) sql = new SQLContext(context())
    sql
  }



  /*
  def frame(textFile: String, meta: Vars): DataFrame =
    sqlContext.createDataFrame(
      context.textFile(textFile).map(meta.readLine(_)),
      meta.toSchema)
*/

}

//

// /opt/hadoop/spark/bin/spark-shell --master yarn --driver-cores 10 --executor-cores 15 --num-executors 5 --executor-memory 20G --driver-memory 10G --jars /home/dpetzoldt/2015/SEAS/Scala/out/artifacts/SEAS_jar/SEAS.jar

// zip -r bak/20150608.zip SEAS.iml src
// ~/idea-IC-139.659.2/bin/idea.sh &
// /opt/hadoop/spark/bin/spark-shell
