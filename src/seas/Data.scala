/**
 * Created by dpetzoldt on 5/20/15.
 */

package seas


import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame}

import seas.meta.{Meta, Var}
import seas.stat.Freq


/**
 * A pair of [[DataFrame]] and [[Meta]]
 */
class Data (val meta:Meta, val df:DataFrame) extends Serializable {

  /** Counts observations in [[df]] */
  lazy val size = df.count()

  override val toString=s"Data(${size}x${meta.size})"
  
  /** New [[Data]] object with data frame transformed by function. */
  def apply(f:DataFrame=>DataFrame):Data=
    new Data(meta, f(df))

  /** Copy with same data frame and new meta data*/
  def copy(meta:Meta):Data= Data(meta,this.df)

  /** Copy with same meta data and new data frame */
  def copy(rows:DataFrame):Data= Data(this.meta,rows)

  /** Collect RDD and write to CSV*/
  def toCSV(path:String) {
    val lines = df.map((r:Row) => r.toSeq.mkString(",")).collect
    import java.io._
    val bw = new java.io.BufferedWriter(new FileWriter(new File(path)))
    for (l <- lines) bw.write(l+"\n")
    bw.close()
  }

  def freq(varName:String)=Freq(df,meta(varName))


}

object Data {
  def apply(meta:Meta, rows:DataFrame)=new Data(meta,rows)
}