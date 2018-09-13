package seas.stat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import seas.meta.{Meta, Var}

/** Zählt Häufigkeiten je Wert einer kategorialen RDD-Variable.*/
class Freq(val rows: DataFrame, val v: Var) extends Serializable {

  //assert(idx < rows.first.size, s"Index $idx existiert nicht in $rows")

  lazy val eval =
    rows.map(v(_)).countByValue().toSeq.sortWith(_._2 > _._2)

  def levels=toSeq.map(_._1)

  /* Absteigend sortiert nach Häufigkeit */
  def toSeq:Seq[(Any,Long)]=eval

  def index(level:Any):Int=levels.indexOf(level)

  def size=levels.size

override lazy val toString=s"${toSeq.head}..${toSeq.last}"
}

object Freq {
  def apply(rows:DataFrame, v:Var)=new Freq(rows,v)
  def apply(rows:DataFrame, vars:Meta)=vars.map(v => new Freq(rows,v))
}