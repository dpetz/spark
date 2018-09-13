package seas.meta


import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType
import seas.{Spark, Data}

import scala.io.Source


/** Set of  [[Var]] objects representing the analytical metadata of an RDD or DataFrame.
  * Multiple data objects can share the same meta object (eg. train and test data, or full data and sample)
  * May contain just a subset of columns in a data object.
  * Instances are immutable. All subsetting methods return copies.
  * Moderiert das Parsen einer RDD[Row] aus Textfiles und
  * Erzeugen eines DataFrame aus der RDD.
  */
class Meta (val vars:Vector[Var])
  extends Traversable[Var] with Serializable {

  /* Target variables only */
  def targets=Meta(vars filter (_.isTarget))

  def target=uniqueByRole(Role.Target)
  def id=uniqueByRole(Role.Id)

  def uniqueByRole(r:Role)={
    val v = vars.filter( v => v.role.map(_ == r) getOrElse false )
    assert(v.size==1,s"No unique variable with role $r. Found: ${v.size}")
    v.head

  }

  /* Copy with Input variables */
  def inputs=Meta(vars filter (_.isInput))

  /** Nur kategorialen Variablen, siehe Var.isCategorial */
  def categorials=Meta(vars.filter(_.isCategorial))

  /** Nominal scaled variables only. */
  def nominals=Meta(vars.filter(_.isNominal))

  /* Binary scaled variables only */
  def binaries =Meta(vars.filter(_.isBinary))

  /** Helper for [[byLevel]] and others */
  private def by[T](f:Var=>T):Map[T,Meta]=
    vars.groupBy(f(_))
      .mapValues(Meta(_))

  /* Groups variable by level. */
  def byLevel:Map[Option[Level],Meta]=  by(_.level)

  /* Groups variables by type */
  def byType:Map[Type,Meta]=by(_.typ)

  /** Find variable by name. Error if no match. */
  def apply(name:String):Var=vars.find(_.name==name).get

  override def toString=s"(${targets.size} targets, ${inputs.size} inputs)"

  /** Sortiert nach Reihenfolge ("ordinal"). */
  def ordered=Meta(vars.filter(_.column.isDefined).sortWith { _.column.get.col < _.column.get.col })

  /** Parsing a csv row based on variable ordering and types.
    * @param l csv data line
    */
  def readLine(l:String)=
    Row.fromSeq(l.split(",").zip(ordered.vars).map(x => x._2.parse(x._1)))

  def readCSV(textFile:String)=
    Data(this,Spark.sqlContext.createDataFrame(
      Spark.context().textFile(textFile).map(readLine(_)), toSchema
    ))

  /** Makes this object a [[Traversable]] of its [[Var]] objects. */
  def foreach[U](f: Var => U)=vars.foreach(f)

  def toSchema=StructType(ordered.vars.map(_.toField))

  /* Modifikationen */

  /** Rolle einer Variablen auf Rejected setzen  */
  def reject(v:Var):Meta=Meta(vars.updated(
    vars.indexOf(v),
    v.copy(v.name,Array(Role.Rejected))
  ))

  def :+(v:Var)=Meta(vars :+ v)
  // Todo: Column Index überprüfen

  /** Spaltenindex um 1 größer als bisher größter Index */
  //def appendIndex=vars.maxBy(_.index).index + 1

    /**
     * Erzeut zum DataFrame passendes Meta-Objekt, d.h.
     * behält nur Variablen, die auch DataFrame sind, und stellt sicher dass v.ordinal
     * für alle Variablen der Sortierung im DataFrame entspricht.
     */

  //def align(df:DataFrame)=Meta(vars.map( _.align(df) ).flatten)

}



/** Factory zum Erzeugen von Var-Objekten */
object Meta {

  val header = "name,column,type,label,role,level"

  /** Wrapped die übergebenen Variablenliste als Vars-Objekt */
  def apply(vars:Vector[Var])=new Meta(vars)

  /** Erzeugt Vars Objekt aus übergebenen csv.
    * Struktur und erste Zeile müssen wie header sein. */
  def apply(src:Source):Meta={
    val lines = src.getLines
    assert(lines.next.toLowerCase==header,
      "Metadaten-Strukur ist nicht "+header)

    // Todo: Werte mit Kommata
    def parseLine(line:String):Var={
      val row = line.replaceAll(","," , ").split(',').map(_.trim)
      Var.fromStrings(row(0),row(2),row(1).toInt,row(3),row(4),row(5))
    }
    new Meta(lines.toVector.map(parseLine))
  }



  /** Wie apply(src:Source), aber mit Pfad als String. */
  def apply(metaFile:String):Meta = apply(Source.fromFile(metaFile))

}


/*
import seas.meta._
val v = Var("x",Type.Num,Vector(Role.Input,Column(1)))
val meta = Meta(Vector(v))
meta.reject(v)("x").describe
 */