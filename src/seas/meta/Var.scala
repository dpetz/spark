/**
 * Created by dpetzoldt on 5/18/15.
 */

package seas.meta

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, StringType, StructField}
import org.apache.spark.sql.Row
import seas.Data

import scala.collection.mutable.ListBuffer


/**
 * Analytical meta data of a [[Data]] variable. Can correspond to a materialized [[DataFrame]] column
 * or be calculated on the fly. Meta such as [[Role]] and [[Level]] are stored in [[attr]].
 *
 *
 * mAnalytischen Metadaten einer Dataframe-Variable z.B. Variablenrolle & -sklaenniveau.
 *
 *
 * Methoden apply oder accessor lesen Wert aus Zeile aus. Kann dynamisch aus
 * anderen Variablen berechnet sein.
 *
 * Variablenreihenfolge folgt aus "column"-Attribut (Erste Position ist 1, nicht 0)
 *
 *
 * customAccessor Liest den Wert der Variablen aus einer Datenzeile aus. Wenn None liest Variable den Wert an
 *                       im DataFrame per Namen aus. Andernfalls wird der übergebene Accessor
 *                       verwendet um z.B. einen Wert dynamisch aus mehreren Row-Einträgen zu berechnen.
 **/
case class Var(name: String, typ:Type, attr:Seq[Attr]) {


  def column:Option[Column]= attr.find(_.isColumn).map(_.asInstanceOf[Column])
  def label:Option[Label]= attr.find(_.isLabel).map(_.asInstanceOf[Label])
  def role:Option[Role]= attr.find(_.isRole).map(_.asInstanceOf[Role])
  def level:Option[Level]= attr.find(_.isLevel).map(_.asInstanceOf[Level])

  require(column.isDefined || attr.find(_.isAccessor).isDefined, "Neither column nor accessor")

  /** If no [[Parser]] provided use [[Parser.double]] for numeric variables and [[Parser.string]] otherwise */
  lazy val parser:Parser = attr.find(_.isParser).map(_.asInstanceOf[Parser]) getOrElse (if (isNumeric) Parser.double else Parser.string)

  /** If no [[Accessor]] provided use [[Accessor.rowByIndex]] based on [[Column]] */
  lazy val accessor:Accessor = attr.find(_.isAccessor).map(_.asInstanceOf[Accessor]) getOrElse Accessor.rowByIndex(column.get.idx)

  def isNumeric = (typ == Type.Num)

  def parse(s: String): Any = parser.parse(s)

  def toField = StructField(name, if (isNumeric) DoubleType else StringType, Var.Nullable)

  /** Nominal, column oder binär */
  def isCategorial = level.map(l => l == Level.Nominal || l == Level.Ordinal|| l == Level.Binary) getOrElse false

  def isNominal = level map (_ == Level.Nominal) getOrElse false
  def isBinary= level map (_ == Level.Binary) getOrElse false
  def isInput =  role map (_ == Role.Input) getOrElse false
  def isTarget =  role map (_ == Role.Target) getOrElse false
  def isId =  role map (_ == Role.Id) getOrElse false

  def apply(r: Row): Any = accessor(r)

  /** Finds column in data frame by name. */
  def column(df: DataFrame) = df(name)

  /** Just this variable's name.
    * @see [[describe]]
    */
  override def toString = name

  /** Long String including name, type and all attributes */
  def describe = s"(Name=$name,Type=$typ,${attr.mkString(",")})"

  /** Create new [[Var]] with modfied [[Type]] and [[Attr]] as specified.
    * @param name new name (required)
    * @param typ new type (if [[None]] copy uses type of this variable
    * @param attr elements will replace attributes during copy if [[Attr.group]] is the same
    * @param clearAttr if no attributes from this variable are copied
    */
  def copy(name: String, typ: Option[Type] = None, attr: Seq[Attr],clearAttr:Boolean=false)={

    def updateAttr(oldAttr:Seq[Attr],newAttr:Seq[Attr])={
      //println(s"Updating $oldAttr with $newAttr")
      import scala.collection.mutable.ListMap
      val joinedAttr = ListMap(oldAttr.map(a => (a.group, a)):_*)
      //println(s"Before Update: $joinedAttr")
      for (kv <- newAttr.map(a => (a.group, a))) {
        joinedAttr += kv // new attributes override old ones
      }
      //println(s"After Update: $joinedAttr")
      joinedAttr.toSeq.map(_._2)
    }

    val newAttr = if (clearAttr) attr else {
      if (attr.isEmpty) this.attr else updateAttr(this.attr,attr)
    }

    new Var(name, typ getOrElse this.typ, newAttr)
  }

  def copy(name:String,typ:Type):Var=copy(name,Option(typ),List())
  def copy(name:String,attr:Seq[Attr]):Var=copy(name,Option(typ),attr)

  /*
  def align(df: DataFrame): Option[Var] = {
    val dfcolumn = df.schema.indexWhere(_.name == name)
    if (dfcolumn == -1) return None
    else Option(
      if (column map (_.idx == dfcolumn) getOrElse false) this
      else copy(name, Vector(Column(dfcolumn+1))))
  }
  */
}

object Var {

  val Nullable = false

  def fromStrings(name: String, typ: String, column:Int = 0,
          label: String = "", role: String = "", level: String = ""):Var ={
    val attr = new ListBuffer[Attr]()
    if (column>0) attr += Column(column)
    if (!label.isEmpty) attr += Label(label)
    if (!role.isEmpty)  attr += Role(role)
    if (!level.isEmpty) attr += Level(level)
    Var.apply(name, Type(typ),attr.toVector)
  }
}