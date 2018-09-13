package seas.proc

import seas.Data
import seas.meta.{Type, Accessor}

case class MapVar(varName:String, codes:Map[Any,Any])(in:Proc[_,Data]) extends Proc[Data,Data](Option(in)) {

  /* Referenziert während Training und Scoring das Var-Objekt mit dem richtigen Spaltenindex. */
  lazy val oldVar = in.result.meta(varName)

  lazy val newName = Encode.prefix + oldVar.name

  /** Name der erzeugten Variable ist alte Name mit übergebenen Prefix.
    * Der Accessor speichert das Mapping und führt es beim Zugriff dynamisch aus */
  lazy val newVar = {
    val newAttr = Accessor.mapAtAccess(oldVar, codes) +: oldVar.attr.filter(!_.isColumn)
    oldVar.copy(newName, Option(Type.Num), newAttr, true)

  }

  def run = Data(in.result.meta.reject(oldVar) :+ newVar, in.result.df)

  def copy(in: Proc[Nothing, Data]) = new MapVar(varName, codes)(in)

  override def toSAS = {
    val sasCode = in.toSAS procHeader (this)
    val varRef = if (oldVar.isNumeric) oldVar.name else s"trim(${oldVar.name})"
    val valueRef = if (oldVar.isNumeric) { x: Any => x} else { x: Any => s"${'"'}$x${'"'}"}
    sasCode declareNumber newName
    codes.iterator.filter(_._2 != 0).foldLeft(sasCode)(
      (c, kv) => c ++= s"IF $varRef eq ${valueRef(kv._1)} THEN $newName = ${kv._2};\nELSE "
    )
    sasCode ++= s"$newName = 0;\n"
  }
}