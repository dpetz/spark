package seas.proc

import seas.Data
import seas.meta.{Accessor, Type, Var}
import seas.stat.Freq
import org.apache.spark.mllib.tree.RandomForest

/**
 *  Replaces levels of a categorial variables by 0,...,k-1
 *  New variable added and old variable rejected
  *
  * Motivation: Input requirement of [[RandomForest]]
  */
case class Encode(v:Var)(in:Proc[_,Data]) extends Proc[Data,Data](in) {

  lazy val freq=Freq(in.result.df,v)

  /* Vollste Kategorie Default für neue Werte. */
  lazy val codes = Map(freq.levels.zipWithIndex.map{ case (lev,idx) => (lev,idx.toDouble) }:_*).withDefaultValue(0d)

  def cardinality=freq.size

  lazy val model= MapVar(v.name,codes)(in)

  /** Erzeugt und ergänzt Metadaten um neue auf Mapping basierter berechneter Spalte  */
  def run=model.run

  lazy val newVar = model.newVar

  override def score(d:Proc[Nothing,Data])=model.copy(in.score(d))

  def copy(in:Proc[Nothing,Data])=Encode(v)(in)

}

object Encode{
  val prefix="_"
  def apply():((Var,Proc[_,Data])=>Encode)=new Encode(_)(_)
}


