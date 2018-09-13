package seas.proc

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import seas.meta.{Var,Accessor}
import seas.Data
import seas.model.Point

/**
 * Erwartet genau eine Zielgröße und mindestens einen Input.
 */
case class Points (in:Proc[Nothing,Data]) extends Proc[Data,RDD[Point]](in)  {

  /** For convenience insert encoders if none in [[flow]] already */
  lazy val last =
    // Todo: Assert all factors encoded (not just one).
    // Todo: Raw data already encoded?
    if (!in.flow.exists(_.name == "Encode")) {
      val factors = in.result.meta.inputs.nominals
      in -> Each(factors,Encode())
    } else in

  override def sub=linearSub(last)

  /* Protected */
   lazy val mapper:PointMapping = {
    val inMeta = last.result.meta
    assert (inMeta.inputs.size > 1, "No inputs:" + inMeta)
    PointMapping(inMeta.inputs.map(_.name).toArray, Option(inMeta.target.name),inMeta.id.name)(last)
  }

  def features = mapper.featureVars
  def label = mapper.labelVar.get
  def run = mapper.run

  override def score(d:Proc[Nothing,Data])=
    new PointMapping(mapper.features,mapper.label,mapper.id)(last.score(d))

  def index(v:Var)=features.indexWhere(_.name == v.name)

  def copy(in:Proc[Nothing,Data])=Points(in)

  /**
   * @param features Variables sorted for LabeledPoint
   */
  case class PointMapping(features: Array[String],
                   label:Option[String],id:String) (in:Proc[_,Data]) extends Proc[Data,RDD[Point]] (in) {
    
    lazy val featureVars = features map (in.result.meta(_))

      /** Falls keien Variable dieses Namens None
        * (für Scoring mit oder ohne Zielvariable)
        */
    lazy val labelVar = label.map(
      l => in.result.meta.find(l == _.name)
    ) getOrElse None

    val accessors = featureVars map (_.accessor)
    
    val idAccessor = in.result.meta(id).accessor

    /** Sets label to 0 if undefined (for scoring)*/
    val labelAccessor = labelVar.map(_.accessor) getOrElse Accessor.constant(0)


    /* idAccessor(r), labelAccessor.dbl(r) */
    def run= in.result.df.map { r => Point(0,0, Vectors.dense(accessors map (_ dbl r)))}

    def copy(in:Proc[Nothing,Data])=PointMapping(features,label,id)(in)

    override def toSAS=in.toSAS

  }
}
