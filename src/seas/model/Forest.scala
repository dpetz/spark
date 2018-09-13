package seas.model
import org.apache.spark.mllib.tree.model.RandomForestModel

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import seas.Data
import seas.meta.{Level, Var}
import seas.proc._

import Level._

/**
 * Wraps [[RandomForest]] as [[Proc]]
 */
class Forest(spec:Forest.Spec)(in: Proc[Nothing,RDD[Point]]) extends Proc[RDD[Point],RDD[Prediction]](in) {

  require(in.name == "Points",s"Forest not preceded by Points but ${in.name}")
  val points = in.asInstanceOf[Points]

  def meta=points.in.result.meta

  def trainPoints = points.result.map { _.labeledPoint }

  def binaryTarget:Boolean=
    meta.target.level.get match { // meta.target guarantees unique target
      case Binary =>  true
      case Interval =>  false
      case _ =>  throw new IllegalArgumentException("Target nor binary nor interval but: " + meta.target.role)
    }

  /**
    * Build map of categorial feature cardinaliyt by (je Variable; spätester falls mehrere). ToPoints-Prozess kennt Koordinate im LabeledPoint.
    */
  def categoricalFeaturesInfo:Map[Int,Int] =
    flow.reverse.filter(_.isInstanceOf[Encode]).map( p => {
      val e = p.asInstanceOf[Encode]
      (points.index(e.newVar), e.cardinality)
    }).toMap

  /** Train model */
  def train: RandomForestModel={
    if (binaryTarget)
    RandomForest.trainClassifier(trainPoints,
      2, categoricalFeaturesInfo, spec.numTrees, spec.featureSubsetStrategy, spec.impurity, spec.maxDepth, spec.maxBins)
    else
      RandomForest.trainRegressor(trainPoints,
        categoricalFeaturesInfo, spec.numTrees, spec.featureSubsetStrategy, spec.impurity, spec.maxDepth, spec.maxBins)

  }

  // Speichert trainiertes Modell. Initiiert beim ersten Aufruf Trainingslauf wenn nötig.
  lazy val model = new ForestModel(train,points.features)(in)

  /** Trainiert Modell. Ergebnis ist Prediction der Trainingsdaten */
  def run:RDD[Prediction]=model.run

  lazy val error=Error.collect(result,binaryTarget)

  override def score(d:Proc[Nothing,Data])=model.copy(in.score(d))

  def copy(in: Proc[Nothing,RDD[Point]])=new Forest(spec)(in)

}

object Forest {

  /* Erzeuge Forest mit Defaultparametern (oder überschreiben)*/
  def apply(
    numTrees: Int = 10,
    featureSubsetStrategy: String = "auto",
    impurity: String = "gini",
    maxDepth: Int = 4,
    maxBins: Int = 32 ):Proc[_,RDD[Point]]=>Forest=

    new Forest(Spec(numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins))(_)

  case class Spec(numTrees: Int ,
                  featureSubsetStrategy: String,
                  impurity: String,
                  maxDepth: Int,
                  maxBins: Int)

}