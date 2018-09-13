package seas.model

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import seas.meta.Var
import seas.proc.Proc
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.tree.configuration.Algo.Classification

/**
 * Wraps [[RandomForestModel]] as [[Proc]]
 *
 * @param model
 * @param features
 * @param in
 */
class ForestModel(val model:RandomForestModel,val features:Array[Var])(in:Proc[_,RDD[Point]]) extends Proc[RDD[Point],RDD[Prediction]](in)  {

  def run= in.result.map { p => Prediction(p.id,p.label, model.predict(p.features)) }

  def predict(features: RDD[Vector])=model.predict(features)
  def numTrees=model.numTrees
  def algo=model.algo
  def totalNumNodes=model.totalNumNodes
  def trees=model.trees
  def isClassification=(algo == Classification)

  lazy val error=Error.collect(result,isClassification)

  def copy(in:Proc[Nothing,RDD[Point]])=new ForestModel(model,features)(in)

  override def toSAS= {

    val code = in.toSAS procHeader this
    val varNames = features.map(_.name)

    for ((t,i) <- model.trees.zipWithIndex) {
      code tree (new Tree(t.topNode), i, varNames)
    }

    val size = model.trees.size
    code sectionHeader "Final Prediction"
    code ++= s"TREES = $size;\n"
    code ++= s"FOREST_MEAN_PROP = SUM(OF TREE_P:) / TREES;\n"
    code ++= s"FOREST_MEAN_CLAS = SUM(OF TREE_C:) / TREES;\n" //( SUM(OF TREE_C:) ge ($size / 2.0) )
    code
  }
}
