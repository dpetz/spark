package seas.proc

import org.apache.spark.rdd.RDD
import seas.Data
import seas.model.{ForestModel, Prediction,Error}

/**
 * Enables extracting and running scorecode from the specified training process.
 */
case class Score(train:Proc[Nothing,RDD[Prediction]])(in: Proc[_,Data]) extends  Proc[Data,RDD[Prediction]](in){
  val scoreflow=train.score(in)
  def model=in.asInstanceOf[ForestModel] // todo: Generalize for other models
  def run=scoreflow.result
  def copy(in: Proc[Nothing,Data])=Score(train)(in)
  def error=Error.collect(result,model.isClassification)
  override def toSAS=scoreflow.toSAS
}
