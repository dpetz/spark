package seas.model

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import seas.meta.Role

/**
 * A [[LabeledPoint]] with an [[Role.Id]]. Optional Label
 */
case class Point(id:Any,label:Double,features:Vector) extends Serializable {
  def labeledPoint=new LabeledPoint(label,features)
}
