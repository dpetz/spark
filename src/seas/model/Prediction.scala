package seas.model

case class Prediction(id:Any, target:Double, prediction:Double) extends Serializable


/**
 * A [[Data]] object containing a [[Id]], [[Target]] and [[Prediction]].
 * Caclulates error statistics.
 * Assumes [[Level.Binary]] or [[Level.Interval]] target.
 * If target is binary assumes target values 1 for events and 0 for non-events.

class Prediction(meta:Meta, df:DataFrame) extends Data(meta,df) {

  val target=meta.target
  val id=meta.id
  val prediction=meta.uniqueByRole(Prediction)

  lazy val error:Error = {

    val trg = target.accessor.dbl(_)
    val p = prediction.accessor.dbl(_)

    if (target.isBinary)
      df.map(r => Error(trg(r),p(r),1,0)).reduce(_ + _)
    else
      df.map(r => Error(trg(r),p(r))).reduce(_ + _)

  }

object Predictions {
  /**
  * @param df DataFrame with three columns with the semantic (in this order): [[Id]], [[Target]], [[Prediction]]
  */
  def apply(rdd:RDD)={

    val meta = Meta(
    Var("ID",Type.String)


    )

  }

}

}

 */
