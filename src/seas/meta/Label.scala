package seas.meta

/**
 * Created by dpetzoldt on 6/30/15.
 */
case class Label(label:String) extends Attr(label) {
  override def isLabel=true
}

