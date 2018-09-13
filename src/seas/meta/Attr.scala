package seas.meta

abstract class Attr(id:Option[Any]) extends Serializable {

  def this(id:Any)=this(Option(id))

  def isRole=false
  def isLevel=false
  def isColumn=false
  def isLabel=false
  def isAccessor=false
  def isParser=false


  /** Variable must contain no more than one attribute per group */
  lazy val group= {
      if (isRole) Role
      else if (isLevel) Level
      else if (isColumn) Column
      else if (isLabel) Label
      else if (isAccessor) Accessor
      else if (isParser) Parser
    }.getClass.getName

  override def toString= id map (_.toString) getOrElse getClass.getSimpleName

}













