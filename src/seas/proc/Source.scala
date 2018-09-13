package seas.proc


  import seas.Data
  import seas.meta.Meta

  /**
   * Liest Datenzeilen aus csv auf Basis übergebener Metadaten
   */
  case class Source(meta:Meta,pathData:String) extends Proc[Nothing,Data](None)  {
    def run=meta.readCSV(pathData)
    def copy(in:Proc[Nothing,Nothing])=new Source(meta,pathData)
    override def score(d:Proc[Nothing,Data])=d
    override def toSAS=new SASCode()
  }

  object Source {
    def apply(pathMeta:String,pathData:String)=new Source(Meta(pathMeta),pathData)
}
