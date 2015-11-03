import java.io.File

object parser {
  case class Config(input: File = new File("1"),
                    combo: File = new File("."),
                    //                    output: File = new File("data"),
                    appName: String = "Cube Build",
                    yyyymm: String = "",
                    master: String = "")

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("Spark Cube") {
      head("Spark Cube", "1.x")
      opt[File]('i', "input") required() valueName ("<Data File>") action {
        (x, c) => c.copy(input = x)
      } text ("The data file")

      opt[File]('c', "combo") required() valueName ("<Combo File>") action {
        (x, c) => c.copy(combo = x)
      } text ("The combo file")

      opt[String]('m',"master") required() valueName("<Master>") action {
        (x,c) => c.copy(master = x)
      } text ("The Spark Master URL")

      opt[String] ('y', "yyyymm") required() valueName("<YYYYMM>") action {
        (x,c) => c.copy(yyyymm = x)
      } text("The Start month and year")

      opt[String] ('n', "name") required() valueName("<AppName>") action {
        (x,c) => c.copy(appName = x)
      } text("The Spark App Name")
      note("")
      help("help") text ("prints this usage text")
    }
    // parser.parse returns Option[C]
    parser.parse(args, Config()) match {
      case Some(config) =>
      // do stuff

      case None =>
      // arguments are bad, error message will have been displayed
    }
  }
}
