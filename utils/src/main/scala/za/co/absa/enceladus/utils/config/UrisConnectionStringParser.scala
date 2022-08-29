package za.co.absa.enceladus.utils.config

object UrisConnectionStringParser {

  private val hostsRegex = """^\s*http(?:s)?://([^\s]+?)(?:/[^\s]*)?\s*$""".r

  def parse(connectionString: String): List[String] = {
    connectionString
      .split(";")
      .flatMap(expandHosts)
      .map(_.trim
        .replaceAll("/$", "")
        .replaceAll("/api$", "")
      )
      .distinct
      .toList
  }

  private def expandHosts(multiHostUrl: String): Array[String] = {
    multiHostUrl match {
      case hostsRegex(hosts) =>
        hosts.split(",").map { host =>
          multiHostUrl.replace(hosts, host)
        }
      case _ =>
        throw new IllegalArgumentException("Malformed connection string")
    }
  }

}
