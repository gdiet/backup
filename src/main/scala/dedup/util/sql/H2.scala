package dedup.util.sql
import java.sql.DriverManager

object H2 {
  val driver = "org.h2.Driver"
  def memoryUrl = s"jdbc:h2:mem:dedupfs"
  val defaultUser = "sa"
  val defaultPassword = ""
  Class forName driver

  def singleMemoryConnection = new SingleConnection(() =>
    // By default, new connections are in auto-commit mode.
    DriverManager.getConnection(memoryUrl, defaultUser, defaultPassword)
  )
}
