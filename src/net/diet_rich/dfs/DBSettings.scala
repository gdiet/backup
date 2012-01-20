package net.diet_rich.dfs

case class DBSettings (
  jdbcDriverClassName: String,
  jdbcURL: String,
  jdbcUser: String,
  jdbcPassword: String,
  enableConstraints: Boolean
)

object DBSettings {
  // HSQLDB without logging: "org.hsqldb.jdbc.JDBCDriver"
  
  // log4jdbc (http://code.google.com/p/log4jdbc) used for JDBC logging.
  // Enable logging: jdbcDriverClassName = "net.sf.log4jdbc.DriverSpy" auto-loads hsqldb driver
  // Enable logging: Add a "jdbc:log4" prefix to the jdbcURL
  
  // Note: As alternative to log4jdbc, jdbcdslog
  // (http://code.google.com/p/jdbcdslog) could be used.
  
  val memoryDatabase = DBSettings (
    jdbcDriverClassName = "net.sf.log4jdbc.DriverSpy",
    jdbcURL = "jdbc:log4jdbc:hsqldb:mem:memdb",
    jdbcUser = "SA",
    jdbcPassword = "",
    enableConstraints = true
  )

  // for file database, use
  // jdbcURL = "jdbc:log4jdbc:hsqldb:file:temp/testdb",

}