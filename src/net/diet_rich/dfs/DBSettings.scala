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

  val h2memoryDatabase = DBSettings (
    jdbcDriverClassName = "net.sf.log4jdbc.DriverSpy",
    jdbcURL = "jdbc:log4jdbc:h2:mem:test",
    jdbcUser = "SA",
    jdbcPassword = "",
    enableConstraints = true
  )

  // for file database, use
  // jdbcURL = "jdbc:log4jdbc:hsqldb:file:temp/testdb",

  // Oracle driver:   oracle.jdbc.driver.OracleDriver
  // Oracle URL e.g.: jdbc:oracle:thin:@localhost:1521:XE
  
  // h2 driver: org.h2.Driver
  // h2 memory: jdbc:h2:mem:test
  // h2 file:   jdbc:h2:temp/testdb
  // h2 client: jdbc:h2:tcp://localhost/temp/testdb
  
}