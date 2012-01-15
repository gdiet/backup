package net.diet_rich.dfs

class DBSettings {
  // log4jdbc (http://code.google.com/p/log4jdbc) used for JDBC logging.
  
  // HSQLDB without logging: "org.hsqldb.jdbc.JDBCDriver"
  // Enable logging: "net.sf.log4jdbc.DriverSpy" auto-loads hsqldb driver
  val jdbcDriverClassName = "net.sf.log4jdbc.DriverSpy"

  // HSQLDB without logging: "jdbc:hsqldb:file:temp/testdb"
  // Enable logging: Add a "jdbc:log4" prefix to the URL
  val jdbcURL = "jdbc:log4jdbc:hsqldb:file:temp/testdb"
  val jdbcUser = "SA"
  val jdbcPassword = ""
    
  // Note: As alternative to log4jdbc, jdbcdslog
  // (http://code.google.com/p/jdbcdslog) could be used.
    
  val enableConstraints = true
}