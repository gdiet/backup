// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import java.sql.DriverManager
import sys.ShutdownHookThread

class DBConnection(val dbSettings: DBSettings) {
  import dbSettings._
  
  Class forName jdbcDriverClassName
  val connection = DriverManager getConnection(jdbcURL, jdbcUser, jdbcPassword)
  connection setAutoCommit true

  // EVENTUALLY add close() method to close connection without shutdown
  
  // Note: Without explicit shutdown, the last changes may not be written to file!
  ShutdownHookThread {
    // HSQLDB explicit database shutdown:
    // A special form of closing the database is via the SHUTDOWN COMPACT command. 
    // This command rewrites the .data file that contains the information stored 
    // in CACHED tables and compacts it to its minimum size. This command should 
    // be issued periodically, especially when lots of inserts, updates or deletes 
    // have been performed on the cached tables. Changes to the structure of the 
    // database, such as dropping or modifying populated CACHED tables or indexes 
    // also create large amounts of unused file space that can be reclaimed using 
    // this command.
    connection.createStatement execute "SHUTDOWN COMPACT;"
  }
  
}