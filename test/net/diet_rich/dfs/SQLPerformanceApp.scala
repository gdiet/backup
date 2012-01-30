package net.diet_rich.dfs

import org.apache.commons.io.FileUtils
import java.io.File
import scala.util.Random
import net.diet_rich.util.sql._
import net.diet_rich.util.data.Bytes

object SQLPerformanceApp extends App {

  val dbdir = new File("temp")
  if (dbdir.exists()) FileUtils.cleanDirectory(dbdir)

//  val dbset = DBSettings("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@localhost:1521:XE", "tcom", "tcom", false)

  val dbset = DBSettings("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:file:temp/testdb", "SA", "", false)
  val dbcon = new DBConnection(dbset)
  val fsset = FSSettings default
  val sqldb = { SqlDB createTables (dbcon, dbset, fsset); new SqlDB(dbcon) }

  val con = dbcon.connection
  val stat = con.createStatement
  val insertDatainfo = con.prepareStatement("INSERT INTO Datainfo (id, size, print, hash, method) VALUES ?, ?, ?, ?, ?")
  val insertTreeEntries = con.prepareStatement("INSERT INTO TreeEntries (id, parent, name, time, dataid) VALUES ?, ?, ?, ?, ?")
  val datasize = 250000
  val sizeFactor = 10
  
  Random.setSeed(0L)
  
  val linked = new collection.mutable.ListBuffer[Long]
  val other = new collection.mutable.ListBuffer[Long]

  stat.executeUpdate("CREATE INDEX TreeEntries_dataid_idx on TREEENTRIES (dataid)")
  
  println("preparing datainfo")
  for (i <- 1 to datasize) {
    val id = Random.nextLong
    if (id % 100 == 7) other.append(id) else linked.append(id)
    setArguments(insertDatainfo, id, 1L, 1L, Bytes(0), 1)
    insertDatainfo.executeUpdate()
  }
  
  println("unlinked elements: " + other.size)

  println("preparing tree entries")
  val time0 = System.currentTimeMillis()
  for (i <- 1 to sizeFactor) {
    for (dataid <- linked) {
      val id = Random.nextLong
      setArguments(insertTreeEntries, id, 0L, ""+id, 1L, dataid)
      insertTreeEntries.executeUpdate()
    }
    println("step " + i + " after " + (System.currentTimeMillis() - time0)/1000 + " seconds.")
  }

  println("unlinked elements: " + other)
  
  println("speed test 1")
  val time1 = System.currentTimeMillis()
  stat.executeQuery("select id FROM DATAINFO WHERE id not in (select dataid from TREEENTRIES)")
  
  println("speed test 2")
  val time2 = System.currentTimeMillis()
  stat.executeQuery("select id from DATAINFO left join TREEENTRIES on DATAINFO.id = dataid where dataid is null")
  
  println("speed test 3")
  val time3 = System.currentTimeMillis()
  stat.executeQuery("select id from DATAINFO where not exists (select 1 from TREEENTRIES where DATAINFO.id = dataid)")
  
  val time4 = System.currentTimeMillis()
  
  println(time2-time1)
  println(time3-time2)
  println(time4-time3)
  
  /*
no indices, 1000 / 50 / 7 elements
+ CREATE INDEX TreeEntries_dataid_idx on TREEENTRIES (dataid)
+  10000 / 50 / 7 elements
+ 250000 / 10 / 7 elements

step 1 after  25 seconds.   25
step 2 after  71 seconds.   46
step 3 after  116 seconds.  45
step 4 after  179 seconds.  63
step 5 after  251 seconds.  72
step 6 after  304 seconds.  53
step 7 after  397 seconds.  93
step 8 after  475 seconds.  78
step 9 after  912 seconds. 437
step 10 after 1189 seconds 277
step 11 after 1693 seconds 504

select id FROM DATAINFO WHERE id not in (select dataid from TREEENTRIES)
656   / 594 / 4821 / 50847(26304)

select id from DATAINFO left join TREEENTRIES on DATAINFO.id = dataid where dataid is null
28673 / 452 / 2840 / 35032(19212)

select id from DATAINFO where not exists (select 1 from TREEENTRIES where DATAINFO.id = dataid)
1466  / 78  / 780  / 25221(12294)
   */
  
}