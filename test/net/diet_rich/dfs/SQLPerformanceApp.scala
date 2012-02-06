package net.diet_rich.dfs

import org.apache.commons.io.FileUtils
import java.io.File
import java.io.FileInputStream
import java.util.zip.GZIPInputStream
import scala.util.Random
import net.diet_rich.util.sql._
import net.diet_rich.util.data.Bytes
import DataDefinitions.TimeAndData

object SQLPerformanceApp extends App {

  def ringIterator(iterable: Iterable[String]): Iterator[String] =
    if(iterable.isEmpty) Nil.iterator else
    new Iterator[String] {
      var prefix = 0
      var iter = iterable iterator
      override def hasNext = true
      override def next : String = {
        if (!iter.hasNext) { iter = iterable.iterator; prefix = prefix + 1 }
        "/" + prefix + iter.next
      }
    }

  val fileRingIterator = {
    val source = io.Source.fromInputStream(new GZIPInputStream(new FileInputStream("test/filelist.gz")))(io.Codec.UTF8)
    ringIterator(source.getLines().toList)
  }
  
  val dbdir = new File("temp")
  if (dbdir.exists()) FileUtils.cleanDirectory(dbdir)

  val dbset = DBSettings("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:file:temp/testdb", "SA", "", false)
//  val dbset = DBSettings("org.h2.Driver", "jdbc:h2:temp/testdb", "SA", "", false)
  val dbcon = new DBConnection(dbset)
  val fsset = FSSettings default
  val sqldb = { SqlDB createTables (dbcon, dbset, fsset); new SqlDB(dbcon) }

  val con = dbcon.connection
  val stat = con.createStatement
  val insertDatainfo = con.prepareStatement("INSERT INTO Datainfo (id, length, print, hash, method) VALUES (?, ?, ?, ?, ?);")
  val datasize = 101000
  val sizeFactor = 50
  
  Random.setSeed(0L)
  
  val linked = new collection.mutable.ListBuffer[Long]
  val other = new collection.mutable.ListBuffer[Long]

  stat.executeUpdate("CREATE INDEX TreeEntries_dataid_idx on TREEENTRIES (dataid)")
//  stat.executeUpdate("CREATE INDEX TreeEntries_parent_idx on TREEENTRIES (parent)") // almost no effect, possibly negative
  
  println("preparing datainfo")
  for (i <- 1 to datasize) {
    val id = Random.nextLong
    if (id % 100 == 7) other.append(id) else linked.append(id)
    setArguments(insertDatainfo, id, 1L, 1L, Bytes(0), 1)
    insertDatainfo.executeUpdate()
  }
  
  println("linked elements: " + linked.size)
  println("other  elements: " + other.size)

  val fscache = new FSDataCache(sqldb)
  val fs : DedupFileSystem = new DedupFileSystem(fscache)
  
  println("preparing tree entries")
  val time0 = System.currentTimeMillis()
  for (i <- 1 to sizeFactor) {
    for (dataid <- linked) {
      val path = fileRingIterator.next
      val id = fs.make(path).get
      fs store (id, TimeAndData(0L, dataid))
    }
    println("step " + i + " after " + (System.currentTimeMillis() - time0)/1000 + " seconds.")
  }

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
Performance (time in ms) of three equivalent queries on hsqldb to find orphan 
data IDs (with "CREATE INDEX TreeEntries_dataid_idx on TREEENTRIES (dataid);") 
for different numbers of elements:

1) select id FROM DATAINFO WHERE id not in (select dataid from TREEENTRIES)
2) select id from DATAINFO left join TREEENTRIES on DATAINFO.id = dataid where dataid is null
3) select id from DATAINFO where not exists (select 1 from TREEENTRIES where DATAINFO.id = dataid)

1) 594 / 4821 / 50847(26304)
2) 452 / 2840 / 35032(19212)
3) 78  / 780  / 25221(12294)

--> 3) performs best on HSQLDB.
   */
  
}