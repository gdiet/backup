import org.apache.commons.io.FileUtils
import org.testng.annotations.BeforeMethod
import java.io.File
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import scala.io.Source
import net.diet_rich.fdfs._
import net.diet_rich.TestFileTree

object TryoutOnlySQL extends App {

  val testfile = new File("temp/TryoutOnlySQL/database")
  testfile.getParentFile.mkdirs
  FileUtils.cleanDirectory(testfile.getParentFile)

  val sqlExecutor = SqlDBCommon.executor(0, 100)
  val connection = DBConnection.h2FileDB(testfile)
  
  TreeSqlDB createTable connection
  val tree = new TreeSqlDB()(connection) // with DeferredInsertTreeDB { val executor = sqlExecutor }

  DataInfoSqlDB createTable (connection, "MD5")
  val datainfo = new DataInfoSqlDB()(connection) // with DeferredInsertDataInfoDB { val executor = sqlExecutor }
  
  ByteStoreSqlDB createTable (connection)
  val bytestore = new ByteStoreSqlDB()(connection) // with DeferredByteStoreDB { val executor = sqlExecutor }
  
  val root = TestFileTree.treeRoot;

  for (i <- 1 to 2000) tree.create(0, "node" + i)
  
  def process(entry: net.diet_rich.TreeEntry, id: Long) : Unit = {
    entry.children.reverse.foreach { node =>
      if (node.timeAndSize isDefined) {
        val time = node.timeAndSize.get._1
        val size = node.timeAndSize.get._2
        val fastCheck = datainfo.hasMatchingPrint(size, size*17)
        val dataid = if (fastCheck) {
          val found = datainfo.findMatch(size, size*17, new Array[Byte](0))
          found.getOrElse {
            val dataid = datainfo.reserveID
            
            var sizeCount = size
            bytestore.write(dataid){ range => 
              val oldCount = sizeCount
              sizeCount = math.max(0, sizeCount - range.length)
              if (sizeCount > 0) range.length else oldCount
            }
            
            datainfo.create(dataid, DataInfo(size, size*17, new Array[Byte](0), 0))
            dataid
          }
        } else {
          val dataid = datainfo.reserveID
          
          var sizeCount = size
          bytestore.write(dataid){ range => 
            val oldCount = sizeCount
            sizeCount = math.max(0, sizeCount - range.length)
            if (sizeCount > 0) range.length else oldCount
          }
          
          datainfo.create(dataid, DataInfo(size, size*17, new Array[Byte](0), 0))
          dataid
        }
        tree.create(id, node.name, time, dataid)
      } else {
        val childId = tree.create(id, node.name)
        process(node, childId)
      }
    }
  }
  
  println("starting...")
  val time = System.currentTimeMillis
  for (i <- 1 to 5) process(root, i);
  sqlExecutor.shutdownAndAwaitTermination
  println(System.currentTimeMillis - time);
  println("shutting down...")
  
  // 100 times, h2, tree only, single thread: 6950 ... 7446ms
  // 100 times, h2, tree and datainfo, single thread: 15900 ... 16600ms
  // 100 times, h2, tree and datainfo with finders, single thread: 10900 ... 11200 ms
  
}
