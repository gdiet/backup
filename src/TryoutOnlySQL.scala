import org.apache.commons.io.FileUtils
import org.testng.annotations.BeforeMethod
import java.io.File
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import scala.io.Source
import net.diet_rich.fdfs._
import net.diet_rich.TestFileTree
import java.util.concurrent.TimeUnit

object TryoutOnlySQL extends App {

  val testfile = new File("temp/TryoutOnlySQL/database")
  testfile.getParentFile.mkdirs
  FileUtils.cleanDirectory(testfile.getParentFile)

  val connection = DBConnection.h2FileDB(testfile)
  TreeSqlDB createTable connection
  val executor = SqlDBCommon.executor(0, 100)
  val tree = new DeferredInsertTreeDB(connection, executor)

  val root = TestFileTree.treeRoot;

  for (i <- 1 to 2000) tree.create(0, "node" + i)
  
  def process(entry: net.diet_rich.TreeEntry, id: Long) : Unit = {
    entry.children.reverse.foreach { node =>
      if (node.timeAndSize isDefined) {
        tree.create(id, node.name, node.timeAndSize.get._1, node.timeAndSize.get._2)
      } else {
        val childId = tree.create(id, node.name)
        process(node, childId)
      }
    }
  }
  
  println("starting...")
//  connection.setAutoCommit(false)
  val time = System.currentTimeMillis
  for (i <- 1 to 5) process(root, i);
  executor.shutdown
  executor.awaitTermination(1, TimeUnit.DAYS)
  println(System.currentTimeMillis - time);
  connection.setAutoCommit(true)
  println("shutting down...")
  
  // 100 times, h2, no constraints, single thread: 6950 ... 7446ms
  
}
