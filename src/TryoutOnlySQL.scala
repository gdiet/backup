import org.apache.commons.io.FileUtils
import org.testng.annotations.BeforeMethod
import java.io.File
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import scala.io.Source
import net.diet_rich.fdfs._
import net.diet_rich.TestFileTree

object TryoutOnlySQL extends App {

  val testDir = new File("temp/TryoutOnlySQL")
  testDir.mkdirs
  FileUtils.cleanDirectory(testDir)

  val connection = DBConnection.h2FileDB(testDir)
  val tree : TreeSqlDB = {
    TreeSqlDB dropTable connection
    TreeSqlDB createTable connection
//    TreeSqlDB addDebuggingConstraints connection // FIXME
    new TreeSqlDB(connection)
  }

  val root = TestFileTree.treeRoot;
  
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
  
  val time = System.currentTimeMillis
  process(root, 0);
  println(System.currentTimeMillis - time);
  
}


object TryoutOnlySQL2 {
  def run1 = {
    
    val testDir = new File("temp/TryoutOnlySQL")
    testDir.mkdirs
    FileUtils.cleanDirectory(testDir)
  
    val connection = DBConnection.h2FileDB(testDir)
    val tree : TreeSqlDB = {
      TreeSqlDB dropTable connection
      TreeSqlDB createTable connection
  //    TreeSqlDB addDebuggingConstraints connection // FIXME
      new TreeSqlDB(connection)
    }
  
    def parentPath(path: String) = path substring (0, path lastIndexOf "/")
    def nameFromPath(path: String) = path substring (1 + path lastIndexOf "/", path length)
    
    val dirs = collection.mutable.Map[String, Long]()
    dirs.put("", 0)
    
    def getOrMakeDir(dir: String) : Long = {
      dirs.getOrElse(dir, {
        val parent = getOrMakeDir(parentPath(dir))
        tree.create(parent, nameFromPath(dir))
      })
    }
    
    var count = 0
    val source = Source.fromInputStream(new GZIPInputStream(new FileInputStream("test/filelist.gz")))
    val time = System.currentTimeMillis
    source.getLines foreach { path =>
  //    if (count < 3) {
      if (System.currentTimeMillis - time < 1000) {
        count = count + 1
  //      println(count + " -> " + path)
        val parent = getOrMakeDir(parentPath(path))
        tree.create(parent, nameFromPath(path))
      }
    }
    source.close
    println(count)
    
  }
}
