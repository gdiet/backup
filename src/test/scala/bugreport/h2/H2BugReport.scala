package bugreport.h2

object H2BugReport extends App {

  import java.sql._
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.Future

  Class forName "org.h2.Driver"
  val url = "jdbc:h2:mem:"
  val connection = DriverManager getConnection (url, "sa", "")
  
  connection.createStatement execute """
    CREATE SEQUENCE treeEntriesIdSeq;
    CREATE TABLE TreeEntries (
      id      BIGINT DEFAULT (NEXT VALUE FOR treeEntriesIdSeq) PRIMARY KEY,
      name    VARCHAR(256) NOT NULL
    );
    CREATE TABLE DataEntries (
      id      BIGINT PRIMARY KEY,
      name    VARCHAR(256) NOT NULL
    );
  """

  val insertTree = connection prepareStatement (
    "INSERT INTO TreeEntries (name) VALUES (?);",
    Statement.RETURN_GENERATED_KEYS
  )

  val insertData = connection prepareStatement (
    "INSERT INTO DataEntries (id, name) VALUES (?, ?);"
  )

  Future {  // comment out this block, and the other block executes fine
    (1 to 50) foreach { n =>
      insertData setLong (1, n)
      insertData setString (2, s"$n")
      insertData executeUpdate
    }
  }

  (1 to 50) foreach { n =>
    println(s"inserting $n in tree")
    insertTree setString (1, s"$n")
    insertTree executeUpdate
    val keys = insertTree.getGeneratedKeys
    keys.next
    keys.getLong(1)
  }
  
}
