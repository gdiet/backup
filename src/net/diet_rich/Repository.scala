package net.diet_rich

import java.io.File
import net.diet_rich.fdfs._

object Repository extends App {
  final val dbPath = "db/database"
  final val dataPath = "data"
  
  if (args.length < 1) throw new IllegalArgumentException("Repository needs at least a repository argument.")
  if (args.length > 1) throw new IllegalArgumentException("Too many arguments.")
  create(new File(args(0)))

  def dbConnection(repository: File) = {
    if (!repository.getParentFile.exists) throw new IllegalArgumentException("Directory containing the repository must exist.")
    val dbFile = new File(repository, dbPath)
    dbFile.getParentFile.mkdirs
    DBConnection.h2FileDB(dbFile)
  }

  def exists(repository: File) = new File(repository, dbPath).exists
  
  def create(repository: File): Unit = {
    if (exists(repository)) throw new IllegalArgumentException("Repository already exists.")
    if (!new File(repository, dataPath).mkdirs) throw new IllegalArgumentException("Can't create data path.")
    val connection = dbConnection(repository)
    TreeSqlDB createTable connection
    DataInfoSqlDB createTable (connection, "MD5")
    ByteStoreSqlDB createTable connection
    println("Repository created.")
  }  
}

