// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

private[webdav] object FileSystemSingleton {

  private val notInitialized = Left("file system not yet initialized")
  private var repositoryPath: Either[Error, String] = notInitialized
  private var storeMethod: Either[Error, Int] = notInitialized
  
  def configure(initialRepositoryPath: String, initialStoreMethod: Int): Either[Error, FileSystem] = 
    if (repositoryPath.isRight || storeMethod.isRight) 
      Left("file system already initialized")
    else {
      this.repositoryPath = Right(initialRepositoryPath)
      this.storeMethod = Right(initialStoreMethod)
      maybeRepository
    }

  private lazy val maybeRepository: Either[Error, FileSystem] = for {
    repositoryPath <- repositoryPath.right
    storeMethod <- storeMethod.right
    fileSystem <- FileSystem(repositoryPath, storeMethod).right
  } yield fileSystem

  lazy val repository: FileSystem = maybeRepository.right.get
  
}
