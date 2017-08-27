package net.diet_rich.dedup.meta

trait MetaBackendFactory {
  def initialize(directory: java.io.File, repositoryId: String, hashAlgorithm: String): Unit
  def metaBackend(directory: java.io.File, repositoryId: String): MetaBackend
}
