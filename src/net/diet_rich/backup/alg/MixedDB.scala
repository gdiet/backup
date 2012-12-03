package net.diet_rich.backup.alg

trait MixedDB {
  /** @return The node's complete data information if any. */
  def fullDataInformation(id: Long): Option[FullDataInformation]
}
