package net.diet_rich.dedup.core.meta

trait MetaUtils {
  def withEmptyMetaBackend[T] (f: MetaBackend => T): T = {
    val sessionFactory = sql.Testutil.memoryDB
    sql.DBUtilities.createTables("MD5")(sessionFactory.session)
    f(new sql.SQLMetaBackend(sessionFactory))
  }
}
