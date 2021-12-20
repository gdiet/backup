package dedup.store

/** Read access to data files should be possible even if they are read-only, e.g. because the user doesn't have write
  * access to the repository.
  *
  * Read access attempts to missing data files must not cause changes in the file system, i.e. must not create any
  * missing parent directories or the data file itself. */
class Req_DataFileReadAccess extends scala.annotation.Annotation
