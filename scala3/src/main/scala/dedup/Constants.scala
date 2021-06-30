package dedup

/** Internal size limit for byte arrays. Set to less than 0.5 MiB to avoid problems with humongous objects in G1GC
  * and to a power of 2 so blocks align well if the fuse layer uses blocks sized a smaller power of 2.
  *
  * The fuse default for write is 4kiB, for read is 128kiB. For DedupFS write is also set to 128kiB
  * using the option "big_writes" and "max_write=131072". So read and write automatically partitions
  * data into small-enough chunks. TODO check actual read and write chunk size
  *
  * @see https://stackoverflow.com/questions/58506337/java-byte-array-of-1-mb-or-more-takes-up-twice-the-ram */
val memChunk = 2 << 18

val hashAlgorithm = "MD5"
