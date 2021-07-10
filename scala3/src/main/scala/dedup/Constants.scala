package dedup

/** Internal size limit for byte arrays.
  *
  * Set to a power of 2 so blocks align well if the fuse layer uses blocks sized a smaller power of 2.
  * Set to less than 0.5 MiB to avoid problems with humongous objects in G1GC.
  * Set to 64 kiB or less to avoid a big array issue
  * Long story short, 2 << 14 (that is 2^15 or 32 kiB) is a good value.
  *
  * The fuse default for write is 4kiB, for read is 128kiB. For DedupFS write is also set to 128kiB
  * using the option "big_writes" and "max_write=131072". So read and write automatically partitions
  * data into small-enough chunks. TODO check actual read and write chunk size
  *
  * @see https://stackoverflow.com/questions/58506337/java-byte-array-of-1-mb-or-more-takes-up-twice-the-ram
  * @see https://stackoverflow.com/questions/68331703/java-big-byte-arrays-use-more-heap-than-expected */
val memChunk = 2 << 14 // TODO add check at startup that simply allocates 90% of the free memory with chunks

val hashAlgorithm = "MD5"
