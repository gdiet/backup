package dedup.server

import java.util.concurrent.atomic.AtomicLong
import java.nio.file.Path

/** Thread safe handler for the mutable contents of a virtual file.
  *
  * @param baseDataId Id of the data record this entry updates. -1 if this entry is independent. */
class DataEntry(val baseDataId: AtomicLong, initialSize: Long, tempDir: Path):
end DataEntry
