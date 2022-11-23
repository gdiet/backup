package dedup
package backend

class FileHandlesWriteSpec extends org.scalatest.freespec.AnyFreeSpec with TestFile:

  s"Scenario test for ${°[FileHandlesWrite]}" - {
    val handles = FileHandlesWrite(testFile.toPath)

    "getSize(1) returns None because entry 1 is missing" in {
      assert(handles.getSize(1) === None)
    }

    "addIfMissing(1) returns true because entry 1 is missing" in {
      assert(handles.addIfMissing(1))
    }

    "addIfMissing(1) returns false because entry 1 is already present" in {
      assert(!handles.addIfMissing(1))
    }

    "getSize(1) returns None because entry 1 is empty" in {
      assert(handles.getSize(1) === None)
    }

    "dataEntry(1, _ => 10) returns a data entry with size 10" in {
      assert(handles.dataEntry(1, _ => 10).get.size === 10)
    }

    "dataEntry(1, _ => 12) returns the existing data entry with size 10" in {
      assert(handles.dataEntry(1, _ => !!!).get.size === 10)
    }

    "dataEntry(2, _ => 12) returns None because there is no handle for entry 2" in {
      assert(handles.dataEntry(2, _ => !!!) === None)
    }

//    handles.dataEntry(1, ???).get.write(Iterator(3L -> Array[Byte](1,2,3)))
  }
