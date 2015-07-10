package fusewrapper;

import com.sun.jna.Library;
import com.sun.jna.Native;

import fusewrapper.struct.*;

interface FuseTestWrapper extends Library {
    FuseTestWrapper INSTANCE = (FuseTestWrapper) Native.loadLibrary("fusewrapper", FuseTestWrapper.class);

    int fuseWrapperTest_sizeOfFuseOperations();
    int fuseWrapperTest_checkForFuseConnectionInfo(int expectedSize, FuseConnectionInfoReference info);
    int fuseWrapperTest_checkForStat(int expectedSize, StatReference stat);
    int fuseWrapperTest_checkForFuseFileInfo(int expectedSize, FuseFileInfoReference info);
    int fuseWrapperTest_checkForCTypes(int expectedSize, CTypesReference stat);
}
