package dedup;

import java.io.IOException;
import java.util.ArrayList;

// https://stackoverflow.com/questions/58506337/java-byte-array-of-1-mb-or-more-takes-up-twice-the-ram
public class Mem {
    private static Runtime rt = Runtime.getRuntime();
    private static long free() { return rt.maxMemory() - rt.totalMemory() + rt.freeMemory(); }
    public static void main(String[] args) throws InterruptedException, IOException {
        int blocks = 200;
        long initiallyFree = free();
        System.out.println("initially free: " + initiallyFree / 1000000);
        ArrayList<byte[]> data = new ArrayList<>();
        for (int n = 0; n < blocks; n++) { data.add(new byte[1048576]); }
        System.gc();
        Thread.sleep(2000);
        long remainingFree = free();
        System.out.println("remaining free: " + remainingFree / 1000000);
        System.out.println("used: " + (initiallyFree - remainingFree) / 1000000);
        System.out.println("expected usage: " + blocks);
        System.in.read();
    }
}
