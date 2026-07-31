import java.security.MessageDigest;
import java.util.Arrays;

/**
 * Raw hashing throughput: Java's {@code MessageDigest("MD5")} vs. Rust's
 * {@code blake3} ({@code src/main.rs} in this same directory - the two are
 * meant to be run side by side, same methodology). See this directory's
 * {@code README.md} for how to run both sides and the measured results.
 *
 * <p>Compile once with {@code javac Md5Bench.java}, run the resulting
 * {@code Md5Bench.class} with any Java 21 runtime, on either platform - a
 * compiled class file is not platform-specific.
 */
public class Md5Bench {
    // Large enough that per-call overhead (allocating the digest, JNI/
    // intrinsic call setup, etc.) is negligible relative to the actual
    // hashing work - matches src/main.rs's BUFFER_SIZE exactly, so both
    // sides measure the same thing.
    private static final int BUFFER_SIZE = 128 * 1024 * 1024;
    private static final long WARMUP_DURATION_NANOS = 1_000_000_000L;
    private static final long TEST_DURATION_NANOS = 5_000_000_000L;

    public static void main(String[] args) throws Exception {
        byte[] data = new byte[BUFFER_SIZE];
        Arrays.fill(data, (byte) 0xAB);
        MessageDigest md5 = MessageDigest.getInstance("MD5");

        runFor(WARMUP_DURATION_NANOS, data, md5); // discarded - JIT warm-up

        long[] result = runFor(TEST_DURATION_NANOS, data, md5);
        long totalBytes = result[0];
        long elapsedNanos = result[1];
        double gbPerSecond = totalBytes / 1e9 / (elapsedNanos / 1e9);
        System.out.printf(
            "Java MD5:    %.2f GB/s (%d MiB buffer, %.1f s measured)%n",
            gbPerSecond, BUFFER_SIZE / 1024 / 1024, elapsedNanos / 1e9
        );
    }

    /** Hashes {@code data} in a loop for {@code durationNanos}. Returns
     * {@code {totalBytes, actualElapsedNanos}}. */
    private static long[] runFor(long durationNanos, byte[] data, MessageDigest md5) {
        long start = System.nanoTime();
        long totalBytes = 0;
        int sink = 0;
        while (System.nanoTime() - start < durationNanos) {
            md5.reset();
            byte[] digest = md5.digest(data);
            sink ^= digest[0];
            totalBytes += data.length;
        }
        // Keep the JIT from proving the digest result is unused and
        // eliminating the whole loop - this branch is only ever taken by
        // pure chance (1 in 2^32), never in practice, but referencing
        // `sink` is enough to keep it live.
        if (sink == 0x5a5a5a5a) {
            System.out.println("unreachable in practice: " + sink);
        }
        return new long[] { totalBytes, System.nanoTime() - start };
    }
}
