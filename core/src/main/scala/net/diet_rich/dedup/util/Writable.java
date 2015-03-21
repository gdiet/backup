package net.diet_rich.dedup.util;

public enum Writable {
    readOnly,
    readWrite;

    public static Writable fromBooleanString(String string) {
        return string.equals("true") ? readWrite : readOnly;
    }
}
