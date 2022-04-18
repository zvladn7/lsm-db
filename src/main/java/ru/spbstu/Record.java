package ru.spbstu;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Objects;

public class Record implements Comparable<Record>  {

    private final ByteBuffer key;
    private final ByteBuffer value;

    private Record(@NotNull final ByteBuffer key,
                   @NotNull final ByteBuffer value) {
        this.key = key;
        this.value = value;
    }

    public static Record of(@NotNull final ByteBuffer key,
                            @NotNull final ByteBuffer value) {
        return new Record(key, value);
    }

    public ByteBuffer getKey() {
        return key;
    }

    public ByteBuffer getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Record record = (Record) o;
        return Objects.equals(key, record.key)
                && Objects.equals(value, record.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public int compareTo(@NotNull final Record other) {
        return key.compareTo(other.key);
    }
}
