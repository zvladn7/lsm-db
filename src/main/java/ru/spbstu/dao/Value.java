package ru.spbstu.dao;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class Value implements Comparable<Value> {

    private final long timestamp;
    private final ByteBuffer data;

    public Value(final long timestamp,
          final ByteBuffer data) {
        this.timestamp = timestamp;
        this.data = data;
    }

    private Value(final long timestamp) {
        this(timestamp, null);
    }

    public static Value newTombstoneValue(final long timestamp) {
        return new Value(timestamp);
    }

    public boolean isTombstone() {
        return data == null;
    }

    @NotNull
    public ByteBuffer getData() {
        if (data == null) {
            throw new DeletedValueException("Value has been removed!");
        }
        return data.asReadOnlyBuffer();
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return Long.compare(o.timestamp, timestamp);
    }

}
