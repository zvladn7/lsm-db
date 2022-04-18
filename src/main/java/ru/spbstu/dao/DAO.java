package ru.spbstu.dao;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.spbstu.Record;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public interface DAO extends Closeable {

    @NotNull
    Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException;

    @NotNull
    Iterator<Cell> cellIterator(@NotNull ByteBuffer from) throws IOException;

    @NotNull
    default Iterator<Record> range(@NotNull ByteBuffer from,
                                   @Nullable ByteBuffer to) throws IOException {
        if (to == null) {
            return iterator(from);
        }

        if (from.compareTo(to) > 0) {
            return Iters.empty();
        }

        final Record bound = Record.of(to, ByteBuffer.allocate(0));
        return Iters.until(iterator(from), bound);
    }

    @NotNull
    default Value getValue(@NotNull ByteBuffer key) throws IOException, NoSuchElementException {
        final Iterator<Cell> iter = cellIterator(key);
        if (!iter.hasNext()) {
            throw new NoSuchElementException("Not found");
        }

        return iter.next().getValue();
    }

    @NotNull
    default ByteBuffer get(@NotNull ByteBuffer key) throws IOException, NoSuchElementException {
        final Iterator<Record> iter = iterator(key);
        if (!iter.hasNext()) {
            throw new NoSuchElementException("Not found");
        }

        final Record next = iter.next();
        if (next.getKey().equals(key)) {
            return next.getValue();
        } else {
            throw new NoSuchElementException("Not found");
        }
    }

        /**
     * Inserts or updates value by given key.
     */
    void upsert(
            @NotNull ByteBuffer key,
            @NotNull ByteBuffer value) throws IOException;

    /**
     * Removes value by given key.
     */
    void remove(@NotNull ByteBuffer key) throws IOException;

    /**
     * Perform compaction
     */
    default void compact() throws IOException {
        // Implement me when you get to stage 3
    }

}
