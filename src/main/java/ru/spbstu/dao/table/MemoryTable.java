package ru.spbstu.dao.table;

import org.jetbrains.annotations.NotNull;
import ru.spbstu.dao.Cell;
import ru.spbstu.dao.Value;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
public class MemoryTable implements Table {

    private final SortedMap<ByteBuffer, Value> map;
    private final AtomicInteger bytes;

    public MemoryTable() {
        this.map = new ConcurrentSkipListMap<>();
        this.bytes = new AtomicInteger();
    }

    public int getBytes() {
        return bytes.get();
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) {
        return map.tailMap(from)
                .entrySet()
                .stream()
                .map(entry -> new Cell(entry.getKey(), entry.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) {
        final Value val = map.put(key.duplicate(), new Value(System.currentTimeMillis(), value.duplicate()));
        if (val == null) {
            bytes.addAndGet(key.remaining() + value.remaining() + Long.BYTES);
        } else {
            if (val.isTombstone()) {
                bytes.addAndGet(value.remaining());
            } else {
                bytes.addAndGet(value.remaining() - val.getData().remaining());
            }
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        final Value value = map.put(key.duplicate(), Value.newTombstoneValue(System.currentTimeMillis()));
        if (value == null) {
            bytes.addAndGet(key.remaining() + Long.BYTES);
        } else if (!value.isTombstone()) {
            bytes.addAndGet(value.getData().remaining());
        }
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public void close() {
        //nothing to close
    }
}
