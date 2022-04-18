package ru.spbstu.dao.table;

import org.jetbrains.annotations.NotNull;
import ru.spbstu.dao.Cell;
import ru.spbstu.dao.Iters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {

    @NotNull
    Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException;

    void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException;

    void remove(@NotNull ByteBuffer key);

    int size();

    void close();

}
