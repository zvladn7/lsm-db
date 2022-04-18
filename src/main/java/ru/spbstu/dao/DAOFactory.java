package ru.spbstu.dao;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;

public final class DAOFactory {

    static final long MAX_HEAP = 256 * 1024 * 1024;

    private DAOFactory() {
        // don't instantiate
    }

    @NotNull
    public static DAO create(@NotNull final File data) throws IOException {
        if (Runtime.getRuntime().maxMemory() > MAX_HEAP) {
            throw new IllegalStateException("The heap is too big. Consider setting Xmx.");
        }

        if (!data.exists()) {
            throw new IllegalArgumentException("Path doesn't exist: " + data);
        }

        if (!data.isDirectory()) {
            throw new IllegalArgumentException("path is not a directory: " + data);
        }

        return new LsmDAOImpl(data, (int) MAX_HEAP / 32, 4);
    }

}
