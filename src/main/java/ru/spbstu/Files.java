package ru.spbstu;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicLong;

public final class Files {

    private static final String TEMP_PREFIX = "lsm-db";

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private Files() {
        // Don't instantiate
    }

    public static File createTempDirectory() throws IOException {
        final File data = java.nio.file.Files.createTempDirectory(TEMP_PREFIX).toFile();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (data.exists()) {
                    recursiveDelete(data);
                }
            } catch (IOException e) {
                log.error("Cannot delete temporary directory: {}", data, e);
            }
        }));
        return data;
    }

    public static void recursiveDelete(@NotNull final File path) throws IOException {
        java.nio.file.Files.walkFileTree(
                path.toPath(),
                new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(@NotNull final Path file,
                                                     @NotNull final BasicFileAttributes attrs) throws IOException {
                        java.nio.file.Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(@NotNull final Path dir,
                                                              @NotNull final IOException exc) throws IOException {
                        java.nio.file.Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                }
        );
    }

    public static long directorySize(@NotNull final File path) throws IOException {
        final AtomicLong result = new AtomicLong(0L);
        java.nio.file.Files.walkFileTree(
                path.toPath(),
                new SimpleFileVisitor<>() {
                    @NotNull
                    @Override
                    public FileVisitResult visitFile(
                            @NotNull final Path file,
                            @NotNull final BasicFileAttributes attrs) {
                        result.addAndGet(attrs.size());
                        return FileVisitResult.CONTINUE;
                    }
                });
        return result.get();
    }

}
