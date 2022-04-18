package ru.spbstu.dao;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.spbstu.Record;
import ru.spbstu.dao.table.SSTable;
import ru.spbstu.dao.table.Table;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

public class LsmDAOImpl implements DAO {

    private static final Logger logger = LoggerFactory.getLogger(LsmDAOImpl.class);

    private static final String SSTable_FILE_POSTFIX = ".dat";
    private static final String SSTable_TEMPORARY_FILE_POSTFIX = ".tmp";

    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    @Nonnull
    private final File storage;
    private final int bytesToFlush;

    private TableSet tableSet;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    @NotNull
    private final ExecutorService service;

    public LsmDAOImpl(@NotNull final File storage,
                      final int bytesToFlush,
                      final int flushQueueSize) throws IOException {
        this.storage = storage;
        this.bytesToFlush = bytesToFlush;
        final NavigableMap<Integer, Table> ssTables = new TreeMap<>();
        try (Stream<Path> files = Files.list(storage.toPath())) {
            files.filter(file -> Files.isRegularFile(file) && file.toString().endsWith(SSTable_FILE_POSTFIX))
                    .forEach(file -> {
                        final String fileName = file.getFileName().toString();
                        try {
                            final String stringGen = fileName.substring(0, fileName.indexOf(SSTable_FILE_POSTFIX));
                            final int gen = Integer.parseInt(stringGen);
                            ssTables.put(gen, new SSTable(file.toFile()));
                        } catch (IOException e) {
                            logger.warn("smth went wrong while SSTables was created");
                        } catch (NumberFormatException e) {
                            logger.info("Unexpected name of SSTable file: " + fileName, e);
                        }
                    });
        }
        final Map.Entry<Integer, Table> genEntry = ssTables.entrySet()
                .stream()
                .max(Comparator.comparing(Map.Entry::getKey))
                .orElse(null);
        int generation = 0;
        if (genEntry != null) {
            generation = genEntry.getKey();
        }
        this.tableSet = TableSet.provideTableSet(ssTables, generation + 1);
        this.service = Executors.newFixedThreadPool(flushQueueSize);
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) {
        final Iterator<Cell> freshElements = cellIterator(from);
        final Iterator<Cell> aliveElements = Iterators.filter(freshElements, el -> !el.getValue().isTombstone());
        return Iterators.transform(aliveElements, el -> Record.of(el.getKey(), el.getValue().getData()));
    }

    @NotNull
    @Override
    public Iterator<Cell> cellIterator(@NotNull ByteBuffer from) {
        final TableSet snapshot = getSnapshot();
        final List<Iterator<Cell>> iters = new ArrayList<>(snapshot.ssTables.size() + snapshot.memToFlush.size() + 2);
        iters.add(snapshot.memTable.iterator(from));
        snapshot.memToFlush.forEach(mem -> iters.add(mem.iterator(from)));
        return freshCellIterator(from, iters, snapshot);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        execute(() -> tableSet.memTable.upsert(key, value));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        execute(() -> tableSet.memTable.remove(key));
    }

    @Override
    public synchronized void compact() throws IOException {
        final boolean isEmptyListOfFiles;
        readLock.lock();
        try {
            isEmptyListOfFiles = tableSet.ssTables.isEmpty();
        } finally {
            readLock.unlock();
        }
        if (isEmptyListOfFiles) {
            return;
        }
        final TableSet snapshot;
        writeLock.lock();
        try {
            snapshot = tableSet;
            tableSet = tableSet.startCompact();
        } finally {
            writeLock.unlock();
        }

        logger.debug("Compacting byte(s) to to {}", snapshot.generation);

        final List<Iterator<Cell>> iters = new ArrayList<>(snapshot.ssTables.size());
        final Iterator<Cell> freshElements = freshCellIterator(EMPTY_BUFFER, iters, snapshot);
        final File dst = serialize(snapshot.generation, freshElements);

        try (Stream<Path> files = Files.list(storage.toPath())) {
            files.filter(f -> {
                final String name = f.getFileName().toFile().toString();
                final int gen = Integer.parseInt(name.substring(0, name.indexOf('.')));
                final boolean correctPostfix = name.endsWith(SSTable_FILE_POSTFIX);
                final boolean isNotFlushing = snapshot.ssTables.containsKey(gen);
                return gen < snapshot.generation && correctPostfix && isNotFlushing;
            }).forEach(f -> {
                try {
                    Files.delete(f);
                } catch (IOException e) {
                    logger.warn("Unable to delete file: " + f.getFileName().toFile().toString(), e);
                }
            });
        }

        logger.debug("Compacted byte(s) to {}", snapshot.generation);

        writeLock.lock();
        try {
            tableSet = tableSet.finishCompact(snapshot.ssTables, dst, snapshot.generation);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        final boolean isReadyToFlush;
        readLock.lock();
        try {
            isReadyToFlush = tableSet.memTable.size() > 0;
        } finally {
            readLock.unlock();
        }

        if (isReadyToFlush) {
            flush();
        }
        service.shutdown();
        while (true) {
            if (service.isTerminated()) {
                break;
            }
        }
        readLock.lock();
        try {
            tableSet.ssTables.values().forEach(Table::close);
        } finally {
            readLock.unlock();
        }
    }

    private TableSet getSnapshot() {
        readLock.lock();
        try {
            return tableSet;
        } finally {
            readLock.unlock();
        }
    }

    private void flush() {
        final TableSet snapshot;
        writeLock.lock();
        try {
            snapshot = tableSet;
            if (snapshot.memTable.size() == 0) {
                return;
            }
            tableSet = tableSet.startFlushingOnDisk();
        } finally {
            writeLock.unlock();
        }
        service.execute(() -> {
            try {
                logger.debug("Flushing {} byte(s) to {}", snapshot.memTable.getBytes(), snapshot.generation);
                final File dst = serialize(snapshot.generation, snapshot.memTable.iterator(EMPTY_BUFFER));
                writeLock.lock();
                try {
                    tableSet = tableSet.finishFlushingOnDisk(snapshot.memTable, dst, snapshot.generation);
                } finally {
                    writeLock.unlock();
                }
                logger.debug("Flushed {} bytes(s) to {}", snapshot.memTable.getBytes(), snapshot.generation);
            } catch (IOException e) {
                logger.error("Cannot flush memory table on disk", e);
                Runtime.getRuntime().halt(-1);
            }
        });
    }

    private Iterator<Cell> freshCellIterator(@NotNull final ByteBuffer from,
                                             @NotNull final List<Iterator<Cell>> itersList,
                                             final TableSet snapshot) {
        final List<Iterator<Cell>> iters = getAllCellItersList(from, itersList, snapshot);

        final Iterator<Cell> mergerElements = Iterators.mergeSorted(
                iters,
                Cell.BY_KEY_AND_VALUE_CREATION_TIME_COMPARATOR
        );

        return Iters.collapseEquals(mergerElements, Cell::getKey);
    }

    private List<Iterator<Cell>> getAllCellItersList(@NotNull final ByteBuffer from,
                                                    @NotNull final List<Iterator<Cell>> iters,
                                                    final TableSet snapshot) {
        snapshot.ssTables.descendingMap().values().forEach(ssTable -> {
            try {
                iters.add(ssTable.iterator(from));
            } catch (IOException e) {
                logger.error("smth went wrong when the SSTable iterator was addede");
            }
        });
        return iters;
    }

    private File serialize(final int generation,
                           final Iterator<Cell> iterator) throws IOException {
        final File file = new File(storage, generation + SSTable_TEMPORARY_FILE_POSTFIX);
        SSTable.serialize(file, iterator);
        final String newFileName = generation + SSTable_FILE_POSTFIX;
        final File dst = new File(storage, newFileName);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);
        return dst;
    }

    private void execute(final Runnable task) {
        final boolean isReadyToFlush;
        readLock.lock();
        try {
            task.run();
            isReadyToFlush = tableSet.memTable.getBytes() > bytesToFlush;
        } finally {
            readLock.unlock();
        }
        if (isReadyToFlush) {
            flush();
        }
    }

}
