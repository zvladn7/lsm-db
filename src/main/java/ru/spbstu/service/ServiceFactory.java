package ru.spbstu.service;

import org.jetbrains.annotations.NotNull;
import ru.spbstu.dao.DAO;
import ru.spbstu.service.topology.ServiceTopology;

import java.io.IOException;
import java.util.Set;

public class ServiceFactory {

    private static final long MAX_HEAP = 256 * 1024 * 1024;
    private static final int EXECUTOR_QUEUE_SIZE = 1024;

    private ServiceFactory() {
        // not supposed to be instantiated
    }

    @NotNull
    public static Service create(final int port,
                                 @NotNull final DAO dao,
                                 @NotNull final Set<String> topology) throws IOException {
        if (Runtime.getRuntime().maxMemory() > MAX_HEAP) {
            throw new IllegalStateException("The heap is too big. Consider setting Xmx.");
        }

        if (port <= 0 || 65536 <= port) {
            throw new IllegalArgumentException("Port out of range");
        }

        final String me = "http://localhost:" + port;
        return new AsyncService(port,
                dao,
                Runtime.getRuntime().availableProcessors(),
                EXECUTOR_QUEUE_SIZE,
                new ServiceTopology(topology, me));
    }

}
