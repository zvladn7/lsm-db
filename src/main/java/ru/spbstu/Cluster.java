package ru.spbstu;

import com.google.common.util.concurrent.Service;
import ru.spbstu.dao.DAO;
import ru.spbstu.dao.DAOFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public final class Cluster {

    private static final int[] PORTS = {8080, 8081, 8082};

    private Cluster() {
        // don't instantiate
    }

    public static void main(String[] args) throws IOException {
        final Set<String> topology = new HashSet<>(3);
        for (final int port : PORTS) {
            topology.add("http://localhost:" + port);
        }

        for (int i = 0; i < PORTS.length; ++i) {
            final int port = PORTS[i];
            final File data = Files.createTempDirectory();
            final DAO dao = DAOFactory.create(data);

            System.out.println("Starting node " + i + " on port " + port + " and data at " + data);        }
    }

}
