package scylla;

import com.datastax.oss.driver.api.core.CqlSession;

public interface TableOperation {
    // Méthodes pour les opérations sur les tables
    static void createTable(CqlSession session) {
        // Implémentation de la méthode createTable
    }

    static void dropTable(CqlSession session) {
        // Implémentation de la méthode dropTable
    }

    static void insertFromJSON(String filepath) {
        // Implémentation de la méthode insertFromJSON
    }
}
