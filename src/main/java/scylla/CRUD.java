package scylla;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public interface CRUD<T> {
    // Méthodes CRUD génériques
    static <T> T get(UUID id) {
        // Implémentation de la méthode get
        return null; // Remplacer null par l'implémentation réelle
    }

    static <T> List<T> getAll() {
        // Implémentation de la méthode getAll
        return null; // Remplacer null par l'implémentation réelle
    }

    static <T> void insert(T entity) {
        // Implémentation de la méthode insert
    }
    static <T> void insert(ArrayList<T> entityList) {
        for (T entity : entityList) {
            insert(entity);
        }
    }

    static <T> void update(UUID id, T entity) {
        // Implémentation de la méthode update
    }

    static <T> void delete(UUID id) {
        // Implémentation de la méthode delete
    }
}
