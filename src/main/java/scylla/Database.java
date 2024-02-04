package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import scylla.type.Author;

import java.net.InetSocketAddress;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace;

public class Database {
    public static final String KEYSPACE = "library";

    /**
     * Drop tables
     */
    private void dropTables(CqlSession session){
        // Drop tables
        Reader.dropTable(session);
        Book.dropTable(session);
        Copy.dropTable(session);
        Loan.dropTable(session);
    }
    /**
     * Create tables
     */
    private void createTables(CqlSession session){
        // Create tables
        Reader.createTable(session);
        Book.createTable(session);
        Copy.createTable(session);
        Loan.createTable(session);
    }

    /**
     * Create the database
     */
    public void createDatabase()
    {
        try (CqlSession session = getSession()) {
            //dropTables(session);
            createTables(session);
        } catch (Exception e) {
            System.out.println("ERROR ! The database wasn't created !");
            e.printStackTrace();
        }
    }

    /**
     * Connect to the ScyllaDB cluster
     * @return CqlSession 
     */
    public CqlSession getSession(){
        CqlSession session = null;
        try {
            session = CqlSession.builder().withKeyspace(KEYSPACE).build();
        } catch (Exception e) {
            System.out.println("Keyspace doesn't exist. Creating keyspace...");
            createKeyspaceIfNotExists();
            session = CqlSession.builder().withKeyspace(KEYSPACE).build();
        }
        return session;
    }

    /**
     * Create keyspace if it doesn't exist
     */
    public void createKeyspaceIfNotExists() {
        try (CqlSession session = CqlSession.builder().build()) {
            CreateKeyspace createKeyspace = createKeyspace(KEYSPACE).ifNotExists().withSimpleStrategy(3);
            session.execute(createKeyspace.build());
            System.out.println("Keyspace created successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

