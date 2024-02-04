package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import scylla.codec.AuthorCodec;
import scylla.type.Author;

import java.net.InetSocketAddress;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace;

public class Database {
    public static final String KEYSPACE = "library";

    /**
     * Create types
     */
    private void createTypes(CqlSession session){
        Author.createType(session);

        // Save the codec
        MutableCodecRegistry registry = ((MutableCodecRegistry) session.getContext().getCodecRegistry());
        registry.register(new AuthorCodec());
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
            createTypes(session);
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

