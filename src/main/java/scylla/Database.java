package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateType;
import scylla.type.Author;

import java.net.InetSocketAddress;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createType;

public class Database {
    public String keyspace = "library";

    /**
     * Create types
     */
    private void createTypes(CqlSession session){
        Author.createType(session);
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
            session = CqlSession.builder().withKeyspace(keyspace).build();
        } catch (Exception e) {
            System.out.println("Keyspace doesn't exist. Creating keyspace...");
            createKeyspaceIfNotExists();
            session = CqlSession.builder().withKeyspace(keyspace).build();
        }
        return session;
    }

    /**
     * Create keyspace if it doesn't exist
     */
    public void createKeyspaceIfNotExists() {
        try (CqlSession session = CqlSession.builder().build()) {
            CreateKeyspace createKeyspace = createKeyspace(keyspace).ifNotExists().withSimpleStrategy(3);
            session.execute(createKeyspace.build());
            System.out.println("Keyspace created successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

