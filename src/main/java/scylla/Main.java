package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import scylla.type.Author;

// For DML queries, such as SELECT
import java.util.Set;
import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

// For DDL queries, such as CREATE TABLE
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

public class Main {
    private final static Database database = new Database();

    public static void main(String[] args) {
        database.createDatabase();

        // Insert JSON files
        Reader.insertFromJSON(Main.class.getClassLoader().getResource("import/readerscylladb.json").getFile());
        Book.insertFromJSON(Main.class.getClassLoader().getResource("import/bookscylladb.json").getFile());
        Copy.insertFromJSON(Main.class.getClassLoader().getResource("import/copyscylladb.json").getFile());
        Loan.insertFromJSON(Main.class.getClassLoader().getResource("import/loanscylladb.json").getFile());

        Reader reader = new Reader(UUID.randomUUID(), "John", "Doe", "1990-01-01", "1234 Main St", "Anytown", "12345", "john.doe@example.com", "123-456-7890");
        Reader.insert(reader);
        // Book book = new Book(UUID.randomUUID(), "The Hobbit", 1937, "The Hobbit is a children's fantasy novel by English author J. R. R. Tolkien.", Set.of("Fantasy"), Set.of(new Author("J. R. R.", "Tolkien")));
        // Book.insert(book);
    }

    public static void test(){
        try (CqlSession session = database.getSession()) {
            session.execute("CREATE COLUMNFAMILY IF NOT EXISTS library.User (id bigint PRIMARY KEY, name text);");
            session.execute("INSERT INTO library.User (id, name) values (1, 'john doe');");

            System.out.println("Table created");
        }

        try (CqlSession session = CqlSession.builder().build()) {
            ResultSet rs = session.execute("select * from library.User");
            Row row = rs.one();
            System.out.println(row.getString("name"));
        }

        try (CqlSession session = CqlSession.builder().build()) {
            InsertInto insert = QueryBuilder.insertInto("library", "User");
            SimpleStatement statement = insert.value("id", QueryBuilder.literal(2))
                    .value("name", QueryBuilder.literal("dev user"))
                    .build();
            ResultSet rs = session.execute(statement);
        }

        try (CqlSession session = CqlSession.builder().build()) {
            Select query = QueryBuilder.selectFrom("library", "User").all()
                    .whereColumn("name").isEqualTo(QueryBuilder.literal("dev user"))
                    .allowFiltering();

            SimpleStatement statement = query.build();
            ResultSet rs = session.execute(statement);
            Row row = rs.one();
        }

        try (CqlSession session = CqlSession.builder().build()) {
            Select query = selectFrom("system", "local").column("release_version"); // SELECT release_version FROM system.local
            SimpleStatement statement = query.build();

            ResultSet rs = session.execute(statement);
            Row row = rs.one();
            System.out.println(row.getString("release_version"));
        }
    }
}
