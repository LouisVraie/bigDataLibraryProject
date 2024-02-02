package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.select.Select;

// For DML queries, such as SELECT
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

// For DDL queries, such as CREATE TABLE
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;


public class Main {

    public static void main(String[] args) {

        try (CqlSession session = CqlSession.builder().build()) {
            session.execute("CREATE KEYSPACE IF NOT EXISTS library WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};");
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
