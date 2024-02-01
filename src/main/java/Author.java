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

public class Author {
    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder().build()) {
            Select query = selectFrom("system", "local").column("release_version"); // SELECT release_version FROM system.local
            SimpleStatement statement = query.build();

            ResultSet rs = session.execute(statement);
            Row row = rs.one();
            System.out.println(row.getString("release_version"));
        }
    }
}
