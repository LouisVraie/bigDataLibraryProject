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

public class Book {
}
