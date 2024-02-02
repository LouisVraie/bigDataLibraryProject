package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.select.Select;

// For DML queries, such as SELECT
import javax.management.Query;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

// For DDL queries, such as CREATE TABLE
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

public class Author {

    private static Database database = new Database();

    public Author(){
    }

    public static String getAuthor(String firstname, String lastname){
        try (CqlSession session = database.getSession()) {
            Select query = selectFrom("Author").all()
                    .whereColumn("firstname_a").isEqualTo(QueryBuilder.literal(firstname))
                    .whereColumn("lastname_a").isEqualTo(QueryBuilder.literal(lastname))
                    .allowFiltering();
        }
    }
    public static void test(){
        try (CqlSession session = database.getSession()){
            Select query = selectFrom("User").all()
                    .whereColumn("name").isEqualTo(QueryBuilder.literal("dev user"))
                    .allowFiltering();

            SimpleStatement statement = query.build();
            ResultSet rs = session.execute(statement);
            Row row = rs.one();
            System.out.println(row.getString("name"));
        }
    }

    public static void main(String[] args) {

        test();
        test();
    }
}
