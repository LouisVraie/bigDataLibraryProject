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
import java.util.List;


public class Loan {
    private static Database database = new Database();
    public String id_load;
    public String id_copy;
    public String id_reader;
    public String id_book;
    public String firsname;
    public String lastname;
    public String title;
    public List<Author> authors;

    public String load_date;
    public String expiry_date;
    public String due_date;

    public Loan(String id_load, String id_copy, String id_reader, String id_book, String firsname, String lastname, String title, List<Author> authors, String load_date, String expiry_date, String due_date) {
        this.id_load = id_load;
        this.id_copy = id_copy;
        this.id_reader = id_reader;
        this.id_book = id_book;
        this.firsname = firsname;
        this.lastname = lastname;
        this.title = title;
        this.authors = authors;
        this.load_date = load_date;
        this.expiry_date = expiry_date;
        this.due_date = due_date;
    }
}
