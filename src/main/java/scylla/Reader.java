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

public class Reader {
    private static Database database = new Database();
    public String id_reader;
    public String firstname;
    public String lastname;
    public String birth_date;
    public String city;
    public String postal_code;
    public String email;
    public String phone_nb;

    public Reader(String id_reader, String firstname, String lastname, String birth_date, String city, String postal_code, String email, String phone_nb) {
        this.id_reader = id_reader;
        this.firstname = firstname;
        this.lastname = lastname;
        this.birth_date = birth_date;
        this.city = city;
        this.postal_code = postal_code;
        this.email = email;
        this.phone_nb = phone_nb;
    }
}
