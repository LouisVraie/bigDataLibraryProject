package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.select.Select;

import java.net.InetSocketAddress;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

public class Database {
    public String keyspace = "library";

    public CqlSession getSession(){
        return CqlSession.builder().withKeyspace(keyspace).build();
    }
    // Constructeur
    public Database() {
    }

}

