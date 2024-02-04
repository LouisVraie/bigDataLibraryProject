package scylla.type;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateType;
import scylla.Database;

import java.nio.ByteBuffer;
import java.util.*;

public class Author {
    public static final String TYPE_NAME = "author";
    private final static Database database = new Database();
    private String firstname;
    private String lastname;

    // Constructeur
    public Author(String firstname, String lastname) {
        this.firstname = firstname;
        this.lastname = lastname;
    }

    // Getters et setters
    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }


    public Map<String, String> getAuthorFormated(){
        Map<String, String> map = new HashMap<>();
        map.put("firstname", firstname);
        map.put("lastname", lastname);
        return map;
    }

    /**
     * Create UDT for Author
     * @param session
     */
    public static void createType(CqlSession session){
        CreateType createAuthorType = SchemaBuilder.createType(TYPE_NAME).ifNotExists()
            .withField("firstname", DataTypes.TEXT)
            .withField("lastname", DataTypes.TEXT);
        session.execute(createAuthorType.build());
        System.out.println("UDT '"+ TYPE_NAME +"' created successfully.");
    }

    /*public static final TypeCodec<Author> AUTHOR_CODEC = new TypeCodec<Author>() {
        private final TypeCodec<Map<String, String>> innerCodec = TypeCodecs.mapOf(TypeCodecs.TEXT, TypeCodecs.TEXT);

        @Override
        public GenericType<Author> getJavaType() {
            return GenericType.of(Author.class);
        }

        @Override
        public DataType getCqlType() {
            try(CqlSession session = CqlSession.builder().build()){
                KeyspaceMetadata keyspaceMetadata = session.getMetadata().getKeyspace(Database.KEYSPACE).orElseThrow(() -> new IllegalStateException("Keyspace metadata not found"));
                return keyspaceMetadata.getUserDefinedType(Author.TYPE_NAME).get();
            }
        }

        @Override
        public ByteBuffer encode(Author author, ProtocolVersion protocolVersion) {
            if (author == null) {
                return null;
            }
            Map<String, String> map = new HashMap<>();
            map.put("firstname", author.getFirstname());
            map.put("lastname", author.getLastname());

            return innerCodec.encode(map, protocolVersion);
        }

        @Override
        public Author decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            if (bytes == null || bytes.remaining() == 0) {
                return null;
            }

            Map<String, String> map = innerCodec.decode(bytes, protocolVersion);

            return new Author(map.get("firstname"), map.get("lastname"));
        }

        @Override
        public String format(Author author) {
            return innerCodec.format(authorToMap(author));
        }


        @Override
        public Author parse(String formatted) {
            Map<String, String> map = innerCodec.parse(formatted);

            return new Author(map.get("firstname"), map.get("lastname"));
        }

        private Map<String, String> authorToMap(Author author) {
            if (author == null) {
                return null;
            }

            Map<String, String> map = new HashMap<>();
            map.put("firstname", author.getFirstname());
            map.put("lastname", author.getLastname());

            return map;
        }
    };*/

    /*public static final TypeCodec<Set<Author>> AUTHOR_SET_CODEC = new TypeCodec<Set<Author>>() {
        private final TypeCodec<Set<Map<String, String>>> innerCodec = TypeCodecs.setOf(TypeCodecs.mapOf(TypeCodecs.TEXT, TypeCodecs.TEXT));
        @Override
        public GenericType<Set<Author>> getJavaType() {
            return GenericType.setOf(Author.class);
        }

        @Override
        public DataType getCqlType() {
            return DataTypes.setOf(SchemaBuilder.udt(Author.TYPE_NAME, true));
        }

        @Override
        public ByteBuffer encode(Set<Author> authors, ProtocolVersion protocolVersion) {
            List<ByteBuffer> authorList = new ArrayList<>();
            for (Author author : authors) {

            }
            return TypeCodecs.setOf(Author.TYPE_NAME).encode(authorList, protocolVersion);
        }

        @Override
        public Set<Author> decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            if (bytes == null || bytes.remaining() == 0) {
                return null;
            }
            Set<Map<String, String>> authorMaps = innerCodec.decode(bytes, protocolVersion);
            Set<Author> authors = new HashSet<>();
            for (Map<String, String> map : authorMaps) {
                authors.add(new Author(map.get("firstname"), map.get("lastname")));
            }
            return authors;
        }

        @Override
        public String format(Set<Author> authors) {
            Set<Map<String, String>> authorMaps = new HashSet<>();
            for (Author author : authors) {
                Map<String, String> map = new HashMap<>();
                map.put("firstname", author.getFirstname());
                map.put("lastname", author.getLastname());
                authorMaps.add(map);
            }
            return innerCodec.format(authorMaps);
        }

        @Override
        public Set<Author> parse(String formatted) {
            Set<Map<String, String>> authorMaps = innerCodec.parse(formatted);
            Set<Author> authors = new HashSet<>();
            for (Map<String, String> map : authorMaps) {
                authors.add(new Author(map.get("firstname"), map.get("lastname")));
            }
            return authors;
        }
    };*/

    public static final TypeCodec<Set<Author>> AUTHOR_SET_CODEC = new TypeCodec<Set<Author>>() {

        private final TypeCodec<Set<UdtValue>> innerCodec = TypeCodecs.setOf(TypeCodecs.udtOf(SchemaBuilder.udt(Author.TYPE_NAME, true)));

        @Override
        public GenericType<Set<Author>> getJavaType() {
            return GenericType.setOf(Author.class);
        }

        @Override
        public DataType getCqlType() {
            return DataTypes.setOf(SchemaBuilder.udt(Author.TYPE_NAME, true));
        }

        @Override
        public ByteBuffer encode(Set<Author> authors, ProtocolVersion protocolVersion) {
            Set<UdtValue> authorValues = new HashSet<>();

            for (Author author : authors) {
                UdtValue authorValue = SchemaBuilder.udt(Author.TYPE_NAME, true).newValue();
                authorValue.setString("firstname", author.getFirstname());
                authorValue.setString("lastname", author.getLastname());
                authorValues.add(authorValue);
            }
            return innerCodec.encode(authorValues, protocolVersion);
        }

        @Override
        public Set<Author> decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            if (bytes == null || bytes.remaining() == 0) {
                return null;
            }

            Set<UdtValue> authorValues = innerCodec.decode(bytes, protocolVersion);
            Set<Author> authors = new HashSet<>();

            for (UdtValue authorValue : authorValues) {
                authors.add(new Author(authorValue.getString("firstname"), authorValue.getString("lastname")));
            }
            return authors;
        }

        @Override
        public String format(Set<Author> authors) {

            Set<UdtValue> authorValues = new HashSet<>();
            for (Author author : authors) {
                UdtValue authorValue = SchemaBuilder.udt(Author.TYPE_NAME, true).newValue();
                authorValue.setString("firstname", author.getFirstname());
                authorValue.setString("lastname", author.getLastname());
                authorValues.add(authorValue);
            }
            return innerCodec.format(authorValues);
        }

        @Override
        public Set<Author> parse(String formatted) {
            Set<UdtValue> authorValues = innerCodec.parse(formatted);
            Set<Author> authors = new HashSet<>();
            for (UdtValue authorValue : authorValues) {
                authors.add(new Author(authorValue.getString("firstname"), authorValue.getString("lastname")));
            }
            return authors;
        }
    };
}
