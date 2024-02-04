package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndex;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import org.json.JSONArray;
import org.json.JSONObject;
import scylla.utils.Converter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

public class Reader implements CRUD<Reader>, TableOperation{

    public static final String TABLE_NAME = "reader";
    private final static Database database = new Database();
    private UUID idReader;
    private String firstname;
    private String lastname;
    private Instant birthDate;
    private String street;
    private String city;
    private String postalCode;
    private String email;
    private String phoneNb;

    // Constructeur
    public Reader(UUID idReader, String firstname, String lastname, Instant birthDate, String street, String city, String postalCode, String email, String phoneNb) {
        this.idReader = idReader;
        this.firstname = firstname;
        this.lastname = lastname;
        this.birthDate = birthDate;
        this.street = street;
        this.city = city;
        this.postalCode = postalCode;
        this.email = email;
        this.phoneNb = phoneNb;
    }

    public static void testReader(){
        // Insert one reader
        Reader reader = new Reader(UUID.fromString("00000000-0000-0000-0000-000000000001"), "John", "Doe", Converter.stringToDate("1990-01-01"), "1234 Main St", "Anytown", "12345", "john.doe@example.com", "123-456-7890");
        reader.insert();

        // Insert Multiple reader
        ArrayList<Reader> readers = new ArrayList<>();
        readers.add(new Reader(UUID.fromString("00000000-0000-0000-0000-000000000002"), "Alice", "Smith", Converter.stringToDate("1985-05-15"), "5678 Elm St", "Sometown", "54321", "alice.smith@example.com", "987-654-3210"));
        readers.add(new Reader(UUID.fromString("00000000-0000-0000-0000-000000000003"), "Bob", "Johnson", Converter.stringToDate("1978-09-22"), "9876 Oak St", "Anothertown", "67890", "bob.johnson@example.com", "456-789-0123"));
        readers.add(new Reader(UUID.fromString("00000000-0000-0000-0000-000000000004"), "Emily", "Brown", Converter.stringToDate("1995-03-10") , "3456 Pine St", "Cityville", "13579", "emily.brown@example.com", "789-012-3456"));
        Reader.insert(readers);

        // Get a reader by id
        Reader readerGet = Reader.get(UUID.fromString("00000000-0000-0000-0000-000000000004"));
        System.out.println(readerGet.toString());

        // Create an index
        createIndex("lastname");

        // Get All
        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("lastname", "Doe");

        int limit = 10;
        String sortByColumn = "firstname";
        List<Reader> getAllReaders = Reader.getAll(whereConditions, limit, sortByColumn);

        // Afficher les lecteurs récupérés
        for (Reader getAllReader : getAllReaders) {
            System.out.println(getAllReader);
        }

        // Update a reader
        Reader readerToUpdate = new Reader(UUID.fromString("00000000-0000-0000-0000-000000000001"), "Jane", "Doe", Converter.stringToDate("1988-07-15"), "1234 Oak St", "UpdateTown", "54321", "jane.doe@example.com", "987-654-3210");
        reader.update(UUID.fromString("00000000-0000-0000-0000-000000000001"), readerToUpdate);

        // Delete a reader
        reader.delete(UUID.fromString("00000000-0000-0000-0000-000000000001"));
    }

    // Getters et Setters pour idReader
    public UUID getIdReader() {
        return idReader;
    }

    public void setIdReader(UUID idReader) {
        this.idReader = idReader;
    }

    // Getters et Setters pour firstname
    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    // Getters et Setters pour lastname
    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }

    // Getters et Setters pour birthDate
    public Instant getBirthDate() {
        return birthDate;
    }
    public String getBirthDateString() {
        return birthDate.toString();
    }

    public void setBirthDate(Instant birthDate) {
        this.birthDate = birthDate;
    }
    public void setBirthDate(String birthDate) {
        this.birthDate = Converter.stringToDate(birthDate);
    }

    // Getters et Setters pour street
    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    // Getters et Setters pour city
    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    // Getters et Setters pour postalCode
    public String getPostalCode() {
        return postalCode;
    }

    public void setPostalCode(String postalCode) {
        this.postalCode = postalCode;
    }

    // Getters et Setters pour email
    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    // Getters et Setters pour phoneNb
    public String getPhoneNb() {
        return phoneNb;
    }

    public void setPhoneNb(String phoneNb) {
        this.phoneNb = phoneNb;
    }

    public static void createTable(CqlSession session) {
        CreateTable createTable = SchemaBuilder.createTable(TABLE_NAME)
            .ifNotExists()
            .withPartitionKey("id_reader", DataTypes.UUID)
            .withColumn("firstname", DataTypes.TEXT)
            .withColumn("lastname", DataTypes.TEXT)
            .withColumn("birth_date", DataTypes.TIMESTAMP)
            .withColumn("street", DataTypes.TEXT)
            .withColumn("city", DataTypes.TEXT)
            .withColumn("postal_code", DataTypes.TEXT)
            .withColumn("email", DataTypes.TEXT)
            .withColumn("phone_nb", DataTypes.TEXT);

        session.execute(createTable.build());
        System.out.println("Table '" + TABLE_NAME + "' created successfully.");
    }

    public static void dropTable(CqlSession session){
        ResultSet result = session.execute(SchemaBuilder.dropTable(TABLE_NAME).ifExists().build());
        
        if(result.wasApplied()){
            System.out.println("Table '"+TABLE_NAME+"' dropped successfully.");
        }
    }

    public static void insertFromJSON(String filepath){

        try (CqlSession session = database.getSession()){
            JSONArray jsonArray = Converter.getJSONFromFile(filepath);
            int count = 0;
            // Iterate through JSON array and insert into ScyllaDB
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);

                UUID idReader = Converter.intToUUID(jsonObject.getInt("id_reader"), Reader.TABLE_NAME);
                String firstname = jsonObject.getString("firstname");
                String lastname = jsonObject.getString("lastname");
                Instant birthDate = Converter.stringToDate(jsonObject.getString("birth_date"));
                String street = jsonObject.getString("street");
                String city = jsonObject.getString("city");
                String postalCode;
                try {
                    postalCode = jsonObject.getString("postal_code");
                } catch (Exception e) {
                    postalCode = "00000";
                }
                String email = jsonObject.getString("email");
                String phoneNb = jsonObject.getString("phone_nb");

                String insertQuery = "INSERT INTO "+Reader.TABLE_NAME+" (id_reader, firstname, lastname, birth_date, street, city, postal_code, email, phone_nb) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";
                // Insert into ScyllaDB
                ResultSet result = session.execute(
                        insertQuery,
                        idReader, firstname, lastname, birthDate, street, city, postalCode, email, phoneNb);
                if(result.wasApplied())
                {
                    count++;
                }
            }
            System.out.println(TABLE_NAME + " : " + count + "/"+ jsonArray.length() +" record(s) imported !");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Reader buildReaderFromRow(Row row){
        return new Reader(
            row.getUuid("id_reader"),
            row.getString("firstname"),
            row.getString("lastname"),
            row.getInstant("birth_date"),
            row.getString("street"),
            row.getString("city"),
            row.getString("postal_code"),
            row.getString("email"),
            row.getString("phone_nb")
        );
    }

    public static Reader get(UUID id){
        try (CqlSession session = database.getSession()){
            System.out.println(TABLE_NAME+" get :");
            Select select = selectFrom(TABLE_NAME)
                    .all()
                    .whereColumn("id_reader").isEqualTo(literal(id));
            Row row = session.execute(select.build()).one();

            if (row != null) {
                return buildReaderFromRow(row);
            } else {
                return null;
            }
        }
    }

    public static List<Reader> getAll(Map<String, Object> whereConditions, int limit, String sortByColumn){
        List<Reader> readers = new ArrayList<>();
        try (CqlSession session = database.getSession()) {
            System.out.println(TABLE_NAME+" get all :");
            Select select = selectFrom(TABLE_NAME).all().allowFiltering().limit(limit);

            // Ajouter l'option de tri si spécifiée
            if (sortByColumn != null && !sortByColumn.isEmpty()) {
                select.orderBy(sortByColumn, ClusteringOrder.ASC);
            }

            if (whereConditions != null && !whereConditions.isEmpty()) {
                whereConditions.forEach((column, value) ->
                    select.whereColumn(column).isEqualTo(literal(value)));
            }

            ResultSet resultSet = session.execute(select.build());

            for (Row row : resultSet) {
                readers.add(buildReaderFromRow(row));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return readers;
    }

    public void insert(){
        try (CqlSession session = database.getSession()){
            System.out.println(TABLE_NAME+" insert :");

            Insert insert = insertInto(Reader.TABLE_NAME)
                .value("id_reader", literal(this.getIdReader()))
                .value("firstname", literal(this.getFirstname()))
                .value("lastname", literal(this.getLastname()))
                .value("birth_date", literal(this.getBirthDate()))
                .value("street", literal(this.getStreet()))
                .value("city", literal(this.getCity()))
                .value("postal_code", literal(this.getPostalCode()))
                .value("email", literal(this.getEmail()))
                .value("phone_nb", literal(this.getPhoneNb()));

            ResultSet result = session.execute(insert.ifNotExists().build());

            System.out.println("Reader inserted ? "+ result.wasApplied());
        }
    }

    public static void insert(ArrayList<Reader> entityList){
        System.out.println(TABLE_NAME+" insert a list :");
        for (Reader entity : entityList) {
            entity.insert();
        }
    }

    public void update(UUID id, Reader entity){
        try (CqlSession session = database.getSession()) {
            System.out.println(TABLE_NAME + " update :");
            Update update = QueryBuilder.update(TABLE_NAME)
                .setColumn("firstname", literal(entity.getFirstname()))
                .setColumn("lastname", literal(entity.getLastname()))
                .setColumn("birth_date", literal(entity.getBirthDate()))
                .setColumn("street", literal(entity.getStreet()))
                .setColumn("city", literal(entity.getCity()))
                .setColumn("postal_code", literal(entity.getPostalCode()))
                .setColumn("email", literal(entity.getEmail()))
                .setColumn("phone_nb", literal(entity.getPhoneNb()))
                .whereColumn("id_reader").isEqualTo(literal(id));

            ResultSet result = session.execute(update.build());
            System.out.println("Reader updated ? " + result.wasApplied());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void delete(UUID id){
        try (CqlSession session = database.getSession()) {
            System.out.println(TABLE_NAME + " delete :");
            Delete delete = deleteFrom(TABLE_NAME)
                    .whereColumn("id_reader").isEqualTo(literal(id));

            ResultSet result = session.execute(delete.build());
            System.out.println("Reader deleted ? " + result.wasApplied());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        // write the string representation of the object on one line
        return "Reader{" +
                "id_reader=" + idReader.toString() +
                ", firstname='" + firstname + '\'' +
                ", lastname='" + lastname + '\'' +
                ", birth_date=" + birthDate.toString() +
                ", street='" + street + '\'' +
                ", city='" + city + '\'' +
                ", postal_code='" + postalCode + '\'' +
                ", email='" + email + '\'' +
                ", phone_nb='" + phoneNb + '\'' + '}';
    }

    public static void createIndex(String columnName){
        try (CqlSession session = database.getSession()) {
            String indexName = TABLE_NAME + "_" + columnName + "_idx";

            String createIndexQuery = "CREATE INDEX IF NOT EXISTS " + indexName +
                    " ON " + TABLE_NAME + " (" + columnName + ");";

            ResultSet result = session.execute(createIndexQuery);
            if (result.wasApplied()) {
                System.out.println("Index '" + indexName + "' created successfully.");
            } else {
                System.out.println("Failed to create index '" + indexName + "'.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
