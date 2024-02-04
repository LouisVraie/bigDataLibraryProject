package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONObject;
import scylla.utils.Converter;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

public class Reader implements CRUD<Reader>{

    private static final String TABLE_NAME = "reader";
    private final static Database database = new Database();
    private UUID idReader;
    private String firstname;
    private String lastname;
    private String birthDate;
    private String street;
    private String city;
    private String postalCode;
    private String email;
    private String phoneNb;

    // Constructeur
    public Reader(UUID idReader, String firstname, String lastname, String birthDate, String street, String city, String postalCode, String email, String phoneNb) {
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
    public String getBirthDate() {
        return birthDate;
    }

    public void setBirthDate(String birthDate) {
        this.birthDate = birthDate;
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
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
                // Insert into ScyllaDB
                ResultSet result = session.execute(
                        insertQuery,
                        idReader, firstname, lastname, birthDate, street, city, postalCode, email, phoneNb);
                if(result.wasApplied())
                {
                    count++;
                }
            }
            System.out.println(TABLE_NAME + " : " + count + "/"+ jsonArray.length() +" imported !");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public Book get(UUID id){
        return null;
    }

    public List<Book> getAll(){
        return null;
    }

    public static void insert(Reader entity){
        try (CqlSession session = database.getSession()){

            Insert insert = insertInto(Reader.TABLE_NAME)
                .value("id_reader", literal(entity.getIdReader()))
                .value("firstname", literal(entity.getFirstname()))
                .value("lastname", literal(entity.getLastname()))
                .value("birth_date", literal(entity.getBirthDate()))
                .value("street", literal(entity.getStreet()))
                .value("city", literal(entity.getCity()))
                .value("postal_code", literal(entity.getPostalCode()))
                .value("email", literal(entity.getEmail()))
                .value("phone_nb", literal(entity.getPhoneNb()));

            ResultSet result = session.execute(insert.ifNotExists().build());

            System.out.println("Reader inserted ? "+ result.wasApplied());
        }
    }

    public void insert(ArrayList<Reader> entityList){
        for (Reader entity : entityList) {
            insert(entity);
        }
    }

    public void update(UUID id, Reader entity){

    }

    public void delete(UUID id){

    }
}
