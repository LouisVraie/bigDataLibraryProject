package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;

import java.util.UUID;

public class Reader {

    private static final String TABLE_NAME = "reader";
    private UUID idReader;
    private String firstname;
    private String lastname;
    private String birthDate;
    private String street;
    private String city;
    private String postalCode;
    private String email;
    private String phoneNb;

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
            .withColumn("birth_date", DataTypes.DATE)
            .withColumn("street", DataTypes.TEXT)
            .withColumn("city", DataTypes.TEXT)
            .withColumn("postal_code", DataTypes.TEXT)
            .withColumn("email", DataTypes.TEXT)
            .withColumn("phone_nb", DataTypes.TEXT);

        session.execute(createTable.build());
        System.out.println("Table '" + TABLE_NAME + "' created successfully.");
    }
}
