package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;

import scylla.utils.Converter;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONObject;

public class Copy implements CRUD<Copy>, TableOperation {
    public static final String TABLE_NAME = "copy";
    private final static Database database = new Database();
    private UUID idCopy;
    private UUID idBook;
    private String title;
    private String editionName;
    private boolean state;
    private String wear;

    // Getters et Setters pour idCopy
    public UUID getIdCopy() {
        return idCopy;
    }

    public void setIdCopy(UUID idCopy) {
        this.idCopy = idCopy;
    }

    // Getters et Setters pour idBook
    public UUID getIdBook() {
        return idBook;
    }

    public void setIdBook(UUID idBook) {
        this.idBook = idBook;
    }

    // Getters et Setters pour title
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    // Getters et Setters pour editionName
    public String getEditionName() {
        return editionName;
    }

    public void setEditionName(String editionName) {
        this.editionName = editionName;
    }

    // Getters et Setters pour state
    public boolean isState() {
        return state;
    }

    public void setState(boolean state) {
        this.state = state;
    }

    // Getters et Setters pour wear
    public String getWear() {
        return wear;
    }

    public void setWear(String wear) {
        this.wear = wear;
    }

    // Méthode pour créer la table Copy
    public static void createTable(CqlSession session) {
        CreateTable createTable = SchemaBuilder.createTable(TABLE_NAME)
                .ifNotExists()
                .withPartitionKey("id_copy", DataTypes.UUID)
                .withClusteringColumn("id_book", DataTypes.UUID)
                .withColumn("title", DataTypes.TEXT)
                .withColumn("edition_name", DataTypes.TEXT)
                .withColumn("state", DataTypes.BOOLEAN)
                .withColumn("wear", DataTypes.TEXT);

        session.execute(createTable.build());
        System.out.println("Table '"+TABLE_NAME+"' created successfully.");
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

                UUID idCopy = Converter.intToUUID(jsonObject.getInt("id_copy"), Copy.TABLE_NAME);
                UUID idBook = Converter.intToUUID(jsonObject.getInt("id_book"), Book.TABLE_NAME);
                String title = jsonObject.getString("title");
                String editionName = jsonObject.getString("edition_name");
                Boolean state = jsonObject.getBoolean("state");
                String wear = jsonObject.getString("wear");

                String insertQuery = "INSERT INTO "+Copy.TABLE_NAME+" (id_copy, id_book, title, edition_name, state, wear) " +
                        "VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS";
                // Insert into ScyllaDB
                ResultSet result = session.execute(
                        insertQuery,
                        idCopy, idBook, title, editionName, state, wear);
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
}

