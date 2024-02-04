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
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import scylla.type.Author;
import scylla.utils.Converter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

    public Copy(UUID idCopy, UUID idBook, String title, String editionName, boolean state, String wear) {
        this.idCopy = idCopy;
        this.idBook = idBook;
        this.title = title;
        this.editionName = editionName;
        this.state = state;
        this.wear = wear;
    }

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
    public boolean getState() {
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
    
    public static void testCopy(){
        // Insert a new copy
        Copy copy = new Copy(UUID.fromString("10000000-0000-0000-0000-000000000001"), UUID.fromString("00000000-0000-0000-0000-100000000001"), "Le Petit Prince", "Gallimard", true, "Good");
        copy.insert();

        // Insert a list of copies
        ArrayList<Copy> copies = new ArrayList<>();
        copies.add(new Copy(UUID.fromString("10000000-0000-0000-0000-000000000002"), UUID.fromString("00000000-0000-0000-0000-100000000002"), "Le Petit Prince", "Gallimard", true, "Good"));
        copies.add(new Copy(UUID.fromString("10000000-0000-0000-0000-000000000003"), UUID.fromString("00000000-0000-0000-0000-100000000003"), "Le Petit Prince", "Gallimard", true, "Fair"));
        Copy.insert(copies);

        // Get a copy
        Copy copy1 = copy.get(UUID.fromString("10000000-0000-0000-0000-000000000001"));
        System.out.println(copy1);

        // Create an index
        Copy.createIndex("edition_name");

        // Get all copies
        List<Copy> copyList = copy.getAll(null, 10, "edition_name");
        for (Copy c : copyList) {
            System.out.println(c);
        }

        // Update a copy
        Copy copy2 = new Copy(UUID.fromString("10000000-0000-0000-0000-000000000001"), UUID.fromString("00000000-0000-0000-0000-100000000001"), "Le Petit Prince", "Gallimard", true, "Bon état");
        copy.update(UUID.fromString("10000000-0000-0000-0000-000000000001"), UUID.fromString("00000000-0000-0000-0000-100000000001"), copy2);

        // Delete a copy
        copy.delete(UUID.fromString("10000000-0000-0000-0000-000000000001"));
    }

    // Méthode toString() pour l'affichage
    @Override
    public String toString() {
        return "Copy{" +
                "idCopy=" + idCopy +
                ", idBook=" + idBook +
                ", title='" + title + '\'' +
                ", editionName='" + editionName + '\'' +
                ", state=" + state +
                ", wear='" + wear + '\'' +
                '}';
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
    
    public Copy get(UUID id){
        try (CqlSession session = database.getSession()){
            System.out.println(TABLE_NAME+" get :");
            Select select = selectFrom(TABLE_NAME)
                    .all()
                    .whereColumn("id_copy").isEqualTo(literal(id)).allowFiltering();
            Row row = session.execute(select.build()).one();

            if (row != null) {
                return buildCopyFromRow(row);
            } else {
                return null;
            }
        }
    }

    public List<Copy> getAll(Map<String, Object> whereConditions, int limit, String sortByColumn){
        List<Copy> copies = new ArrayList<>();
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
                copies.add(buildCopyFromRow(row));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return copies;
    }

    public void insert(){
        try (CqlSession session = database.getSession()){

            Insert insert = insertInto(Copy.TABLE_NAME)
                .value("id_copy", literal(this.getIdCopy()))
                .value("id_book", literal(this.getIdBook()))
                .value("title", literal(this.getTitle()))
                .value("edition_name", literal(this.getEditionName()))
                .value("state", literal(this.getState()))
                .value("wear", literal(this.getWear()));

            ResultSet result = session.execute(insert.ifNotExists().build());

            System.out.println(TABLE_NAME + " inserted ? "+ result.wasApplied());
        }
    }
    public static void insert(ArrayList<Copy> copies) {
        for (Copy entity : copies) {
            entity.insert();
        }
    }

    public void update(UUID id_copy, UUID id_book, Copy entity){
        try (CqlSession session = database.getSession()) {
            System.out.println(TABLE_NAME + " update :");
            Update update = QueryBuilder.update(TABLE_NAME)
                .setColumn("title", literal(entity.getTitle()))
                .setColumn("edition_name", literal(entity.getEditionName()))
                .setColumn("state", literal(entity.getState()))
                .setColumn("wear", literal(entity.getWear()))
                .whereColumn("id_copy").isEqualTo(literal(id_copy))
                .whereColumn("id_book").isEqualTo(literal(id_book));

            ResultSet result = session.execute(update.build());
            System.out.println(TABLE_NAME + " updated ? " + result.wasApplied());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void delete(UUID id){
        try (CqlSession session = database.getSession()) {
            System.out.println(TABLE_NAME + " delete :");
            Delete delete = deleteFrom(TABLE_NAME)
                    .whereColumn("id_copy").isEqualTo(literal(id));

            ResultSet result = session.execute(delete.build());
            System.out.println(TABLE_NAME + " deleted ? " + result.wasApplied());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Copy buildCopyFromRow(Row row){
        return new Copy(
            row.getUuid("id_copy"),
            row.getUuid("id_book"),
            row.getString("title"),
            row.getString("edition_name"),
            row.getBoolean("state"),
            row.getString("wear")
        );
    }

}

