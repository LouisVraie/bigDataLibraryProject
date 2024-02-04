package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

import java.util.UUID;

public class Copy {
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
}

