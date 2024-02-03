package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.*;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import scylla.type.Author;

import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Book {
    public static final String TABLE_NAME = "book";
    private UUID idBook;
    private String title;
    private int year;
    private String summary;
    private Set<String> categories;
    private Set<Author> authors;

    // Constructeur
    public Book(UUID idBook, String title, int year, String summary, Set<String> categories, Set<Author> authors) {
        this.idBook = idBook;
        this.title = title;
        this.year = year;
        this.summary = summary;
        this.categories = categories;
        this.authors = authors;
    }

    /**
     * Create table for Book
     * @param session CqlSession object to execute the query
     */
    public static void createTable(CqlSession session){
        CreateTable createTable = SchemaBuilder.createTable(TABLE_NAME).ifNotExists()
            .withPartitionKey("id_book", DataTypes.UUID)
            .withColumn("title", DataTypes.TEXT)
            .withColumn("year", DataTypes.INT)
            .withColumn("summary", DataTypes.TEXT)
            .withColumn("categories", DataTypes.setOf(DataTypes.TEXT))
            .withColumn("authors", DataTypes.setOf(SchemaBuilder.udt(Author.TYPE_NAME, true)));

        session.execute(createTable.build());
        System.out.println("Table '"+TABLE_NAME+"' created successfully.");
    }
    // Getters et Setters
    public UUID getIdBook() {
        return idBook;
    }

    public void setIdBook(UUID idBook) {
        this.idBook = idBook;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public Set<String> getCategories() {
        return categories;
    }

    public void setCategories(Set<String> categories) {
        this.categories = categories;
    }

    public Set<Author> getAuthors() {
        return authors;
    }

    public void setAuthors(Set<Author> authors) {
        this.authors = authors;
    }

    // Méthode toString() pour l'affichage
    @Override
    public String toString() {
        return "Book{" +
                "idBook=" + idBook +
                ", title='" + title + '\'' +
                ", year=" + year +
                ", summary='" + summary + '\'' +
                ", categories=" + categories +
                ", authors=" + authors +
                '}';
    }
}
