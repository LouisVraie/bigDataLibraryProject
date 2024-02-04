package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.*;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import com.datastax.oss.driver.api.querybuilder.term.Term;
import scylla.type.Author;

import java.util.*;
import java.util.stream.Collectors;

public class Book implements CRUD<Book> {
    public static final String TABLE_NAME = "book";
    private final static Database database = new Database();
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
            .withColumn("year", DataTypes.DOUBLE)
            .withColumn("summary", DataTypes.TEXT)
            .withColumn("categories", DataTypes.setOf(DataTypes.TEXT))
            // .withColumn("authors", DataTypes.setOf(SchemaBuilder.udt(Author.TYPE_NAME, true)));
            .withColumn("authors", DataTypes.setOf(DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT, true)));

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

    public Set<Map<String, String>> getAuthorsFormated() {
        Set<Map<String, String>> set = new HashSet<>();

        for (Author author : authors) {
            Map<String, String> map = new HashMap<>();
            map.put("firstname", author.getFirstname());
            map.put("lastname", author.getLastname());
            set.add(map);
        }
        return set;
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

    public Book get(UUID id){
        return null;
    }

    public List<Book> getAll(){
        return null;
    }

    public static void insert(Book book){
        try (CqlSession session = database.getSession()){

            Insert insert = insertInto(Book.TABLE_NAME)
                .value("id_book", literal(book.getIdBook()))
                .value("title", literal(book.getTitle()))
                .value("year", literal(book.getYear()))
                .value("summary", literal(book.getSummary()))
                .value("categories", literal(book.getCategories()))
                .value("authors", literal(book.getAuthorsFormated()));

            ResultSet result = session.execute(insert.ifNotExists().build());

            System.out.println("Book inserted ? "+ result.wasApplied());
        }
    }

    public void update(UUID id, Book entity){

    }

    public void delete(UUID id){

    }
}
