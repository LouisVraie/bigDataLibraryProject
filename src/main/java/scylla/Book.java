package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.*;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import scylla.type.Author;
import scylla.utils.Converter;

import java.util.*;

import org.json.JSONArray;
import org.json.JSONObject;

public class Book implements CRUD<Book>, TableOperation{
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
            .withColumn("year", DataTypes.INT)
            .withColumn("summary", DataTypes.TEXT)
            .withColumn("categories", DataTypes.setOf(DataTypes.TEXT))
            // .withColumn("authors", DataTypes.setOf(SchemaBuilder.udt(Author.TYPE_NAME, true)));
            .withColumn("authors", DataTypes.setOf(DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT, true)));

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

                UUID idBook = Converter.intToUUID(jsonObject.getInt("id_book"), Book.TABLE_NAME);
                String title = jsonObject.getString("title");
                Integer year = jsonObject.getInt("year");
                String summary = jsonObject.getString("summary");
                Set<Map<String, String>> authors = Converter.stringToAuthors(jsonObject.getString("authors"));
                Set<String> categories = Converter.stringToSet(jsonObject.getString("categories"));

                String insertQuery = "INSERT INTO "+Book.TABLE_NAME+" (id_book, title, year, summary, authors, categories) " +
                        "VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS";
                // Insert into ScyllaDB
                ResultSet result = session.execute(
                        insertQuery,
                        idBook, title, year, summary, authors, categories);
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

    public static void testBook(){


        // Search by name
        Set<Book> books = Book.searchByName("Zathura");
        System.out.println(books);
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
            set.add(author.getAuthorFormated());
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

    public static Book toBook(Row row){
        Set<String> categories = (Set<String>) row.getObject("categories");
        Set<Map<String, String>> authors1 = (Set<Map<String, String>>) row.getObject("authors");

        Set<Author> authors = new HashSet<Author>();

        for (Map<String, String> a : authors1) {
            authors.add(Author.toAuthor(a));
        }

        return new Book(
            row.getUuid("id_book"),
            row.getString("title"),
            row.getInt("year"),
            row.getString("summary"),
            categories,
            authors
        );
    }


    public void groupByCategories(String category) {
        try (CqlSession session = database.getSession()) {
            Select query = selectFrom(Book.TABLE_NAME).all()
                    .whereColumn("category").isEqualTo(QueryBuilder.literal(category))
                    .allowFiltering();

            SimpleStatement statement = query.build();
            ResultSet rs = session.execute(statement);
            Row row = rs.one();
        }
    }

    public void groupByAuthors(Author category) {
        try (CqlSession session = database.getSession()) {
            Select query = selectFrom(Book.TABLE_NAME).all()
                    .whereColumn("author").isEqualTo(QueryBuilder.literal(category))
                    .allowFiltering();

            SimpleStatement statement = query.build();
            ResultSet rs = session.execute(statement);
            Row row = rs.one();
        }
    }

    public static Set<Book> searchByName(String name) {
        System.out.println();
        Set<Book> books = new HashSet<>();

        try (CqlSession session = database.getSession()) {
            Select query = selectFrom(Book.TABLE_NAME).all()
                    .whereColumn("title").isEqualTo(QueryBuilder.literal(name))
                    .allowFiltering();

            SimpleStatement statement = query.build();
            ResultSet rs = session.execute(statement);

            for (Row row : rs) {
                books.add(Book.toBook(row));
            }

        }
        return books;
    }
}
