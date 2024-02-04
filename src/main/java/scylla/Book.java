package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.*;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;

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
        // Insert a book
        Book book = new Book(UUID.fromString("00000000-0000-0000-0000-100000000000"), "The Hobbit", 1937, "The Hobbit is a children's fantasy novel by English author J. R. R. Tolkien.", Set.of("Fantasy"), Set.of(new Author("J. R. R.", "Tolkien")));
        book.insert();

        // Insert multiple books
        ArrayList<Book> books = new ArrayList<>();
        books.add(new Book(UUID.fromString("00000000-0000-0000-0000-100000000001"), "Zathura", 2002, "Zathura is an illustrated children's book by the American author Chris Van Allsburg.", Set.of("Children's literature"), Set.of(new Author("Chris", "Van Allsburg"))));
        books.add(new Book(UUID.fromString("00000000-0000-0000-0000-100000000002"), "The Polar Express", 1985, "The Polar Express is a children's book written and illustrated by Chris Van Allsburg.", Set.of("Children's literature"), Set.of(new Author("Chris", "Van Allsburg"))));
        books.add(new Book(UUID.fromString("00000000-0000-0000-0000-100000000003"), "Jumanji", 1981, "Jumanji is a 1981 fantasy children's picture book, written and illustrated by the American author Chris Van Allsburg.", Set.of("Children's literature"), Set.of(new Author("Chris", "Van Allsburg"))));
        Book.insert(books);

        // Get a book by id
        Book getBook = book.get(UUID.fromString("00000000-0000-0000-0000-100000000000"));
        System.out.println(getBook);

        // Get all books
        List<Book> allBooks = book.getAll(null, 10, "title");
        for (Book b : allBooks) {
            System.out.println(b);
        }

        // Update a book
        Book updateBook = new Book(UUID.fromString("00000000-0000-0000-0000-100000000000"), "The Hobbit", 1937, "The Hobbit is a children's fantasy novel by English author J. R. R. Tolkien.", Set.of("Fantasy"), Set.of(new Author("J. R. R.", "Tolkien")));
        updateBook.update(UUID.fromString("00000000-0000-0000-0000-100000000000"), updateBook);

        // Delete a book
        book.delete(UUID.fromString("00000000-0000-0000-0000-100000000000"));
        
        // Search by name
        Set<Book> searchBooks = Book.searchByName("Zathura");
        System.out.println(searchBooks);
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
        try (CqlSession session = database.getSession()){
            System.out.println(TABLE_NAME+" get :");
            Select select = selectFrom(TABLE_NAME)
                    .all()
                    .whereColumn("id_book").isEqualTo(literal(id)).allowFiltering();
            Row row = session.execute(select.build()).one();

            if (row != null) {
                return buildBookFromRow(row);
            } else {
                return null;
            }
        }
    }

    public List<Book> getAll(Map<String, Object> whereConditions, int limit, String sortByColumn){
        List<Book> books = new ArrayList<>();
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
                books.add(buildBookFromRow(row));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return books;
    }

    public void insert(){
        try (CqlSession session = database.getSession()){

            Insert insert = insertInto(Book.TABLE_NAME)
                .value("id_book", literal(this.getIdBook()))
                .value("title", literal(this.getTitle()))
                .value("year", literal(this.getYear()))
                .value("summary", literal(this.getSummary()))
                .value("categories", literal(this.getCategories()))
                .value("authors", literal(this.getAuthorsFormated()));

            ResultSet result = session.execute(insert.ifNotExists().build());

            System.out.println("Book inserted ? "+ result.wasApplied());
        }
    }
    public static void insert(ArrayList<Book> books) {
        for (Book entity : books) {
            entity.insert();
        }
    }

    public void update(UUID id, Book entity){
        try (CqlSession session = database.getSession()) {
            System.out.println(TABLE_NAME + " update :");
            Update update = QueryBuilder.update(TABLE_NAME)
                .setColumn("title", literal(entity.getTitle()))
                .setColumn("year", literal(entity.getYear()))
                .setColumn("summary", literal(entity.getSummary()))
                .setColumn("categories", literal(entity.getCategories()))
                .setColumn("authors", literal(entity.getAuthorsFormated()))
                .whereColumn("id_book").isEqualTo(literal(id));

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
                    .whereColumn("id_book").isEqualTo(literal(id));

            ResultSet result = session.execute(delete.build());
            System.out.println(TABLE_NAME + " deleted ? " + result.wasApplied());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Book buildBookFromRow(Row row){
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
        System.out.println(TABLE_NAME + " : searchByName");
        Set<Book> books = new HashSet<>();

        try (CqlSession session = database.getSession()) {
            Select query = selectFrom(Book.TABLE_NAME).all()
                    .whereColumn("title").isEqualTo(QueryBuilder.literal(name))
                    .allowFiltering();

            SimpleStatement statement = query.build();
            ResultSet rs = session.execute(statement);

            for (Row row : rs) {
                books.add(Book.buildBookFromRow(row));
            }

        }
        return books;
    }
}
