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

import java.time.Instant;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONObject;

public class Loan implements CRUD<Loan>, TableOperation {
    public static final String TABLE_NAME = "loan";
    private final static Database database = new Database();
    private UUID idLoan;
    private UUID idBook;
    private UUID idCopy;
    private UUID idReader;
    private String firstname;
    private String lastname;
    private String title;
    private Set<Author> authors;
    private Instant loanDate;
    private Instant expiryDate;
    private Instant dueDate;

    // Constructeur
    public Loan(UUID idLoan, UUID idBook, UUID idCopy, UUID idReader, String firstname, String lastname, String title, Set<Author> authors, Instant loanDate, Instant expiryDate, Instant dueDate) {
        this.idLoan = idLoan;
        this.idBook = idBook;
        this.idCopy = idCopy;
        this.idReader = idReader;
        this.firstname = firstname;
        this.lastname = lastname;
        this.title = title;
        this.authors = authors;
        this.loanDate = loanDate;
        this.expiryDate = expiryDate;
        this.dueDate = dueDate;
    }

    // Getters et Setters pour idLoan
    public UUID getIdLoan() {
        return idLoan;
    }

    public void setIdLoan(UUID idLoan) {
        this.idLoan = idLoan;
    }

    // Getters et Setters pour idBook
    public UUID getIdBook() {
        return idBook;
    }

    public void setIdBook(UUID idBook) {
        this.idBook = idBook;
    }

    // Getters et Setters pour idCopy
    public UUID getIdCopy() {
        return idCopy;
    }

    public void setIdCopy(UUID idCopy) {
        this.idCopy = idCopy;
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

    // Getters et Setters pour title
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    // Getters et Setters pour authors
    public Set<Author> getAuthors() {
        return authors;
    }

    public void setAuthors(Set<Author> authors) {
        this.authors = authors;
    }

    public Set<Map<String, String>> getAuthorsFormated() {
        Set<Map<String, String>> set = new HashSet<>();

        for (Author author : authors) {
            set.add(author.getAuthorFormated());
        }
        return set;
    }

    // Getters et Setters pour loanDate
    public Instant getLoanDate() {
        return loanDate;
    }

    public void setLoanDate(Instant loanDate) {
        this.loanDate = loanDate;
    }

    // Getters et Setters pour expiryDate
    public Instant getExpiryDate() {
        return expiryDate;
    }

    public void setExpiryDate(Instant expiryDate) {
        this.expiryDate = expiryDate;
    }

    // Getters et Setters pour dueDate
    public Instant getDueDate() {
        return dueDate;
    }

    public void setDueDate(Instant dueDate) {
        this.dueDate = dueDate;
    }

    // Méthode pour créer la table Loan
    public static void createTable(CqlSession session) {
        CreateTable createTable = SchemaBuilder.createTable(TABLE_NAME)
                .ifNotExists()
                .withPartitionKey("id_loan", DataTypes.UUID)
                .withClusteringColumn("id_book", DataTypes.UUID)
                .withClusteringColumn("id_copy", DataTypes.UUID)
                .withClusteringColumn("id_reader", DataTypes.UUID)
                .withColumn("firstname", DataTypes.TEXT)
                .withColumn("lastname", DataTypes.TEXT)
                .withColumn("title", DataTypes.TEXT)
                .withColumn("authors",DataTypes.setOf(DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT, true)))
                .withColumn("loan_date", DataTypes.TIMESTAMP)
                .withColumn("expiry_date", DataTypes.TIMESTAMP)
                .withColumn("due_date", DataTypes.TIMESTAMP);

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

                UUID idLoan = Converter.intToUUID(jsonObject.getInt("id_loan"), Loan.TABLE_NAME);
                UUID idBook = Converter.intToUUID(jsonObject.getInt("id_book"), Book.TABLE_NAME);
                UUID idCopy = Converter.intToUUID(jsonObject.getInt("id_copy"), Copy.TABLE_NAME);
                UUID idReader = Converter.intToUUID(jsonObject.getInt("id_reader"), Reader.TABLE_NAME);
                String firstname = jsonObject.getString("firstname");
                String lastname = jsonObject.getString("lastname");
                String title = jsonObject.getString("title");
                Set<Map<String, String>> authors = Converter.stringToAuthors(jsonObject.getString("authors"));
                Instant loanDate = Converter.stringToDate(jsonObject.getString("loan_date"));
                Instant expiryDate = Converter.stringToDate(jsonObject.getString("expiry_date"));
                Instant dueDate = Converter.stringToDate(jsonObject.getString("due_date"));

                String insertQuery = "INSERT INTO "+TABLE_NAME+" (id_loan, id_book, id_copy, id_reader, firstname, lastname, title, authors, loan_date, expiry_date, due_date) " +
                                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";

                // Insert into ScyllaDB
                ResultSet result = session.execute(insertQuery,
                    idLoan, idBook, idCopy, idReader, firstname, lastname, title,
                    authors, loanDate, expiryDate, dueDate);

                if (result.wasApplied()) {
                    count++;
                }
            }
            System.out.println(TABLE_NAME + " : " + count + "/" + jsonArray.length() + " record(s) imported !");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testLoan(){
        // Insert a new loan on one line
        Loan loan = new Loan(
            UUID.fromString("10000000-1000-0000-0000-000000000001"),
            UUID.fromString("00000000-0000-0000-0000-100000000001"),
            UUID.fromString("10000000-0000-0000-0000-100000000001"),
            UUID.fromString("00000000-0000-0000-0000-000000000001"),
            "John",
            "Doe",
            "The Book",
            Set.of(new Author("Mysterious", "Bokk")),
            Converter.stringToDate("2021-01-01"),
            Converter.stringToDate("2021-01-15"),
            Converter.stringToDate("2021-01-30")
        );
        loan.insert();

        // Insert a list of loans
        ArrayList<Loan> loans = new ArrayList<>();
        loans.add(new Loan(
            UUID.fromString("10000000-1000-0000-0000-000000000002"),
            UUID.fromString("00000000-0000-0000-0000-100000000002"),
            UUID.fromString("10000000-0000-0000-0000-100000000002"),
            UUID.fromString("00000000-0000-0000-0000-000000000002"),
            "Jane",
            "Doe",
            "The Book",
            Set.of(new Author("Mysterious", "Bokk")),
            Converter.stringToDate("2021-01-01"),
            Converter.stringToDate("2021-01-15"),
            Converter.stringToDate("2021-01-30")
        ));
        loans.add(new Loan(
            UUID.fromString("10000000-1000-0000-0000-000000000003"),
            UUID.fromString("00000000-0000-0000-0000-100000000003"),
            UUID.fromString("10000000-0000-0000-0000-100000000003"),
            UUID.fromString("00000000-0000-0000-0000-000000000003"),
            "Jack",
            "Doe",
            "The Book",
            Set.of(new Author("Mysterious", "Bokk")),
            Converter.stringToDate("2021-01-01"),
            Converter.stringToDate("2021-01-15"),
            Converter.stringToDate("2021-01-30")
        ));
        Loan.insert(loans);

        // Get a loan
        Loan getLoan = loan.get(UUID.fromString("10000000-1000-0000-0000-000000000001"));
        System.out.println(getLoan);

        // Create an index
        Loan.createIndex("firstname");

        // Get all loans
        List<Loan> allLoans = loan.getAll(null, 10, "id_loan");
        for (Loan l : allLoans) {
            System.out.println(l);
        }

        // Update a loan
        Loan updateLoan = new Loan(
            UUID.fromString("10000000-1000-0000-0000-000000000001"),
            UUID.fromString("00000000-0000-0000-0000-100000000001"),
            UUID.fromString("10000000-0000-0000-0000-100000000001"),
            UUID.fromString("00000000-0000-0000-0000-000000000001"),
            "John",
            "Doe",
            "The Book has been updated",
            Set.of(new Author("Mysterious", "Bokk")),
            Converter.stringToDate("2021-01-01"),
            Converter.stringToDate("2021-01-15"),
            Converter.stringToDate("2021-01-30")
        );
        loan.update(UUID.fromString("10000000-1000-0000-0000-000000000001"), UUID.fromString("00000000-0000-0000-0000-100000000001"), UUID.fromString("10000000-0000-0000-0000-100000000001"), UUID.fromString("00000000-0000-0000-0000-000000000001"), updateLoan);

        // Delete a loan
        loan.delete(UUID.fromString("10000000-1000-0000-0000-000000000001"));
    }

    // Méthode toString() pour l'affichage
    @Override
    public String toString() {
        return "Loan{" +
                "idLoan=" + idLoan +
                ", idBook=" + idBook +
                ", idCopy=" + idCopy +
                ", idReader=" + idReader +
                ", firstname='" + firstname + '\'' +
                ", lastname='" + lastname + '\'' +
                ", title='" + title + '\'' +
                ", authors=" + authors +
                ", loanDate=" + loanDate +
                ", expiryDate=" + expiryDate +
                ", dueDate=" + dueDate +
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
    
    public Loan get(UUID id){
        try (CqlSession session = database.getSession()){
            System.out.println(TABLE_NAME+" get :");
            Select select = selectFrom(TABLE_NAME)
                    .all()
                    .whereColumn("id_loan").isEqualTo(literal(id)).allowFiltering();
            Row row = session.execute(select.build()).one();

            if (row != null) {
                return buildLoanFromRow(row);
            } else {
                return null;
            }
        }
    }

    public List<Loan> getAll(Map<String, Object> whereConditions, int limit, String sortByColumn){
        List<Loan> loans = new ArrayList<>();
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
                loans.add(buildLoanFromRow(row));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return loans;
    }

    public void insert(){
        try (CqlSession session = database.getSession()){

            Insert insert = insertInto(Loan.TABLE_NAME)
                .value("id_loan", literal(this.getIdLoan()))
                .value("id_book", literal(this.getIdBook()))
                .value("id_copy", literal(this.getIdCopy()))
                .value("id_reader", literal(this.getIdReader()))
                .value("firstname", literal(this.getFirstname()))
                .value("lastname", literal(this.getLastname()))
                .value("title", literal(this.getTitle()))
                .value("authors", literal(this.getAuthorsFormated()))
                .value("loan_date", literal(this.getLoanDate()))
                .value("expiry_date", literal(this.getExpiryDate()))
                .value("due_date", literal(this.getDueDate()));

            ResultSet result = session.execute(insert.ifNotExists().build());

            System.out.println(TABLE_NAME + " inserted ? "+ result.wasApplied());
        }
    }
    public static void insert(ArrayList<Loan> loans) {
        for (Loan entity : loans) {
            entity.insert();
        }
    }

    public void update(UUID idLoan, UUID idBook, UUID idCopy, UUID idReader, Loan entity){
        try (CqlSession session = database.getSession()) {
            System.out.println(TABLE_NAME + " update :");
            Update update = QueryBuilder.update(TABLE_NAME)
                .setColumn("firstname", literal(entity.getFirstname()))
                .setColumn("lastname", literal(entity.getLastname()))
                .setColumn("title", literal(entity.getTitle()))
                .setColumn("authors", literal(entity.getAuthorsFormated()))
                .setColumn("loan_date", literal(entity.getLoanDate()))
                .setColumn("expiry_date", literal(entity.getExpiryDate()))
                .setColumn("due_date", literal(entity.getDueDate()))
                .whereColumn("id_loan").isEqualTo(literal(idLoan))
                .whereColumn("id_book").isEqualTo(literal(idBook))
                .whereColumn("id_copy").isEqualTo(literal(idCopy))
                .whereColumn("id_reader").isEqualTo(literal(idReader));

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
                    .whereColumn("id_loan").isEqualTo(literal(id));

            ResultSet result = session.execute(delete.build());
            System.out.println(TABLE_NAME + " deleted ? " + result.wasApplied());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Loan buildLoanFromRow(Row row){
        Set<Map<String, String>> authors1 = (Set<Map<String, String>>) row.getObject("authors");

        Set<Author> authors = new HashSet<Author>();

        for (Map<String, String> a : authors1) {
            authors.add(Author.toAuthor(a));
        }

        return new Loan(
            row.getUuid("id_loan"),
            row.getUuid("id_book"),
            row.getUuid("id_copy"),
            row.getUuid("id_reader"),
            row.getString("firstname"),
            row.getString("lastname"),
            row.getString("title"),
            authors,
            row.getInstant("loan_date"),
            row.getInstant("expiry_date"),
            row.getInstant("due_date")
        );}

}

