package scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import scylla.type.Author;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

import java.util.Date;
import java.util.Set;
import java.util.UUID;

public class Loan {
    public static final String TABLE_NAME = "loan";

    private UUID idLoan;
    private UUID idBook;
    private UUID idCopy;
    private UUID idReader;
    private String firstname;
    private String lastname;
    private String title;
    private Set<Author> authors;
    private Date loanDate;
    private Date expiryDate;
    private Date dueDate;

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

    // Getters et Setters pour loanDate
    public Date getLoanDate() {
        return loanDate;
    }

    public void setLoanDate(Date loanDate) {
        this.loanDate = loanDate;
    }

    // Getters et Setters pour expiryDate
    public Date getExpiryDate() {
        return expiryDate;
    }

    public void setExpiryDate(Date expiryDate) {
        this.expiryDate = expiryDate;
    }

    // Getters et Setters pour dueDate
    public Date getDueDate() {
        return dueDate;
    }

    public void setDueDate(Date dueDate) {
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
                .withColumn("authors", DataTypes.setOf(SchemaBuilder.udt(Author.TYPE_NAME, true)))
                .withColumn("loan_date", DataTypes.DATE)
                .withColumn("expiry_date", DataTypes.DATE)
                .withColumn("due_date", DataTypes.DATE);

        session.execute(createTable.build());
        System.out.println("Table '"+TABLE_NAME+"' created successfully.");
    }
}

