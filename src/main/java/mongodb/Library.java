package mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Arrays;

public class Library {

    public static void main(String[] args) {
        // Replace the placeholder with your MongoDB deployment's connection string
        String uri = "mongodb+srv://antomine:AIITHht7ULqgoWnM@cluster0.tm7wn5a.mongodb.net/?retryWrites=true&w=majority";

        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("Library");

            // Instantiate your Book and Reader classes
            Book book = new Book(database);

            // Create a new book
            System.out.println(" \n\n\n #### Insert test ################################");
            Document newBook = new Document("title", "New Book").append("authors", Arrays.asList(new Document("lastname", "Doe").append("firstname", "John"))).append("year", 2021).append("categories", Arrays.asList("Fiction"));
            book.insertOne(newBook);

            // Read/find the book by title
            System.out.println(" \n\n\n #### Get Test ################################");
            book.get(new Document("title", "New Book"), new Document(), new Document());

            // Update the book's year
            System.out.println(" \n\n\n #### Update Test ################################");
            book.update(new Document("title", "New Book"), new Document("$set", new Document("year", 2022)));
            book.get(new Document("title", "New Book"), new Document(), new Document());

            // Delete the book
            System.out.println(" \n\n\n #### Delete Test ################################");
            book.delete(new Document("title", "New Book"));

            // Group by categories, looking for books in the "Drama" category
            Document groupByCategoriesFilter = new Document();
            book.group_by_categories(groupByCategoriesFilter);

            // Copy number for the book titled "Heaven"
            Document copyNumberFilter = new Document("title", "Heaven");
            book.copy_number(copyNumberFilter);

            // Find the worst condition book copies among all books
            Document worstBookCopiesFilter = new Document(); // No filter needed for the worst condition across all books
            book.worst_book_copies(worstBookCopiesFilter);

            // Available copies for the book "Orphanage, The (Orfanato, El)"
            Document availableCopiesFilter = new Document("title", "Orphanage, The (Orfanato, El)");
            book.available_copies(availableCopiesFilter);

            // Most read author among all books, top 5
            Document mostReadAuthorFilter = new Document(); // No specific filter, we want the top 5 among all books
            book.most_read_author(mostReadAuthorFilter, 5);

            // Loan information for the book "Heaven"
            Document loanInformationFilter = new Document("title", "Heaven");
            book.loan_information(loanInformationFilter);

            // Loan trends, assuming top 5
            Document loanTrendsFilter = new Document(); // No specific filter for trends, assuming top 5
            book.loan_trends(loanTrendsFilter, 5);


            /**
             * Second part
             */


            // Instantiate the Reader class
            Reader reader = new Reader(database);

            System.out.println(" \n\n\n #### Insert Test ################################");
            Document newReader = new Document("firstname", "Alice").append("lastname", "Smith").append("email", "alice.smith@email.com").append("birth_date", "1980-02-02").append("loans", Arrays.asList()); // Assuming the reader starts with no loans
            reader.insertOne(newReader);

            System.out.println(" \n\n\n #### Get Test ################################");
            // Assuming 'get' method is similar to 'find' and returns one or more documents based on a query
            reader.get(new Document("lastname", "Smith"), new Document(), new Document());


            System.out.println(" \n\n\n #### Update Test ################################");
            reader.update(new Document("lastname", "Smith"), new Document("$set", new Document("email", "alice_updated@email.com")));
            // To verify the update, get the reader again
            reader.get(new Document("lastname", "Smith"), new Document(), new Document());


            System.out.println(" \n\n\n #### Delete Test ################################");
            reader.delete(new Document("lastname", "Smith"));


            // For all readers, print the number of loans realized
            Document readerFilters = new Document(); // No filter means it applies to all readers
            reader.loan_count_by_reader(readerFilters);

            // Print the information of the reader who has 44 as an ID
            Document readerSpecificFilter = new Document("id_reader", 44);
            reader.loaner_information(readerSpecificFilter);

            // Print all readers who have late returns
            reader.late_returns(new Document()); // Assuming late_returns handles the logic internally

            // Print the loan count by day for a specific reader
            Document loanCountByDayFilter = new Document("id_reader", 5);
            reader.loan_count_by_day(loanCountByDayFilter);

            // Check if a specific reader can loan 6 more books
            boolean canLoan = reader.can_loan_copies("5", 6);
            System.out.println("Reader ID 5 can loan more copies: " + canLoan);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
