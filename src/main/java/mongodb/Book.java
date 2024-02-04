package mongodb;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class Book extends MongoDBCollection {

    public Book(MongoDatabase database) {
        super(database);
        this.collectionName = "book";
        this.collection = database.getCollection(collectionName, Document.class);
    }

    public void group_by_categories(Document filters) {
        Bson match = Aggregates.match(filters);
        Bson unwind = Aggregates.unwind("$categories");
        Bson group = Aggregates.group("$categories", Accumulators.sum("count", 1));
        List<Bson> pipeline = Arrays.asList(match, unwind, group);

        this.collection.aggregate(pipeline).forEach(printBlock);
    }

    public void copy_number(Document filters) {
        Bson match = Aggregates.match(filters);
        Bson unwind = Aggregates.unwind("$editions");
        Bson unwindCopies = Aggregates.unwind("$editions.copies");
        Bson group = Aggregates.group("$title", Accumulators.sum("totalCopies", 1));
        List<Bson> pipeline = Arrays.asList(match, unwind, unwindCopies, group);

        this.collection.aggregate(pipeline).forEach(printBlock);
    }

    public void worst_book_copies(Document filters) {
        Bson match = Aggregates.match(filters);
        Bson unwind = Aggregates.unwind("$editions");
        Bson unwindCopies = Aggregates.unwind("$editions.copies");
        Bson matchCondition = Aggregates.match(Filters.eq("editions.copies.wear", "Poor"));
        Bson group = Aggregates.group("$title", Accumulators.sum("poorCopies", 1));
        Bson sort = Aggregates.sort(Sorts.descending("poorCopies"));
        Bson limit = Aggregates.limit(1);
        List<Bson> pipeline = Arrays.asList(match, unwind, unwindCopies, matchCondition, group, sort, limit);

        this.collection.aggregate(pipeline).forEach(printBlock);
    }

    public void available_copies(Document filters) {
        Bson match = Aggregates.match(filters);
        Bson unwind = Aggregates.unwind("$editions");
        Bson unwindCopies = Aggregates.unwind("$editions.copies");
        Bson matchAvailable = Aggregates.match(Filters.eq("editions.copies.state", true));
        Bson group = Aggregates.group("$title", Accumulators.sum("availableCopies", 1));
        List<Bson> pipeline = Arrays.asList(match, unwind, unwindCopies, matchAvailable, group);

        this.collection.aggregate(pipeline).forEach(printBlock);
    }

    // Placeholder for printBlock, a Consumer<Document> to output the results of aggregations
    private Consumer<Document> printBlock = document -> System.out.println(document.toJson());

    // Methods requiring cross-collection data or more complex data relationships
    // Conceptual approaches provided; actual implementation would depend on specific application logic and data model

    public void most_read_author(int number) {
        // Stage 1: Unwind the loans array from the readers collection
        Bson unwindLoans = Aggregates.unwind("$loans");

        // Stage 2: Group by the copy_id to count the number of times each copy has been loaned
        Bson groupByCopyId = Aggregates.group("$loans.copy_id", Accumulators.sum("count", 1));

        // Stage 3: Look up the corresponding book documents from the book collection based on copy_id
        Bson lookupBooks = Aggregates.lookup("book", "_id", "editions.copies.copy_id", "book_docs");

        // Stage 4: Unwind the resulting book_docs array
        Bson unwindBooks = Aggregates.unwind("$book_docs");

        // Stage 5: Unwind the authors array from the book documents
        Bson unwindAuthors = Aggregates.unwind("$book_docs.authors");

        // Stage 6: Group by author_id to count the total number of books loaned per author
        Bson groupByAuthor = Aggregates.group("$book_docs.authors.id_author", Accumulators.sum("total_loans", "$count"));

        // Stage 7: Sort the results by total_loans in descending order
        Bson sortByTotalLoans = Aggregates.sortByCount("$total_loans");

        // Stage 8: Limit the results to the specified number
        Bson limitToTopN = Aggregates.limit(number);

        // Combine all stages into a pipeline
        List<Bson> pipeline = Arrays.asList(
                unwindLoans,
                groupByCopyId,
                lookupBooks,
                unwindBooks,
                unwindAuthors,
                groupByAuthor,
                sortByTotalLoans,
                limitToTopN
        );

        // Run the aggregation pipeline against the readers collection
        MongoDatabase database = this.database;
        database.getCollection("readers").aggregate(pipeline).forEach(printDocument());
    }

    // Helper method to print documents
    private Consumer<Document> printDocument() {
        return doc -> System.out.println(doc.toJson());
    }

    public void loan_information(Document whereQuery) {
        // Assuming we're looking for loan information for specific criteria
        Bson match = Aggregates.match(whereQuery);
        List<Bson> pipeline = Arrays.asList(match);

        // Assuming 'this.collection' is the 'readers' collection since it contains the loan data
        this.collection.aggregate(pipeline).forEach(printDocument());
    }

    public void loan_trends(int number) {
        // Stage 1: Unwind the loans array from the readers collection
        Bson unwindLoans = Aggregates.unwind("$loans");

        // Stage 2: Group by the book's copy_id to count the number of times each book has been loaned
        Bson groupByCopyId = Aggregates.group("$loans.copy_id", Accumulators.sum("loanCount", 1));

        // Stage 3: Sort by loanCount in descending order to find the most loaned books
        Bson sortByLoanCount = Aggregates.sort(new Document("loanCount", -1));

        // Stage 4: Limit the results to the top 'number' of loaned books
        Bson limitToTopN = Aggregates.limit(number);

        // Stage 5: Look up the corresponding book documents from the book collection based on copy_id
        Bson lookupBooks = Aggregates.lookup("book", "_id", "editions.copies.copy_id", "book_docs");

        // Stage 6: Unwind the resulting book_docs array (since lookup produces an array of matches)
        Bson unwindBooks = Aggregates.unwind("$book_docs");

        // Combine all stages into a pipeline
        List<Bson> pipeline = Arrays.asList(
                unwindLoans,
                groupByCopyId,
                sortByLoanCount,
                limitToTopN,
                lookupBooks,
                unwindBooks
        );

        // Execute the pipeline against the 'readers' collection to get the loan trends
        MongoDatabase database = this.database;
        database.getCollection("readers").aggregate(pipeline).forEach(printDocument());
    }

}
