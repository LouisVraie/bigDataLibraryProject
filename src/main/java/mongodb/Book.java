package mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Book extends MongoDBCollection {

    public Book(MongoDatabase database) {
        super(database);
        this.collectionName = "book";
        this.collection = database.getCollection(collectionName, Document.class);
    }

    public void group_by_categories(Document filters) {
        System.out.println(" \n\n\n #### Group by categories ################################");
        Bson match = Aggregates.match(filters);
        Bson unwind = Aggregates.unwind("$categories");
        Bson group = Aggregates.group("$categories", Accumulators.sum("count", 1));
        List<Bson> pipeline = Arrays.asList(match, unwind, group);

        this.collection.aggregate(pipeline).forEach(printBlock);
    }

    public void copy_number(Document filters) {
        System.out.println(" \n\n\n #### Copy number ################################");
        Bson match = Aggregates.match(filters);
        Bson unwind = Aggregates.unwind("$editions");
        Bson unwindCopies = Aggregates.unwind("$editions.copies");
        Bson group = Aggregates.group("$title", Accumulators.sum("totalCopies", 1));
        List<Bson> pipeline = Arrays.asList(match, unwind, unwindCopies, group);

        this.collection.aggregate(pipeline).forEach(printBlock);
    }

    public void worst_book_copies(Document filters) {
        System.out.println(" \n\n\n #### Worst book copies ################################");
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
        System.out.println(" \n\n\n #### Available copies ################################");
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

    public void most_read_author(Document mostReadAuthorFilter, int number) {
        System.out.println(" \n\n\n #### Most read author ################################");

        // Step 1: Count loans per copy from the readers collection
        MongoCollection<Document> readersCollection = database.getCollection("readers");
        Map<String, Long> copyLoanCount = new HashMap<>();
        readersCollection.find(mostReadAuthorFilter).forEach((Consumer<Document>) reader -> {
            List<Document> loans = reader.getList("loans", Document.class, Collections.emptyList());
            for (Document loan : loans) {
                String copyId = loan.getString("copy_id");
                copyLoanCount.put(copyId, copyLoanCount.getOrDefault(copyId, 0L) + 1);
            }
        });

        // Step 2: Aggregate loans per book based on copy counts
        Map<String, Long> bookLoanCount = new HashMap<>();
        collection.find().forEach((Consumer<Document>) book -> {
            List<Document> editions = book.getList("editions", Document.class, Collections.emptyList());
            for (Document edition : editions) {
                List<Document> copies = edition.getList("copies", Document.class, Collections.emptyList());
                for (Document copy : copies) {
                    // Handle copy_id as Integer if stored as such
                    Object copyIdObj = copy.get("copy_id");
                    String copyId;
                    if (copyIdObj instanceof Integer) {
                        // Convert Integer to String
                        copyId = String.valueOf(copyIdObj);
                    } else if (copyIdObj instanceof String) {
                        // Use as is if already a String
                        copyId = (String) copyIdObj;
                    } else {
                        // Skip or handle other unexpected types appropriately
                        continue;
                    }
                    Long loans = copyLoanCount.getOrDefault(copyId, 0L);
                    String title = book.getString("title");
                    bookLoanCount.put(title, bookLoanCount.getOrDefault(title, 0L) + loans);
                }
            }
        });



        // Step 3: Identify the most read authors based on top 'number' of books
        List<Map.Entry<String, Long>> topBooks = bookLoanCount.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(number)
                .collect(Collectors.toList());

        for (Map.Entry<String, Long> entry : topBooks) {
            String title = entry.getKey();
            collection.find(new Document("title", title)).forEach((Consumer<Document>) book -> {
                // Assuming each book document contains an 'authors' array of documents
                List<Document> authors = book.getList("authors", Document.class, Collections.emptyList());
                // Creating a string representation of all authors
                String authorsStr = authors.stream()
                        .map(author -> author.getString("firstname") + " " + author.getString("lastname"))
                        .collect(Collectors.joining(", "));
                System.out.println("Book: " + title + ", Authors: " + authorsStr);
            });
        }

    }

    // Helper method to print documents
    private Consumer<Document> printDocument() {
        return doc -> System.out.println(doc.toJson());
    }

    public void loan_information(Document whereQuery) {
        System.out.println(" \n\n\n #### Loan information ################################");
        // Assuming we're looking for loan information for specific criteria
        Bson match = Aggregates.match(whereQuery);
        List<Bson> pipeline = Arrays.asList(match);

        // Assuming 'this.collection' is the 'readers' collection since it contains the loan data
        this.collection.aggregate(pipeline).forEach(printDocument());
    }

    public void loan_trends(Document whereQuery, int number) {
        System.out.println(" \n\n\n #### Loan trends ################################");

        // Étape 1 : Comptage des emprunts par copie
        MongoCollection<Document> readersCollection = database.getCollection("readers");
        Map<String, Long> copyLoanCount = new HashMap<>();
        readersCollection.find(whereQuery).forEach((Consumer<Document>) reader -> {
            List<Document> loans = reader.getList("loans", Document.class, Collections.emptyList());
            for (Document loan : loans) {
                String copyId = String.valueOf(loan.get("copy_id"));
                copyLoanCount.merge(copyId, 1L, Long::sum);
            }
        });

        // Étape 2 : Aggrégation des emprunts par livre
        Map<String, Long> bookLoanCount = new HashMap<>();
        collection.find(new Document("_id", new Document("$in", new ArrayList<>(copyLoanCount.keySet()))))
                .forEach((Consumer<Document>) book -> {
                    long bookLoans = book.getList("editions", Document.class, Collections.emptyList()).stream()
                            .flatMap(edition -> ((List<Document>) edition.get("copies")).stream())
                            .filter(copy -> copyLoanCount.containsKey(String.valueOf(copy.get("copy_id"))))
                            .mapToLong(copy -> copyLoanCount.get(String.valueOf(copy.get("copy_id"))))
                            .sum();

                    if (bookLoans > 0) {
                        String title = book.getString("title");
                        bookLoanCount.put(title, bookLoanCount.getOrDefault(title, 0L) + bookLoans);
                    }
                });

        // Étape 3 : Sélection et affichage des "n" livres les plus empruntés
        bookLoanCount.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(number)
                .forEach(entry -> System.out.println("Book: " + entry.getKey() + ", Loans: " + entry.getValue()));
    }

}
