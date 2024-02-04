package mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import mongodb.MongoDBCollection;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Reader extends MongoDBCollection {

    public Reader(MongoDatabase database) {
        super(database);
        this.collectionName = "reader";
        this.collection = database.getCollection(collectionName);
    }

    public void loan_count_by_reader(Document filters) {
        System.out.println(" \n\n\n #### Loan count by reader ################################");
        // Use a mutable list
        List<Document> documents = new ArrayList<>();
        collection.find(filters).into(documents);

        documents.forEach(doc -> {
            // Fetching 'id_reader' as an Integer since it's stored as an Integer
            Integer readerId = doc.getInteger("id_reader"); // Correctly use getInteger for an integer ID
            if (readerId != null) {
                long loanCount = Optional.ofNullable(doc.getList("loans", Document.class))
                        .orElse(Collections.emptyList()) // Use an empty list if 'loans' is null
                        .stream()
                        .filter(loan -> {
                            Object dueDate = loan.get("due_date");
                            // Check if due_date is either null or an empty string, regardless of its type
                            return dueDate == null || (dueDate instanceof String && ((String) dueDate).isEmpty());
                        })
                        .count(); // Count these loans

                System.out.println("Reader ID: " + readerId + ", Number of Loans without due_date: " + loanCount);
            }
        });
    }




    public void loaner_information(Document whereQuery) {
        System.out.println(" \n\n\n #### Loaner information ################################");
        FindIterable<Document> readers = collection.find(whereQuery);
        readers.forEach(printDocument());
    }

    public void late_returns(Document whereQuery) {
        System.out.println(" \n\n\n #### Late returns ################################");
        Bson filter = Filters.and(
                whereQuery,
                Filters.lt("loans.expiry_date", new Date()),
                Filters.eq("loans.return_date", "")
        );

        FindIterable<Document> readers = collection.find(filter);
        readers.forEach(printDocument());
    }

    public void loan_count_by_day(Document filters) {
        System.out.println(" \n\n\n #### Loan count by days ################################");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd"); // Adjust the format to match your date strings
        FindIterable<Document> readers = collection.find(filters);
        Map<Date, Long> loanCountByDay = new HashMap<>();

        for (Document reader : readers) {
            List<Document> loans = reader.getList("loans", Document.class);
            for (Document loan : loans) {
                String loanDateString = loan.getString("loan_date");
                try {
                    Date loanDate = dateFormat.parse(loanDateString); // Parse the string to a Date
                    loanCountByDay.put(loanDate, loanCountByDay.getOrDefault(loanDate, 0L) + 1);
                } catch (ParseException e) {
                    // Handle the case where the date string cannot be parsed
                    System.err.println("Error parsing date string: " + loanDateString);
                }
            }
        }

        loanCountByDay.forEach((date, count) ->
                System.out.println("Date: " + date + ", Number of Loans: " + count));
    }

    public boolean can_loan_copies(String id_reader, int nb_wanted_loan) {
        System.out.println(" \n\n\n #### Can loand copies ################################");
        Document reader = collection.find(new Document("id_reader", id_reader)).first();
        if (reader == null) return false; // Reader not found

        long currentLoans = reader.getList("loans", Document.class).stream()
                .filter(loan -> loan.getString("return_date").isEmpty())
                .count();

        // Assume a business logic that a reader can have a maximum of 5 concurrent loans
        int maxLoans = 5;
        return (currentLoans + nb_wanted_loan) <= maxLoans;
    }

    // Helper method to print documents
    private Consumer<Document> printDocument() {
        return doc -> System.out.println(doc.toJson());
    }
}
