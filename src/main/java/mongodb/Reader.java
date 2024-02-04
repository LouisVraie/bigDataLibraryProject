package mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;

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
                long loanCount = Optional.ofNullable(doc.getList("loans", Document.class)).orElse(Collections.emptyList()) // Use an empty list if 'loans' is null
                        .stream().filter(loan -> {
                            Object dueDate = loan.get("due_date");
                            // Check if due_date is either null or an empty string, regardless of its type
                            return dueDate == null || (dueDate instanceof String && ((String) dueDate).isEmpty());
                        }).count(); // Count these loans

                System.out.println("Reader ID: " + readerId + ", Number of current loans : " + loanCount);
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
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        FindIterable<Document> readers = collection.find(whereQuery);

        readers.forEach((Consumer<Document>) reader -> {
            List<Document> loans = reader.getList("loans", Document.class, Collections.emptyList());
            for (Document loan : loans) {
                try {
                    Date dueDate = null;
                    Date expiryDate = null;
                    Object dueDateObj = loan.get("due_date");
                    Object expiryDateObj = loan.get("expiry_date");

                    // Handle due_date if it's a string that needs to be parsed
                    if (dueDateObj instanceof String && !((String) dueDateObj).isEmpty()) {
                        try {
                            dueDate = dateFormat.parse((String) dueDateObj);
                        } catch (ParseException e) {
                            System.err.println("Error parsing due_date string: " + dueDateObj);
                            continue; // Skip this loan if the date can't be parsed
                        }
                    }

                    // Similar parsing for expiry_date if necessary
                    if (expiryDateObj instanceof String && !((String) expiryDateObj).isEmpty()) {
                        try {
                            expiryDate = dateFormat.parse((String) expiryDateObj);
                        } catch (ParseException e) {
                            System.err.println("Error parsing expiry_date string: " + expiryDateObj);
                            continue; // Skip this loan if the date can't be parsed
                        }
                    }

                    // Assuming expiryDate is always a Date object; if not, similar parsing would be required
                    if (expiryDate == null) {
                        // Handle cases where expiry_date is directly a Date or null
                        expiryDate = loan.getDate("expiry_date");
                    }

                    // Now compare the dates
                    if (dueDate != null && expiryDate != null && dueDate.after(expiryDate)) {
                        Integer readerId = reader.getInteger("id_reader");
                        System.out.println("Reader ID: " + readerId + " has a late return: " + loan.toJson());
                    }
                } catch (Exception e) {
                    System.err.println("Error processing loan dates for reader: " + e.getMessage());
                }
            }
        });
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

        loanCountByDay.forEach((date, count) -> System.out.println("Date: " + date + ", Number of Loans: " + count));
    }

    public boolean can_loan_copies(String id_reader, int nb_wanted_loan) {
        System.out.println(" \n\n\n #### Can loand copies ################################");
        Document reader = collection.find(new Document("id_reader", id_reader)).first();
        if (reader == null) return false; // Reader not found

        long currentLoans = reader.getList("loans", Document.class).stream().filter(loan -> loan.getString("return_date").isEmpty()).count();

        // Assume a business logic that a reader can have a maximum of 5 concurrent loans
        int maxLoans = 5;
        return (currentLoans + nb_wanted_loan) <= maxLoans;
    }

    // Helper method to print documents
    private Consumer<Document> printDocument() {
        return doc -> System.out.println(doc.toJson());
    }
}
