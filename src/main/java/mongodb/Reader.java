package mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import mongodb.MongoDBCollection;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Reader extends MongoDBCollection {

    public Reader(MongoDatabase database) {
        super(database);
        this.collectionName = "reader";
        this.collection = database.getCollection(collectionName);
    }

    public void loaner_information(Document whereQuery) {
        FindIterable<Document> readers = collection.find(whereQuery);
        readers.forEach(printDocument());
    }

    public void late_returns(Document whereQuery) {
        Bson filter = Filters.and(
                whereQuery,
                Filters.lt("loans.expiry_date", new Date()),
                Filters.eq("loans.return_date", "")
        );

        FindIterable<Document> readers = collection.find(filter);
        readers.forEach(printDocument());
    }

    public void loan_count_by_day(Document filters) {
        FindIterable<Document> readers = collection.find(filters);
        Map<Date, Long> loanCountByDay = new HashMap<>();

        for (Document reader : readers) {
            List<Document> loans = reader.getList("loans", Document.class);
            for (Document loan : loans) {
                Date loanDate = loan.getDate("loan_date");
                loanCountByDay.put(loanDate, loanCountByDay.getOrDefault(loanDate, 0L) + 1);
            }
        }

        loanCountByDay.forEach((date, count) ->
                System.out.println("Date: " + date + ", Number of Loans: " + count));
    }

    public boolean can_loan_copies(String id_reader, int nb_wanted_loan) {
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
