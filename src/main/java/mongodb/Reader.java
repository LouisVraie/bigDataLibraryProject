package mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Reader extends MongoDBCollection {

    Reader(MongoDatabase database) {
        super(database);
        this.collectionName = "reader";
        this.collection = database.getCollection(collectionName);
    }

    /* Other Méthods */

    /*
    reader collection :
    {
        "id_reader": "reader_id",
        "firstname": "Alice",
        "lastname": "Smith",
        "birth_date": "1980-02-02",
        "street": "123 Library Street",
        "city": "Booktown",
        "postal_code": "12345",
        "email": "alice.smith@email.com",
        "phone_nb": "123-456-7890",
        "loans": [
            {
            "loan_date": "2023-01-01",
            "expiry_date": "2023-01-15",
            "return_date": "",
            "copy_id": "copy_id"
            }
        ]
    }
    */


    // print the current number of loan for each selected reader
    public void loan_count_by_reader(Document filters){
        System.out.println(" \n #### Loan count by reader ################################");
        List<Document> documents = collection.find(filters).into(List.of());
        Map<String, Long> loanCountByReader = documents.stream()
                .flatMap(doc -> doc.getList("loans", Document.class).stream())
                .filter(loan -> loan.getString("return_date").isEmpty())
                .collect(Collectors.groupingBy(
                        loan -> loan.getString("id_reader"),
                        Collectors.counting()
                ));

        for (Map.Entry<String, Long> entry : loanCountByReader.entrySet()) {
            System.out.println("Reader ID: " + entry.getKey() + ", Number of Loans with return_date = \"\": " + entry.getValue());
        }
    }

    public void loaner_information(Document whereQuery){} // print information about reader who loaned book
    public void late_returns(Document whereQuery){} // print the reader with the book that is in late
    public void loan_count_by_day(Document filters){} // print the number of loan by date
    public boolean can_loan_copies(String id_reader, int nb_wanted_loan){} // True if the reader can loan "nb_wanted_loan" copies

}
