package mongodb;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.util.List;

public class Book extends MongoDBCollection{

    Book(MongoDatabase database) {
        super(database);
        this.collectionName = "book";
        this.collection = database.getCollection(collectionName);
    }

    /* Other Méthods */
    public void group_by_categories(Document filters){}  // group books by their category
    public void copy_number(Document filters){} // print the number of copy for each book selected
    public void worst_book_copies(Document filters){} // print the book which have the most copies of poor condition
    public void available_copies(Document filters){} // print the number of copies available for selected book
    public void most_read_author(Document whereQuery, int number){} // print the "number" most read author
    public void loan_information(Document whereQuery){} // print information about loaned copies
    public void loan_trends(Document whereQuery, int number){} // print the "number" most read book
}