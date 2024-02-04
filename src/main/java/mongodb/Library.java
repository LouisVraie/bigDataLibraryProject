package mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class Library {


    public static void main(String[] args) {

        // Replace the placeholder with your MongoDB deployment's connection string
        String uri = "mongodb+srv://antomine:AIITHht7ULqgoWnM@cluster0.tm7wn5a.mongodb.net/?retryWrites=true&w=majority";

        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("Library");






            // Create 'book' collection
            MongoCollection<Document> bookCollection = database.getCollection("book");
            Document bookDocument = new Document("title", "The Great Gatsby")
                    .append("author", "F. Scott Fitzgerald")
                    .append("year", 1925);
            bookCollection.insertOne(bookDocument);

            // Create 'reader' collection
            MongoCollection<Document> readerCollection = database.getCollection("reader");
            Document readerDocument = new Document("name", "Alice Smith")
                    .append("age", 30)
                    .append("city", "Booktown");
            readerCollection.insertOne(readerDocument);

            // Display documents from 'book' collection
            bookCollection.find().forEach(document -> System.out.println("Book: " + document.toJson()));

            // Display documents from 'reader' collection
            readerCollection.find().forEach(document -> System.out.println("Reader: " + document.toJson()));
        }
    }
}
