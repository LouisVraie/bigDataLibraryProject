package mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.Iterator;
import java.util.List;

public abstract class MongoDBCollection {

    protected MongoDatabase database;
    protected MongoCollection<Document> collection;
    protected String collectionName;
    protected final String dbName = "Library";

    /**
     * Constructor
     * @param database : the database to connect to
     */
    MongoDBCollection(MongoDatabase database) {
        this.database = database;
    }

    /* CRUD Méthods */

    /**
     * FV1 : Insert one reader in the collection.
     * @param reader : the reader to insert
     */
    public void insertOne(Document reader) {
        collection.insertOne(reader);
        System.out.print(collectionName+" added successfully");
    }

    /**
     * FV2 : Insert many readers in the collection.
     * @param readers : the list of readers to insert
     */
    public void insertMany(List<Document> readers) {
        collection.insertMany(readers);
        System.out.print(collectionName+"s added successfully");
    }

    /**
     * FV3 : display the reader(s) of the collection.
     * @param it : the iterator to display
     * @param message : the message to display
     */
    private void displayIterator(Iterator it, String message){
        System.out.println(" \n #### "+ message + " ################################");
        while(it.hasNext()) {
            System.out.println(it.next());

        }
    }

    /**
     * FV4 : Delete the specified reader(s) of the collection.
     * @param filters : the reader matching the filter will be deleted
     */
    public void delete(Document filters) {
        System.out.println("\n\n\n*********** In " + collectionName + "*****************");
        FindIterable<Document> listReader;
        Iterator it;

        listReader=collection.find(filters).sort(new Document("_id", 1));
        it = listReader.iterator();// Getting the iterator
        this.displayIterator(it, "In " + collectionName + " before deletion");

        collection.deleteMany(filters);
        listReader=collection.find(filters).sort(new Document("_id", 1));
        it = listReader.iterator();// Getting the iterator
        this.displayIterator(it, "In " + collectionName + " after deletion");
    }

    /**
     * FV5 : Update the specified reader(s) of the collection.
     * @param whereQuery : the reader matching the filter will be updated
     * @param updateExpressions : the new values of the reader
     */
    public void update(Document whereQuery, Document updateExpressions) {
        System.out.println("\n\n\n*********** dans " + collectionName + " *****************");

        UpdateResult updateResult = collection.updateMany(whereQuery, updateExpressions);

        System.out.println("\nResultat update : "
                +"getUpdate id: "+updateResult
                +" getMatchedCount : "+updateResult.getMatchedCount()
                +" getModifiedCount : "+updateResult.getModifiedCount()
        );
    }

    /**
     * FV6 : Get the specified reader(s) of the collection.
     * @param whereQuery : the reader matching the filter will be returned
     * @param projectionFields : the fields to be returned
     * @param sortFields : the fields to be sorted
     */
    public void get(Document whereQuery, Document projectionFields, Document sortFields){
        System.out.println("\n\n\n*********** dans " + collectionName + " *****************");
        FindIterable<Document> listReader=collection.find(whereQuery).sort(sortFields).projection(projectionFields);

        // Getting the iterator
        Iterator it = listReader.iterator();
        while(it.hasNext()) {
            System.out.println(it.next());
        }
    }

}
