package scylla.type;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateType;

public class Author {
    public static final String TYPE_NAME = "author";

    private String firstname;
    private String lastname;

    // Constructeur
    public Author(String firstname, String lastname) {
        this.firstname = firstname;
        this.lastname = lastname;
    }

    // Getters et setters
    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }

    /**
     * Create UDT for Author
     * @param session
     */
    public static void createType(CqlSession session){
        CreateType createAuthorType = SchemaBuilder.createType(TYPE_NAME).ifNotExists()
            .withField("firstname", DataTypes.TEXT)
            .withField("lastname", DataTypes.TEXT);
        session.execute(createAuthorType.build());
        System.out.println("UDT '"+ TYPE_NAME +"' created successfully.");
    }

}
