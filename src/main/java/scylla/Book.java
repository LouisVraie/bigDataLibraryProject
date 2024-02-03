package scylla;

import com.datastax.driver.core.utils.UUIDs;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.Literal;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;

// For DML queries, such as SELECT
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

// For DDL queries, such as CREATE TABLE


public class Book {

    private static Database database = new Database();

    public String id_book;
    public String title;
    public String year;
    public String summary;
    public List<String> categories;
    public List<Author> authors;

    public Book(String id_book, String title, String year, String summary, List<String> categories, List<Author> author){
        this.id_book = id_book;
        this.title = title;
        this.year = year;
        this.summary = summary;
        this.categories = categories;
        this.authors = author;
    }

    public static Book getBookByTitle(String title){
        String id_book;
        String titre;
        String year;
        String summary;
        List<String> categories;
        List<Author> authors;

        try (CqlSession session = database.getSession()) {
            Select query = selectFrom("Book").all()
                    .whereColumn("title").isEqualTo(QueryBuilder.literal(title))
                    .allowFiltering();

            SimpleStatement statement = query.build();
            ResultSet rs = session.execute(statement);
            Row row = rs.one();

            System.out.println(row.getFormattedContents());

            id_book = row.getString("id_book");
            titre = row.getString("title");
            year = row.getString("year");
            summary = row.getString("summary");

            categories = row.getList("categories", String.class);

            List<Map> authorRows = row.getList("authors", Map.class);
            authors = authorRows.stream()
                    .map(m -> new Author(m.get("firstname").toString(), m.get("lastname").toString()))
                    .collect(Collectors.toList());

        }

        Book book = new Book(id_book, titre, year, summary, categories, authors);

        return book;
    }

    public static void addBook(Book book) {
        try (CqlSession session = database.getSession()) {
            String id_book = UUIDs.timeBased().toString();
            String title = book.title;
            String year = book.year;
            String summary = book.summary;
            List<String> categories = book.categories;
            List<Author> authors = book.authors;

            for (Author author : book.authors) {
                ImmutableMap<String, String> stringStringImmutableMap = ImmutableMap.of("firstname", author.firstname, "lastname", author.lastname);
                authors.add(stringStringImmutableMap);
            }

            List<Literal> list = new ArrayList<>();
            for (String category : categories) {
                Literal literal = literal(category);
                list.add(literal);
            }
            Insert insert = insertInto("Book")
                    .value("id_book", literal(id_book))
                    .value("title", literal(title))
                    .value("year", literal(year))
                    .value("summary", literal(summary))
                    .value("categories", (Term) udt(list.toString()))
                    .value("authors", (Term) udt((CqlIdentifier) authors.stream().map(author -> raw(String.valueOf(author))).collect(Collectors.toList())));


            SimpleStatement statement = insert.build();
            session.execute(statement);

            System.out.println("Added book: " + book.title);
        }
    }

    public static void main(String[] args) {
        List<Author> authors = new ArrayList<Author>();
        authors.add(new Author("joseph", "joestar"));

        List<String> categories = new ArrayList<String>();
        categories.add("fiction");

        Book b = new Book("1", "The Great Book", "2022", "a book", categories, authors);
        addBook(b);

        Book book = getBookByTitle("The Great Book");
        System.out.println(book.toString());
    }
}
