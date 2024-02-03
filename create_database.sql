CREATE COLUMNFAMILY IF NOT EXISTS library.Author (firstname_a text, lastname_a text PRIMARY KEY);
INSERT INTO library.Author (firstname_a, lastname_a) values ('george', 'orwell');

CREATE COLUMNFAMILY IF NOT EXISTS library.Book (
    id_book UUID PRIMARY KEY,
    title text,
    year text,
    summary text,
    categories list<text>,
    authors list<frozen<map<text, text>>>
);