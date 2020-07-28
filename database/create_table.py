from functions import execute_query

table_creation_query = '''
    DROP TABLE IF EXISTS articles;
    CREATE TABLE articles(
        article_id BIGINT PRIMARY KEY,
        title TEXT,
        content TEXT,
        type TEXT,
        domain TEXT,
        url TEXT,
        scraped_at TIMESTAMP,
        inserted_at TIMESTAMP,
        updated_at TIMESTAMP
    );
    
    DROP TABLE IF EXISTS authors;
    CREATE TABLE authors(
        article_id BIGINT,
        author TEXT
    );


    DROP TABLE IF EXISTS meta_keywords;
    CREATE TABLE meta_keywords(
        article_id BIGINT,
        meta_keyword TEXT 
    );

    DROP TABLE IF EXISTS tags;
    CREATE TABLE tags(
        article_id BIGINT,
        tag TEXT
    )


'''

# commented out to avoid overwriting :) 
#execute_query(table_creation_query)