from functions import chunk_upload, select

articles = """                              
    INSERT INTO articles VALUES(
        %(id)s,
        %(title)s,
        %(content)s,
        %(type)s,
        %(domain)s,
        %(url)s,
        %(scraped_at)s,
        %(inserted_at)s,
        %(updated_at)s
    );
    """                         
            
authors = """
    INSERT INTO authors VALUES(
        %s,
        %s
    )
    """

meta_keywords = """
    INSERT INTO meta_keywords VALUES(
        %s,
        %s
    )
    """

queries = {
    'authors' : authors,
    'meta_keywords' : meta_keywords,
    'articles': articles
    }

query = """
    SELECT COUNT(*) FROM articles;
    """

nrows = select(query)[0][0]

chunk_upload(
    input_csv="data/results1.csv",
    chunk_size=10,
    queries = queries,
    skip_rows=0
)
