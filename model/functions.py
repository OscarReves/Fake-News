import psycopg2
import csv
import re
import pandas as pd

from pathlib import Path

connection = psycopg2.connect(
    user = "postgres",
    password = "password",
    host = "127.0.0.1",
    port = "5432",
    database = "postgres")

def clean_text(text):               # clean_text cleans text (duh)
    
    ### lowercase 
    text = text.lower()

    ### special characters
    chars = r"(,|.|!|\?|:|;)"
    text = re.sub(',','',text)

    ### deleting multiple whitespaces, tabs and newlines 
    text = re.sub('(\s{1,}|\\n|\')', ' ', text)


    ### replacing dates with '<DATE>'
    def replaceDates(text):
    
        ### REGEX
        month1 = '(january|february|march|april|may|june|july|august|september|october|november|december|'
        month2 = 'jan|feb|mar|apr|may|jun|jul|aug|sept|oct|nov|dec)'
        month = month1 + month2
        day1 = '((monday|tuesday|wednesday|thursday|friday|saturday|sunday|'
        day2 = 'mon|tue|wed|thur|fri|sat|sun)\s)'
        day = day1 + day2
        suffix = '\d(st|nd|rd)'
        year = '\d{4}'
        dd = '\d{1,2}'

        dates = (
            [dd + '/' + dd + '/' + year] +
            [year + '/' + dd + '/' + dd] +
            [dd + '-' + dd + '-' + year] +
            [year + '-' + dd + '-' + dd] +
            [month + '\s' + suffix + '\s' + year] +
            [month + '\s' + dd + '\s' + year] +
            [day + '?' + month + '\s(the\s)*' + suffix + '\s' + year] +
            [day + '?(the\s)?' + suffix + '\s(of\s)*' + month + '\s' + year]
        )

        dateFormats = dates[0]
        for date in dates[1:]:
            dateFormats = dateFormats + '|' + date

        return re.sub(dateFormats,'<DATE>',text)
    text = replaceDates(text)        


    ### replacing e-mails with '<EMAIL>'
    text = re.sub('(\S)+@(\S)+','<EMAIL>',text)


    ### replacing numbers with '<NUM>'
    text = re.sub('[0-9]+','<NUM>',text)


    ### replacing urls with '<URL>'
    url = '(http://(\S)+)'
    text = re.sub(url,'<URL>',text)

    return text

def read_csv(filepath):             # reads csv as generator
    with open(filepath,encoding='utf-8') as f:
        reader = csv.reader(f,delimiter=',')
        for row in reader:
            yield row

def write_csv(data, csv_file):      # writes list of lists to csv
    
    with csv_file.open('w+', encoding='utf-8', newline='') as f:
        writer = csv.writer(
            f,
            delimiter=',', 
            quotechar='"', 
            quoting=csv.QUOTE_MINIMAL
            )

        i = 0
        for row in data:
            writer.writerow(row)
            
            i += 1
            if i%1000 == 0:
                print(i,' rows written to csv')

    return

def execute_query(query):           # executes a string as a query in database
   
    try:
        connection = psycopg2.connect(user = "postgres",
                                    password = "password",
                                    host = "127.0.0.1",
                                    port = "5432",
                                    database = "postgres")

        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        print("Query executed succesfully in PostgreSQL")

    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
    
    finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")
    return

def select(query):                  # returns the result of a selection query
    
    connection = psycopg2.connect(user = "postgres",
                                password = "password",
                                host = "127.0.0.1",
                                port = "5432",
                                database = "postgres")

    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()

    return result

def df_import(query,columns):       # returns result of selection as pd dataframe
        
    connection = psycopg2.connect(user = "postgres",
                            password = "password",
                            host = "127.0.0.1",
                            port = "5432",
                            database = "postgres")

    cursor = connection.cursor()
    cursor.execute(query)
    
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=columns)

    return df

def splitter(strng,article_id):     # string '[elm1,elm2]' -> [(article_id,elm)]
    
    list_of_tuples = []
    
    if strng:
        words = strng.split(',')

        for word in words:
            word = word.strip()
            tpl = (article_id,word)
            list_of_tuples.append(tpl)


    return list_of_tuples

def process(df):                    # processes a pd dataframe for insertion

    # produces a dictionary with structure:
    #   values = {
    #       table_name : list of values for insertion
    #       }



    values = {}

    # replace NaN with empty strings
    df = df.fillna('')

    # create empty lists
    values['authors'] = []
    values['meta_keywords'] = []
    values['articles'] = []
    values['tags'] = []
    
    for row in df.iterrows():
        
        article_id = row[1]['id']
        
        # articles
        dct = {
            'id' : article_id,
            'content': row[1]['content'],
            'type': row[1]['type'],
            'domain':row[1]['domain'],
            'url':row[1]['url'],
            'scraped_at':row[1]['scraped_at'],
            'inserted_at':row[1]['inserted_at'],
            'updated_at':row[1]['updated_at'],

        }

        values['articles'].append(dct)

        # authors
        names = row[1]['authors']
        tpls = splitter(names,article_id)
        for tpl in tpls:
            values['authors'].append(tpl)
    
        # meta_keywords
        words = row[1]['meta_keywords']       
        words = words.strip('[]')          # remove characters []
        words = words.replace('\'','')     
        tpls = splitter(words,article_id)
        for tpl in tpls:
            values['meta_keywords'].append(tpl)

        # tags
        tags = row[1]['tags']
        tpls = splitter(tags,article_id)
        for tpl in tpls:
            values['tags'].append(tpl)


    return values

def insert_pd(input_csv,batch_size,queries,batches):    # csv -> database (in batches)

    # queries is a dictionary with structure:
    #   queries = {
    #       name_of_table : insertion_query_string
    #       }

    # n number of batches with size = batch_size
    for i in range(batches):

        # reads batch_size number of rows as dataframe
        df = pd.read_csv(input_csv,
                        nrows=batch_size,
                        skiprows=range(1,(i*batch_size)+1)
                        )
        
        # formats the whole batch for insertion
        values = process(df)

        # inserts the whole batch into different tables
        with connection.cursor() as cursor:
        
            # executes each query in queries
            try:
                for query in queries.keys():
                    cursor.executemany(queries[query],values[query])
            except psycopg2.DataError:
                connection.rollback()
            except psycopg2.IntegrityError:
                connection.rollback()

            connection.commit()

        print((i+1)*batch_size," rows inserted")
    
    connection.close()

