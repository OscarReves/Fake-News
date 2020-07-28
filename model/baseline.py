import pandas as pd
from functions import df_import

# FETCHING DATA
query ="""
    SELECT type
    FROM articles
    LIMIT 1000000
    ;
    """

# converting to dataframe
df = df_import(query,columns=['type'])

# drop rows with type = 'uknown' or ''
df = df[df.type != 'unknown']
df = df[df.type != '']

# label aggregation
df['type'] = [
    'REAL' if label in {
        'political',
        'reliable'
    }
    else 'FAKE'
    for label in df.type
]

# factorize
df['category_id'] = df['type'].factorize()[0]

# monkey guesses
import random 
df['predicted'] = [random.randint(0,1) for elm in range(len(df))]

# calculate score
a = df[df.category_id == df.predicted]
score = len(a) / len(df)
print(score)