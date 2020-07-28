from model import pipe
import pandas as pd
from functions import df_import

# FETCHING DATA
query ="""
    SELECT content,type
    FROM articles
    LIMIT 10000
    ;
    """

# converting to dataframe
df = df_import(query,columns=['content','type'])

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


# convert type-labels to integers
print("indexing labels")
df['int_type'] = df['type'].factorize()[0]


# splitting data into test and train
print("splitting data into test and train")
split = int(len(df)/5)
test_data = df[0:split]
training_data = df[split+1:]

# upscaling the training set
print("upsampling the training set")
from model_functions import upsample
training_data = upsample(training_data)

# label targets
X_train = training_data['content']
y_train = training_data['int_type']
X_test = test_data['content']
y_test = test_data['int_type']

# import 
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline

vectorize = Pipeline([
    ('vect',CountVectorizer()),
    ('tfidf',TfidfTransformer())
    ])

X_train_vectorized = vectorize.fit_transform(X_train)
model =  MultinomialNB().fit(X_train_vectorized,y_train)
X_test_vectorized = vectorize.fit(X_test)
y_pred = model.predict(X_test_vectorized)

print("plotting results")
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix
conf_mat = confusion_matrix(y_test, y_pred)
fig, ax = plt.subplots(figsize=(12,12))
sns.heatmap(conf_mat, annot=True, fmt='d',
            xticklabels=df['type'].drop_duplicates().values, yticklabels=df['type'].drop_duplicates().values)
plt.ylabel('Actual')
plt.xlabel('Predicted')
plt.title('Confusion Matrix')
plt.savefig("figures/confusion_matrix")
plt.show()

# saving model
import pickle
pickle.dump(model,open("models/test.sav",'wb'))