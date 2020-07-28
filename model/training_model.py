import pandas as pd
from functions import df_import

# FETCHING DATA
query ="""
    WITH selection AS (
        SELECT content,type
        FROM articles
        ORDER BY RANDOM()
        )
    SELECT * 
    FROM selection
    LIMIT 200000
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
        'reliable',
        'bias'
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

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.naive_bayes import MultinomialNB

print("vectorizing training data to bag of words")
count_vect = CountVectorizer()
X_train_counts = count_vect.fit_transform(X_train)

print("Vectorizing training data as tf-idf")
tfidf_transformer = TfidfTransformer()
X_train_tfidf = tfidf_transformer.fit_transform(X_train_counts)

print("fitting model")
model = MultinomialNB().fit(X_train_tfidf, y_train)


X_test_counts = count_vect.transform(X_test)
X_test_tfidf = tfidf_transformer.transform(X_test_counts)
y_pred = model.predict(X_test_tfidf)

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

# metrics
from sklearn import metrics
print(metrics.classification_report(
    y_test,
    y_pred
))

# saving model
print("saving model")
import pickle
pickle.dump(model,open("models/model.sav",'wb'))
pickle.dump(tfidf_transformer,open("models/tfidf.sav",'wb'))
pickle.dump(count_vect,open("models/count.sav",'wb'))
