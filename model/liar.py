import pandas as pd

# load file
df = pd.read_csv("data/train.tsv",sep='\t',header=None)

# select relevant cdfolumns
labels = df.iloc[:,1]
content = df.iloc[:,2]
df = pd.concat([labels,content],axis=1)
df.columns = ['label','content']

# label aggregation
df['label'] = [
    'REAL' if label in {
        'mostly-true',
        'true',
        'half-true',
        'barely-true'
    }
    else 'FAKE'
    for label in df.label
]

# load models and transformers
import joblib
loaded_model = joblib.load("models/model.sav")
tfidf_transformer = joblib.load("models/tfidf.sav")
count_transformer = joblib.load("models/count.sav")

# make prediction
print("vectorizing")
count = count_transformer.transform(df['content'])
tfidf = tfidf_transformer.transform(count)

print("making prediction")
y_pred = loaded_model.predict(tfidf)
df['predicted'] = y_pred

# convert labels
df['predicted'] = df['predicted'].apply(lambda x: 'FAKE' if x == 0 else 'REAL')


# calculate score


print("plotting results")
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix
conf_mat = confusion_matrix(list(df['predicted']), list(df['label']))
fig, ax = plt.subplots(figsize=(12,12))
sns.heatmap(conf_mat, annot=True, fmt='d',
            xticklabels=df['label'].drop_duplicates().values, yticklabels=df['label'].drop_duplicates().values)
plt.ylabel('Actual')
plt.xlabel('Predicted')
plt.title('Confusion Matrix')
plt.savefig("figures/liar_confusion_matrix")
plt.show()

# metrics
from sklearn import metrics
print(metrics.classification_report(
    list(df['predicted']),
    list(df['label'])
    ))

