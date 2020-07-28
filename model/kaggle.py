import json
import pandas as pd

# load kaggle data
df = pd.read_json(open("data/test_set.json"))

# load models and transformers
import joblib
loaded_model = joblib.load("models/model.sav")
tfidf_transformer = joblib.load("models/tfidf.sav")
count_transformer = joblib.load("models/count.sav")

# vectorize
print("vectorizing")
count = count_transformer.transform(df['article'])
tfidf = tfidf_transformer.transform(count)

print("making prediction")
y_pred = loaded_model.predict(tfidf)
df['predicted'] = y_pred

# convert labels
df['label'] = df['predicted'].apply(lambda x: 'FAKE' if x == 0 else 'REAL')
df = df[['id','label']]

print("writing to csv")
df.to_csv("results/kaggle.csv",index=False)