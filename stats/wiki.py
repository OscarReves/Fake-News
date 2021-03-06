import psycopg2
import pandas as pd
from functions import select

query = """
    WITH wiki AS(
        SELECT article_id
        FROM articles
        WHERE domain = 'wikinews.org'
    )
    SELECT meta_keyword, COUNT(meta_keywords.article_id)
    FROM meta_keywords, wiki
    WHERE meta_keywords.article_id = wiki.article_id
    GROUP BY meta_keyword
    ;
    """

results = select(query)
results.sort(key = lambda x: x[1])
results.reverse()

# convert to dataframe
df = pd.DataFrame(results,columns=['tag','count'])
df = df[0:20]

# plot distribution of labels
import matplotlib.pyplot as plt
df.plot.bar(x='tag',y='count',ylim=0)
plt.xlabel('meta_keyword')
plt.ylabel('Frequency')
plt.title('Most popular meta_keywords')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig("figures/wiki_meta_keywords")
plt.show()