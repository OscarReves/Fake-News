import psycopg2
import pandas as pd
from functions import select

# define the selection query
query = """
    SELECT type, COUNT(type)
    FROM articles
    GROUP BY type
    ;
    """


# save the results as a dataframe
results = select(query)
df = pd.DataFrame(results,columns=['type','count'])     

# convert to percentages
df['percentages'] = df['count'].values / sum(df['count'].values)*100
df = df.sort_values('percentages',ascending=False)

# plot distribution of labels
import matplotlib.pyplot as plt
df.plot.bar(x='type',y='percentages',ylim=0)
plt.xlabel('Article type')
plt.ylabel('Relative frequency')
plt.title('Frequency of types of articles')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig("figures/article_types")
plt.show()


