import psycopg2
import pandas as pd
from functions import select

query = """
    SELECT domain, COUNT(domain)
    FROM articles
    GROUP BY domain
    ;
    """

results = select(query)
results.sort(key = lambda x: x[1])
results.reverse()

# convert to dataframe
df = pd.DataFrame(results,columns=['source','count'])
df = df[0:20]

# convert to thousands
df['count'] = df['count'].values / 10000

# plot distribution of labels
import matplotlib.pyplot as plt
df.plot.bar(x='source',y='count',ylim=0)
plt.xlabel('Source')
plt.ylabel('Frequency (in tens of thousands)')
plt.title('Most popular domains')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig("figures/sources")
plt.show()