# ctv analyze
import pandas as pd
df=pd.read_csv('ctv.csv', sep='\t')
df['cd2'] = pd.to_datetime(df.cd)
df2 = df.groupby('cd2').mean()
df3 = df2[df2.index < pd.to_datetime('2022-11-09')]

from statsmodels.tsa.seasonal import STL, seasonal_decompose
import matplotlib.pyplot as plt
stl = STL(df3.ctv)
res = stl.fit()
fig = res.plot()
plt.savefig('test.png')
