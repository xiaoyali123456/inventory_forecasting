seq = q.x
seq2 = seq.map(lambda s: [s[0:i] for i in range(3, 10)])
seq3 = seq2.explode()

df = q[['x', 'y']]
df.x = df.x.map(lambda s: [s[0:i] for i in range(3, 10)])
df = df.explode('x').groupby('x')['y'].sum().reset_index().sort_values('y', ascending=False)
