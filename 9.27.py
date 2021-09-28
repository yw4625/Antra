import pandas as pd
df = pd.read_table('people_1.txt',skipinitialspace=True,delimiter = "\t")
df[['FirstName','LastName','Email','Phone','Address']]=df[['FirstName','LastName','Email','Phone','Address']].apply(lambda x : x.str.strip())
df[['FirstName','LastName']]=df[['FirstName','LastName']].apply(lambda x : x.str.title())
df[['Phone']]=df[['Phone']].apply(lambda x : x.str.replace('-', ''))

df2 = pd.read_table('people_2.txt',skipinitialspace=True,delimiter = "\t")
df2[['FirstName','LastName','Email','Phone','Address']]=df2[['FirstName','LastName','Email','Phone','Address']].apply(lambda x : x.str.strip())
df2[['FirstName','LastName']]=df2[['FirstName','LastName']].apply(lambda x : x.str.title())
df2[['Phone']]=df[['Phone']].apply(lambda x : x.str.replace('-', ''))

merge = pd.concat([df, df2])
merge.to_csv('file.csv',index=False)



import pandas as pd

df = pd.read_json('movie.json')

result1 = df[0:1250]
result2 = df[1249:2499]
result3 = df[2499:3749]
result4 = df[3749:4999]
result5 = df[4999:6249]
result6 = df[6249:7499]
result7 = df[7499:8749]
result8 = df[8749:9995]


result1.to_json (r'JS1.json',orient='records')
result2.to_json (r'JS2.json',orient='records')
result3.to_json (r'JS3.json',orient='records')
result4.to_json (r'JS4.json',orient='records')
result5.to_json (r'JS5.json',orient='records')
result6.to_json (r'JS6.json',orient='records')
result7.to_json (r'JS7.json',orient='records')
result8.to_json (r'JS8.json',orient='records')


# In[ ]:




