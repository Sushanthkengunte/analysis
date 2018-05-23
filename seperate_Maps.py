import pandas as pd

filename = "Data/sample.csv"
# columns = ["a","b","c","d","e","f","g","h","i","j","k"]
df = pd.read_csv(filename,sep=",",header=None,names = ["a","b","c","d","e","f","g","h","i","j","k"])
print(df)
# df2 = df[df.f != "MIRAMAR" ]
# df = df[df.f == "MIRAMAR"]
