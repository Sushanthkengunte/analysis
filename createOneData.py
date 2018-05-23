import pandas as pd

import re
import os
abstractDictionary = []
pattern = re.compile('.*:.*')
files = [f for f in os.listdir('deaths/') if re.match(r'.*', f)]
# files = ["sample.csv","sample.csv"]
listOfFrames = []
for file in files:
    filename = "deaths/" + file
    df = pd.read_csv(filename)
    # s = df['killer_name']
    # print(s[42],s[43])
    df = df.astype(object).where(pd.notnull(df), "None")
    # s = df['killer_name']
    # print(s[42], s[43])
    # print(len(df))
    listOfFrames.append(df)
MIRMAR = []
notMIRMAR = []
for each in listOfFrames:
    df2 = each[each.map != "MIRAMAR"]
    df = each[each.map == "MIRAMAR"]
    MIRMAR.append(df)
    notMIRMAR.append(df2)
allFrames_mir = pd.concat(MIRMAR)
allFrames_not_mir = pd.concat(notMIRMAR)
# print(len(allFrames))
# print(allFrames)
outputFileName = "Data/deaths_mir.csv"
allFrames_mir.to_csv(outputFileName,index_label = False,index = False,header = False)

outputFileName1 = "Data/deaths_not_mir.csv"
allFrames_not_mir.to_csv(outputFileName1,index_label = False,index = False,header = False)

