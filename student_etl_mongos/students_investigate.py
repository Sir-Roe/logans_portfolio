import pymongo
import pandas as pd


# grab both files
df1 = pd.read_csv(r'C:\Users\lmpro\Documents\GitHub\logans_portfolio\student_etl_mongos\data\student-mat.csv',sep=';')
df2 = pd.read_csv(r'C:\Users\lmpro\Documents\GitHub\logans_portfolio\student_etl_mongos\data\student-por.csv',sep=';')
#add flags
df1.loc[:,'course'] = 'math'
df2.loc[:,'course'] = 'portuguese'
#concat the data together 
df = pd.concat([df1,df2])

df.columns = df.columns.str.lower().str.strip().str.replace('-','_').str.replace(' ','_')

#check df info
df.columns.str.lower().str.strip().str.replace('-','_').str.replace(' ','_')
print(df.info())

#declare mongo_url
mongo_url='mongodb+srv://lmproe27:SirRoe124@cluster0.zhnjqzj.mongodb.net/?retryWrites=true&w=majority'
