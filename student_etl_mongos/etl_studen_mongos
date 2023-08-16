#import statements
import pandas as pd
import pymongo


class ToMongo():
    '''
    moves student data to mongos

    '''
    def __init__(self):
        self.mongo_url= 'mongodb+srv://lmproe27:SirRoe124@cluster0.zhnjqzj.mongodb.net/?retryWrites=true&w=majority'
        #connect to PyMongo
        self.client = pymongo.MongoClient(self.mongo_url)

        self.db2 = self.client.db2
        self.students = self.db2.students
        self.clean_data()
    def clean_data(self):
        # grab both files
        self.df1 = pd.read_csv(r'C:\Users\lmpro\Documents\GitHub\logans_portfolio\student_etl_mongos\data\student-mat.csv',sep=';')
        self.df2 = pd.read_csv(r'C:\Users\lmpro\Documents\GitHub\logans_portfolio\student_etl_mongos\data\student-por.csv',sep=';')
        #add flags
        self.df1.loc[:,'course'] = 'math'
        self.df2.loc[:,'course'] = 'portuguese'
        #concat the data together 
        self.df = pd.concat([self.df1,self.df2])

        self.df.columns = self.df.columns.str.lower().str.strip().str.replace('-','_').str.replace(' ','_')

        #check df info
        self.df.columns.str.lower().str.strip().str.replace('-','_').str.replace(' ','_')

        #self.df.set_index('id', inplace=True)
    def upload_collection(self):
        '''
        upload an entire collection of items to MongoDB.
        BEWARE THERE IS A MAXIMUM UPLOAD SIZE
        limitations to the amount of data that you can upload at once
        '''
        self.students.insert_many(self.df.to_dict('records'))

if __name__ == '__main__':
    c = ToMongo()
    print('Successful Connection to Client Object')
    c.upload_collection()
    print('upload success')