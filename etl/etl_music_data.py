import pandas as pd
from dotenv import load_dotenv
from os import getenv
import psycopg2
load_dotenv()

#build a dataframe
class MUSE_SQL:
    __user = getenv("USER")
    __password= getenv("PASSWORD")
    __server= getenv("SERVER")
    
    #used for to_sql function
    __sql_url = getenv("SQLURL")
    #used for querying
    __TS_con= psycopg2.connect(
        dbname = __user,
        user= __user,
        password = __password,
        host = __server
        )
    __cur=__TS_con.cursor()

    def create_dataframe(self):
        
        #Grabs the data from the titanic file and cleans the columns etc
        #header string  
        self.df= pd.read_csv(r'C:\Users\Logan\Documents\GitHub\wine_analysis\etl\music_genre.csv',na_values='?')
        self.df= self.df.apply(pd.to_numeric,errors='ignore')
        self.df.columns= self.df.columns.str.lower().str.strip().str.replace('/','_').str.replace(' ','_')
        self.df= self.df[(self.df['duration_ms']>0)&(self.df['duration_ms']<750000)]
        self.df =self.df[(self.df['popularity']>0)]
        self.df.dropna(inplace=True)
        #return DF incase user does NOT want to query the db for results
        return self.df

    def update_data_sql(self):
        #Used to take my DF and send into database
        self.df.to_sql('music',con=self.__sql_url, if_exists='replace')

    def refresh(self):
        self.create_dataframe()
        self.update_data_sql()


'''  def query_db(self, sql_filepath: str):
        start = self.create_file(sql_filepath)
        queries = start.split(';')
        for query in queries:
            try:
                print(query)
                self.__cur.execute(query)
                self.__TS_con.commit()
                print(self.__cur.fetchall())
            except psycopg2.ProgrammingError as msg:
                print(f'Error: {msg}')'''