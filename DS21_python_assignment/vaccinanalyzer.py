import numpy as np
import pandas as pd
import sqlite3 as sql
from sklearn.preprocessing import MultiLabelBinarizer

class DataAnalyzer:
    """
    A class to represent a manual workflow with data. It is designed specifically for vaccin_covid.csv,
    and cannot be used for other datasets.

    Attributes
    ----------
    path : str
        path to the file

    Methods
    -------
    _preprocess():
        Manual manipulations with a dataframe, changing datatypes and deleting rows with little information. All data stays in csv file, and
        in data_raw dataframe.

    _data_normalising():
        Manual manipulations with a dataframe to normalise relations and covert into 3rd Normal Form.
        Returns a dictionary with three dataframes created through a normalisation process.
        Format: key: table_name, value: dataframe.
    """

    def __init__(self, path:str):
        """
        Constructs an object tha reads automatically csv file into data_raw variabel.
            
        Parameters
        ----------
        path : str
            path to the file, use a relative path if in the same folder, or full path otherwise.
        """
        self.path = path
        self.data_raw = pd.read_csv(self.path) 
        self.datadict = {} #empty dictionary that will store all dataframes
   

    def _preprocess(self):

        """
        Manual manipulations with the dataframe data_raw -> vaccinations_data: 
        - column Date from String type to Datetime type - to increase functionality
        - setting index of the original dataframe to Country and Date columns,
        - removing rows with a lot of NaNs 
        since those are the uniqie attributes that charactirise most of recordings. That partly answers 2nd and 3rd normal form principles 
        (further normalisation in _data_normalising() method.)
        """

        #Changing type of date columns - to increase functionality 
        self.data_raw.date = pd.to_datetime(self.data_raw.date)
        #CHanging typy of values under vaccine column - from string to a list
        self.data_raw.vaccines = self.data_raw.vaccines.apply(lambda x: x.split(", "))
        #Setting index
        self.data_raw = self.data_raw.set_index(['country', 'date']).sort_index()

        #Removing rows that dont have less than  non-NaNs in a row of chosen columns, i.e. to little usefull information.
        self.vaccinations_data = self.data_raw.dropna(axis=0, thresh = 4, subset=['total_vaccinations',
       'people_vaccinated', 'people_fully_vaccinated',
       'daily_vaccinations_raw',
       'total_vaccinations_per_hundred', 'people_vaccinated_per_hundred',
       'people_fully_vaccinated_per_hundred', 'daily_vaccinations_per_million'])
        
        

    def _data_normalising(self):
        """
        Manual chain of python operations on dataframe(s) specifically suited for the vaccin_covid.csv dataset.
        
        - Using scikit module to convert lists of vaccines into a new DF with columns vaccin names. 
        
        https://stackoverflow.com/questions/29034928/pandas-convert-a-column-of-list-to-dummies/51420716#51420716
        https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.MultiLabelBinarizer.html
        Returns a new DF with Country, date of first use, and 1 value - if country used vaccine.
        That brings table to a 1st Normal Form. Added functionality - column sum that shows number of vaccins used. In order not to loose information, kept date 
        column - to indicate a first date when a specific number of vaccins is used. If theoretically a country changes a set of vaccins, a new record can be recorded.
    
        - Iso-codes violate 2nd normal form, as it is redundant to have this data repetitive -  creating separate DF with pairs iso_codes - country.
        
        Returns a dictionary with three dataframes created through a normalisation process.
        Format: key: table_name, value: dataframe.        
        
        """
        
        #Using sklearn module for a faster transformation of values into columns. Saving into a new DF. 
        #We want to keep the first value of the date for each unique combination of country-used vaccines 
        #in case later some country uses different vaccins - notthe case so far.

        self.vaccines_used = pd.DataFrame(
            {'vax': self.data_raw.vaccines
            }, columns=['vax'])

        mlb = MultiLabelBinarizer()
        self.vaccines_used = pd.DataFrame(mlb.fit_transform(self.vaccines_used['vax']),columns=mlb.classes_, index=self.data_raw.index)
       
        #replacing some characters in the names of new columns, that Sqlite can process as column names later.
        self.vaccines_used.columns = self.vaccines_used.columns.str.replace("[-&/ ]", "_", regex = True) 
        
        self.vaccines_used = self.vaccines_used.reset_index(level=0).drop_duplicates(keep="first").set_index('country',append=True)
        self.vaccines_used["total_number"] = self.vaccines_used.loc[:,"Abdala":"Sputnik_V"].sum(axis=1) #Adding a column sum of vaccinations used.
        
        #Creating a new DF with pairs country-iso_codes_code to avoid redundancy of repetitive information.
        self.iso_codes = self.data_raw.reset_index()[["country","iso_code"]].drop_duplicates()
        self.iso_codes.index.names = ["ind"]

        #We can drop column vaccines to avoid redundancy from a vaccinations_data, as we now have this data in a separate DF.
        self.vaccinations_data = self.vaccinations_data.drop(columns=["iso_code", "vaccines"]) 


        
        self.datadict["iso_codes"] = self.iso_codes #DF to dictionary of DFs
        self.datadict["vaccines_used"] = self.vaccines_used #DF to dictionary of DFs
        self.datadict["vaccinations_data"] = self.vaccinations_data #DF to dictionary of DFs

    def read_data(self):
        return self.datadict

    # def tb_name(self): 
    #creating a function to return a name of the CSV file - it will be the name of a database.
    # Instead changed functionality to giving a name as str while creating an instance of IntoDatabase class. 
      #  self.db_name = os.path.splitext(self.path)[0]
       # return self.db_name 

    

class IntoDatabase():

    """
    Implementation of an automatic convertation of dataframes into tables of the same database. 
    Instance of class will take a dictionary of dataframes and a name for the database. 
    It is not dataset specific and can turn ANY dataframes to datatable of a database, with certain limitaions.

    Attributes
    ----------
    datadict : object
        dictionary with dataframes as values.

    tbname : str
        name of the database

    Methods
    -------
    _set_pk() 
        sets a primary key of a datatable- based on the index of a dataframe. Further can develop functionality to ask user which column
        they want to set as primary key.

    _set_fk()
        sets a foreign key based on users input. It was raw, so I put user = False, and to set FKs automatically.

    into_db()
        Turns dataframes into tables of a database.  

    """
    def __init__(self, datadict, tbname):
        """
        Constructs an object tha reads a dictionary of dataframes and a name for the table.
            
        Parameters
        ----------

        datadict : obj
            dictionary of dataframes
        tbname : str
            name under which database will be saved.
        """
        self.datadict = datadict
        self.tbname = tbname


    def _set_pk(self, value, pk=True):
        """
        When used while looped through a dictionary with data frames takes dataframe as attibute.
        Depends if there are index or indexes, sets them as primary key.
        """
        self.value = value
        self.pk = pk

        if len(value.index.names) == 0:
            self.pk = False
        elif len(value.index.names) == 1:
            ind = "("+str(tuple(value.index.names)[0])+")"
        else:
            ind = str(tuple(value.index.names)).replace("'","")

        if self.pk == True:
            return f"PRIMARY KEY {ind} "
        else: 
            return ""
        
    def _set_fk(self, key, user = False):

        """
        When used while looped through a dictionary with data frames takes key - name of dataframe- as attibute.
        Normally would ask a user input, but i set user to False, so it sets FKs automatically.
        """

        self.key=key
        self.user = user

        if user == True:
        # Was a very raw attempt to set FK by engaging with a user. It works, but i'll code in FKs.
        
            ans = input("To set a Foreign Key, press Y/y!   ")
            if ans == "y" or ans == "Y":
                print(f"Choose from columns {self.datadict[self.key].index.name} {list(self.datadict[self.key])}. If two or more, use comma:   ")
                col = input("Please type column carefully!   ")
                print(f"choose a table two connect with {list(self.datadict.keys())}  ")
                tb = input("Please type in one name carefully!   ")
                print(f"choose column/s to connect with {self.datadict[tb].index.name} {list(self.datadict[tb])} ")
                col2 = input("Please type column carefully!   ")


                return f"FOREIGN KEY ({col}) REFERENCES {tb} ({col2})"

            else:
                return ""
        else:
            if self.key =="iso_codes":
                return "FOREIGN KEY (country) REFERENCES vaccinations_data (country)"
            if self.key =="vaccinations_data":
                return ""
            if self.key == "vaccines_used":
                return "FOREIGN KEY (country) REFERENCES vaccinations_data (country)"
            


    def into_db(self):
        """
        Due to the fact that sqlight doesnt support adding PK and FK to existing tables we follow these steps:
        Created a database.
        Creates a table from a dataframe while looping through a dictionary.
        Renames them
        Creates new tables, sets PK and FK
        Inserts data from old tables. 

        """
        db = sql.connect(self.tbname+".db")
        cur = db.cursor()

        for key, value in self.datadict.items():  #key - DFs name, value - DFs that are turned into tables
           
            value.to_sql(key, db, index = True)

            cur.execute(f"PRAGMA table_info({key});")  ## reads column names and their type, to later post it into SQL Query
            x = cur.fetchall()
            #here goes manipulation to change pragma table_info to a suitable for sqlight text format: column name, datatype
            columns = ""
            
            for i in x:
                columns = columns + str(i[1])+ " " +str(i[2]).lower() + ",\n"

            cur.execute(f"""ALTER TABLE {key} RENAME TO old_{key}""")
            db.commit()
            cur.execute(f"""CREATE TABLE IF NOT EXISTS {key}
            (
            {columns}  
            {self._set_pk(value)}
            {self._set_fk(key)}

            )
        """)
            cur.execute(f"INSERT INTO {key} SELECT * FROM old_{key}")
            cur.execute(f"DROP TABLE IF EXISTS old_{key}")
            db.commit()

    


    



