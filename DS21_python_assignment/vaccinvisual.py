import pandas as pd
import sqlite3 as sql
import seaborn as sns
import matplotlib.pyplot as plt

class DataVisualiser:
    """
    A class reads a database and allows for representation of information. In a table and visually.

    """

    
    def __init__(self):
        self.ndb = sql.connect("new_database.db")
        self.cur = self.ndb.cursor()

    def show_data(self, country0):
        
        view = pd.read_sql(f"SELECT vd.country, vd.date, vd.people_fully_vaccinated FROM vaccinations_data AS vd LEFT JOIN vaccines_used AS vu ON vd.country = vu.country WHERE vd.country = '{country0}'  ", self.ndb)
        print(view.tail(10))

    def show_plot(self, country1, country2):
        self.new_plot = pd.read_sql(f"SELECT country, date, total_vaccinations_per_hundred FROM vaccinations_data  WHERE country = '{country1}' OR country = '{country2}' ", self.ndb)
        self.new_plot = self.new_plot.set_index(pd.DatetimeIndex(self.new_plot['date']))
        np_2 = self.new_plot.groupby(['country', pd.Grouper(freq='2W')])['total_vaccinations_per_hundred'].mean().reset_index()
        
        plt.figure()
        sns.scatterplot(x="date", y = "total_vaccinations_per_hundred", hue = "country", data=np_2)
        plt.show()




### plot speed of vaccinations with the amount vaccines used
