import vaccinanalyzer as van
import vaccinvisual as vis

def main():
    data_table = van.DataAnalyzer("vaccin_covid.csv")
    data_table._preprocess()
    data_table._data_normalising()
    
    
    vacc_databases = van.IntoDatabase(data_table.read_data(), "new_database")
    vacc_databases.into_db()

    vacc_info = vis.DataVisualiser()
    vacc_info.show_plot("Spain", "New Zealand")
    vacc_info.show_data("Sweden")


if __name__ == "__main__":
    main()
