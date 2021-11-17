import DataConnector

dc = DataConnector.DataBase()

dc.connect_to_db("root", "pw123", "companies_house")
dc.save_nomis_la_stats()

confirm = input("Function run, enter any key to close script")