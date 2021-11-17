import DataConnector

dc = DataConnector.DataBase()

dc.connect_to_db("root", "pw123", "companies_house")

dc.produce_graphs()
print("graphs done")
