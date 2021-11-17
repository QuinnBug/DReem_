import DataConnector
import ConsoleCommands as cc

dc = DataConnector.DataBase()

dc.connect_to_db("root", "pw123", "companies_house")
cc.name_search_point(dc)
