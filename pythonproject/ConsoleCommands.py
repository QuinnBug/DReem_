import CSVHandler as csv
import NomisDL as nomis


def start_point(dc):
    while True:
        print("-----")
        cmd = str.lower(input('Menu Action : '))

        if cmd == "quit":
            break

        elif cmd == "help":
            print("""
> connect = connect to a database with username and password
> dbc = quick debug connect 
> nomis = access nomis commands
> quit = quit
            """)

        elif cmd == "connect":
            database = input(cmd + ' > Database Name : ')
            username = input(cmd + ' > Username : ')
            password = input(cmd + ' > Password : ')

            if dc.connect_to_db(username, password, database):
                print("connected")
                full_debug_point(dc)
                break

        elif cmd == "dbc":
            if dc.connect_to_db('root', 'pw123', 'companies_house'):
                print("connected")
                full_debug_point(dc)
                break

        elif cmd == "nomis":
            nomis_point()
        else:
            print('Unknown Action')


def nomis_point():

    while True:
        print("-----")
        cmd = str.lower(input('Nomis Action : '))
        if cmd == "quit" or cmd == "back":
            break

        elif cmd == "help":
            print("""
> update = update all the nomis data in list files and nomis.csv
> back = return to main menu
            """)

        elif cmd == "update":
            # file = input("> Filename : ")
            file = "nomis.csv"
            csv.save_file(file, nomis.fetch_all_data())

        else:
            print('Unknown Action')


def name_search_point(dc):
    while True:
        print("-----")
        name = str.upper(input('Sub/String To Search : '))
        if name is '':
            print("invalid entry")
            continue

        companies = dc.search_for_name(name)

        for c in companies:
            print(c)


def full_debug_point(dc):
    while True:
        print("-----")
        cmd = str.lower(input('Database Action : '))

        if cmd == "quit" or cmd == "back":
            dc.disconnect()
            break

        elif cmd == "help":
            print("""
> fetch closed companies = get all companies with a closure date 
> add = import the data from a companies house oriented csv to the database 
> add test = import the data from a test csv to the database 
> search = get info from the database using a criteria 
> execute = input an SQL query 
> fetch last = get the info from the last query
> commit = commit actions from the last query
> custom = custom query
            """)

        elif cmd == "add ch":
            file = input(cmd + ' > file to read : ')
            dc.add_companies(file, False)

        elif cmd == "add nomis":
            dc.add_nomis("nomis.csv")

        elif cmd == "add test":
            file = input(cmd + ' > test file to read : ')
            dc.add_companies(file, True)

        elif cmd == "add table":
            dc.create_table()

        elif cmd == "update population":
            dc.import_pop()

        elif cmd == "execute":
            query = input(cmd + " > Enter SQL Command : ")
            dc.execute_command(query)

        elif cmd == "fetch last":
            dc.fetchall_last_command()

        elif cmd == "commit":
            dc.commit_to_db()

        elif cmd == "search":
            vari = input(cmd + " > criteria : ")
            dc.fetch_company(vari)

        elif cmd == "fetch closed companies":
            dc.fetch_company("is_active='false'")

        elif cmd == "custom":
            dc.custom_query()

        else:
            print('Unknown Action')
