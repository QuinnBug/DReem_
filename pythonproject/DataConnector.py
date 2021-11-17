import csv
import json
from datetime import datetime

from mysql.connector import connect, Error
import CSVHandler
import Graphs
import CompanyFunctions as cf

ee_sics = [70210, 73110, 73120, 71111, 32120, 32130, 74100, 59111, 59112, 59113, 59120, 59131, 59133, 59140,
           60100, 60200, 74201, 74202, 74209, 58210, 58290, 62011, 62020, 62090, 58110, 58120, 58130, 58141,
           58142, 58190, 91012, 91020, 59200, 85520, 90010, 90020, 90030, 90040, 26110, 26120, 26200, 26301,
           74300, 91011, 47630, 91030, 77351, 79110, 79120, 79901, 79909, 82300, 91040, 92000, 93110, 93120,
           26309, 26400, 26800, 46510, 46520, 61100, 61200, 61300, 61900, 62030, 63110, 63120, 63910, 63990,
           95110, 95120, 18201, 32200, 68202, 77110, 93130, 93199, 77210, 77220, 77291, 77341, 93210, 93290,
           49000, 50000, 51000, 55100, 55201, 55202, 55209, 55300, 55900, 56101, 56102, 56210, 56301, 56302,
           85510]


class DataBase:
    def __init__(self):
        self.db = None
        self.cursor = None

    def disconnect(self):
        self.db = None
        self.cursor = None

    # specific purpose functions

    def save_nomis_la_stats(self):
        readfile = open("local_auths_trimmed.txt", 'r', newline='')
        local_auths = readfile.read().splitlines()
        readfile.close()

        self.update_indexes("nomis", "nomis_ysla_asc", "year,sic,local_auth")
        self.update_indexes("nomis", "nomis_la_asc", "local_auth")
        self.update_indexes("companies", "ch_sla_asc", "sic1,local_authority")

        skip_to = "Thanet"
        do_write = False

        if skip_to == "":
            with open("stat_output.csv", 'w') as file:
                file.write("Local Authority,Category,EE 2015,ALL 2015,CH 2015,EE 2016,ALL 2016,CH 2016,EE 2017,"
                           "ALL 2017,CH 2017,EE 2018,ALL 2018,CH 2018,EE 2019,ALL 2019,CH 2019,EE 2020,ALL 2020,"
                           "CH 2020,\n")
            do_write = True

        for la in local_auths:
            if do_write == False and skip_to == la:
                do_write = True
                continue

            if do_write:
                self.write_nomis_stats_to_file(la)

        print("saved stats")

    def write_nomis_stats_to_file(self, local_auth):
        turnovers = ["total_count", "25k", "75k", "150k", "350k", "750k", "1500k",
                     "3500k", "7500k", "25000k", "50000k_plus", "employees", "employees_min", "employees_max"]

        categories = 'total_count,25k,75k,150k,350k,750k,1500k,3500k,7500k,25000k,50000k_plus,employees,' \
                     'employees_min,employees_max '

        company_counts = self.nomis_search(local_auth, categories)
        ch_counts = self.companies_house_ee_fetch(local_auth)

        with open("stat_output.csv", 'a', newline='') as file:
            writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)

            for to in turnovers:
                company_count = company_counts[turnovers.index(to)]

                if to == "total_count":
                    row = [local_auth, to,
                           str(company_count[0][0]), str(company_count[1][0]), str(ch_counts[0]),
                           str(company_count[0][1]), str(company_count[1][1]), str(ch_counts[1]),
                           str(company_count[0][2]), str(company_count[1][2]), str(ch_counts[2]),
                           str(company_count[0][3]), str(company_count[1][3]), str(ch_counts[3]),
                           str(company_count[0][4]), str(company_count[1][4]), str(ch_counts[4])]
                else:
                    row = [local_auth, to,
                           str(company_count[0][0]), str(company_count[1][0]), " ",
                           str(company_count[0][1]), str(company_count[1][1]), " ",
                           str(company_count[0][2]), str(company_count[1][2]), " ",
                           str(company_count[0][3]), str(company_count[1][3]), " ",
                           str(company_count[0][4]), str(company_count[1][4]), " "]

                writer.writerow(row)

            print("done")

    def nomis_search(self, place, category):
        years = ['2015', '2016', '2017', '2018', '2019']

        ee_search_query = "SELECT " + category + "FROM" + ' nomis WHERE  year={0} AND sic={2} AND local_auth="{1}"'
        all_search_query = "SELECT " + category + "FROM" + ' nomis WHERE year={0} AND local_auth="{1}"'

        return_list = []

        for x in range(14):
            return_list.append([[0, 0, 0, 0, 0], [0, 0, 0, 0, 0]])

            i = -1
            for year in years:
                i += 1
                for code in ee_sics:
                    self.cursor.execute(ee_search_query.format(year, place, code))
                    results = self.cursor.fetchall()
                    for result in results:
                        if result[x] is None:
                            print(place + " " + str(code) + " " + str(year))
                        else:
                            return_list[x][0][i] += result[x]

                self.cursor.execute(all_search_query.format(year, place))
                results = self.cursor.fetchall()
                for result in results:
                    if result[x] is None:
                        continue
                    else:
                        return_list[x][1][i] += result[x]

        return return_list

    def companies_house_ee_fetch(self, place):
        counts = [0, 0, 0, 0, 0]
        years = ['2015-12-31', '2016-12-31', '2017-12-31', '2018-12-31', '2019-12-31']

        ee_search_query = 'SELECT incorporation_date FROM companies WHERE sic1="{1}" AND local_authority="{0}"'

        for code in ee_sics:
            self.cursor.execute(ee_search_query.format(place, code))
            results = self.cursor.fetchall()
            for result in results:
                for date in years:
                    if str(result[0]) < date:
                        counts[years.index(date)] += 1

        return counts

    def connect_to_db(self, username, pword, database):
        try:
            self.db = connect(
                host="localhost",
                user=username,
                password=pword,
                database=database
            )
            self.cursor = self.db.cursor()
            return True
        except Error as e:
            print(e)
            return False

    def create_new_database(self, username, pword, database):
        self.db = connect(
            host="localhost",
            user=username,
            password=pword
        )
        self.cursor = self.db.cursor()
        create_db_query = "CREATE DATABASE " + str(database)
        self.cursor.execute(create_db_query)

    def add_companies(self, filename, test):
        file = []

        print("reading and checking companies from file @ " + datetime.now().strftime("%H:%M:%S"))

        CSVHandler.add_ch_to_database(filename, self)

        print("done @ " + datetime.now().strftime("%H:%M:%S"))

    def add_nomis(self, filename):
        print("reading and checking companies from file @ " + datetime.now().strftime("%H:%M:%S"))

        CSVHandler.add_nomis_to_database(filename, self)

        self.update_indexes("nomis", "nomis_la_asc", "local_auth")
        self.update_indexes("nomis", "nomis_year_asc", "year")
        self.update_indexes("nomis", "nomis_sic_asc", "sic")

        print("done @ " + datetime.now().strftime("%H:%M:%S"))

    def search_for_name(self, name):
        print("searching for " + name)
        search_query = "SELECT * FROM companies WHERE name='{0}'"
        self.cursor.execute(search_query.format(name))
        result = self.cursor.fetchall()
        if self.cursor.rowcount > 0:
            print(str(self.cursor.rowcount) + " companies found with name")
            return result

        print("no full match found")
        # if there's no full match
        search_query = "SELECT id,name,sic1,sic2,sic3,sic4 FROM companies WHERE name LIKE '%{0}%'"
        self.cursor.execute(search_query.format(name))
        result = self.cursor.fetchall()
        if self.cursor.rowcount > 0:
            print(str(self.cursor.rowcount) + " companies found with partial match")
            return result

        print("none found")
        return [".", "."]

    def fetch_company(self, variable):
        select_companies_query = """
        SELECT *
        FROM companies
        WHERE 
        """ + variable
        self.cursor.execute(select_companies_query)
        result = self.cursor.fetchall()
        for row in result:
            print(row)

    def custom_query(self):
        years = [2018, 2019, 2020]
        employees_per_year = []
        select_q = 'SELECT employees FROM nomis WHERE year={0} AND local_auth="Derby"'
        for year in years:
            self.cursor.execute(select_q.format(year))
            results = self.cursor.fetchall()
            employees_per_year.append(0)
            for result in results:
                employees_per_year[years.index(year)] += result[0]

        print(employees_per_year)
        input("press any button to continue...")

    def update_indexes(self, table, key, col):
        index_q = "SHOW INDEX FROM {0} WHERE Key_name='{1}'"
        alter_q = "ALTER TABLE {0} DROP INDEX {1}"
        create_q = "CREATE INDEX {1} ON {0}({2} ASC)"

        self.cursor.execute(index_q.format(table, key))
        results = self.cursor.fetchall()

        if self.cursor.rowcount > 0:
            self.cursor.execute(alter_q.format(table, key))
            self.commit_to_db()

        self.cursor.execute(create_q.format(table, key, col))
        self.commit_to_db()

    def produce_graphs(self):
        # self.update_indexes('nomis', 'nomis_ysla_asc', 'year,sic,local_auth')
        # self.update_indexes('nomis', 'nomis_la_asc', 'local_auth')
        # self.update_indexes('companies', 'ch_ee', 'is_ee')
        # self.update_indexes('companies', 'ch_la_asc', 'local_authority')
        # self.update_indexes('companies', 'ch_sic1_asc', 'sic1')
        # self.update_indexes('companies', 'ch_sic2_asc', 'sic2')
        # self.update_indexes('companies', 'ch_sic3_asc', 'sic3')
        # self.update_indexes('companies', 'ch_sic4_asc', 'sic4')

        self.cursor.execute("SELECT DISTINCT local_authority FROM companies")
        local_auths = self.cursor.fetchall()

        ee_specs = cf.EESpecification("definition.csv")
        labels = ee_specs.header
        del labels[0]

        start_from = "Dundee City"
        end_at = "Dundee City"
        do_write = False

        f = open('UK.geojson', 'r')
        counties = json.load(f)
        f.close()

        for la in local_auths:
            if la[0] == start_from or start_from == "":
                do_write = True

            if not do_write:
                continue

            # ch_data = [Graphs.ch_graph_data(self, la[0])]
            # ch_data[0].append("Company Count")
            # Graphs.make_line_graph(ch_data, "Number Of Active E-E Companies Registered At Companies House",
            #                        "Graphs/CH_Companies/" + la[0], False, "Month", "# of Companies")
            #
            # nom_data = Graphs.nomis_graph_data(self, la[0], [2016, 2017, 2018, 2019, 2020, 2021])
            #
            # Graphs.make_line_graph(nom_data[0],
            #                        "Estimated Employee Count Across All E-E Companies",
            #                        "Graphs/Employees/" + la[0], True, "Year", "# of Employees")
            #
            # Graphs.make_line_graph(nom_data[1],
            #                        "Number Of Active E-E Companies Pay VAT/PAYE",
            #                        "Graphs/Companies/" + la[0], False, "Year", "# of Companies")
            #
            # Graphs.make_line_graph(nom_data[2],
            #                        "Estimated Annual Turnover Of All E-E Companies",
            #                        "Graphs/Turnover/" + la[0], True, "Year", "Annual Turnover")
            #
            # Graphs.make_pie_graph(graph_data=Graphs.ee_v_all_pie_data(self, 2021, la[0]),
            #                       graph_labels=["EE Companies", "Other Companies"],
            #                       title="E-E Companies As A Proportion Of All Companies Registered",
            #                       filename="Graphs/EEvALL_Pie/" + la[0], as_percent=True)
            #
            # Graphs.make_histogram(Graphs.lifespan_of_company_data(self, "2021-11-01", la[0]),
            #                       "E-E Company Survival Rate From Birth To 3 Years", "Graphs/Lifespan/" + la[0],
            #                       "Months in Operation", "# of Companies")

            Graphs.make_pie_graph(graph_data=Graphs.freelancer_estimate(self, 2021, la[0], ee_specs),
                                  graph_labels=["Creative Industries", "Digital sector",
                                                "Cultural sector", "Tourism sector", "Sports sector"],
                                  title="Estimated Distribution Of Freelancers Across All E-E Sectors",
                                  filename="Graphs/Freelancers/" + la[0], as_percent=False)

            # Graphs.make_pie_graph(graph_data=Graphs.percent_of_category_pie_data(self, 2021, la[0], ee_specs),
            #                       graph_labels=["Creative Industries", "Digital sector",
            #                                     "Cultural sector", "Tourism sector", "Sports sector"],
            #                       title="Representation Of E-E Companies By Sector",
            #                       filename="Graphs/PercentOfCategory/" + la[0], as_percent=True)
            #
            # hm_data = Graphs.ch_heatmap_data(self, la[0])
            # Graphs.make_heatmap(hm_data[0], hm_data[1], counties, la[0])

            print(str(la[0]) + " done")

            if la[0] == end_at:
                break

    # generic commands
    def fetchall_last_command(self):
        result = self.cursor.fetchall()
        for row in result:
            print(row)

    def commit_to_db(self):
        self.db.commit()

    def execute_command(self, variable):
        self.cursor.execute(variable)

    def print_cursor_rows(self):
        for row in self.cursor:
            print(row)

    def import_pop(self):
        update_query = 'UPDATE nomis SET population={0} WHERE local_auth="{1}"'
        with open("population.csv", newline='', encoding='cp437') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for line in reader:
                line[1] = line[1].replace(',', '')
                self.cursor.execute(update_query.format(line[1], line[0]))
                self.commit_to_db()
        print("pop inserted")

    def create_table(self):
        # create_table_query = """
        #     CREATE TABLE companies(
        #     id INT AUTO_INCREMENT PRIMARY KEY,
        #     name VARCHAR(200),
        #     postcode VARCHAR(100),
        #     lat VARCHAR(100),
        #     lon VARCHAR(100),
        #     actual_location VARCHAR(5),
        #     local_authority VARCHAR(100),
        #     incorporation_date DATE,
        #     closure_date DATE,
        #     is_ee VARCHAR(5),
        #     est_employees INT,
        #     est_value INT,
        #     sic1 VARCHAR(200),
        #     sic2 VARCHAR(200),
        #     sic3 VARCHAR(200),
        #     sic4 VARCHAR(200)
        #     is_active VARCHAR(5)
        #     )
        # """

        create_table_query = """
                    CREATE TABLE nomis(
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    year INT,
                    local_auth VARCHAR(200),
                    sic INT,
                    employees INT,
                    employees_min INT,
                    employees_max INT,
                    total_count INT,
                    25k INT,
                    75k INT,
                    150k INT,
                    350k INT,
                    750k INT,
                    1500k INT,
                    3500k INT,
                    7500k INT,
                    25000k INT,
                    50000k_plus INT,
                    population INT
                    )
                """

        self.cursor.execute(create_table_query)
        self.db.commit()

# alter_table_query = """
# ALTER TABLE nomis MODIFY COLUMN year INT
# """
# cursor.execute(alter_table_query)
