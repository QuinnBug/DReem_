import csv
import DataConnector
import CompanyFunctions as cf
from datetime import date, datetime
from collections import namedtuple
from LocalAuthFinder import Counties

ch_idx = namedtuple("indexes",
                    ("name", "sic1", "active", "close_date",))

n_idx = namedtuple("indexes",
                   ("year", "local_auth", "sic", "employees", "band", "count"))

r_idx = namedtuple("indexes",
                   ("year", "local_auth", "sic", "e_mid", "e_min", "e_max", "band_start", "count_start"))


def setup_r_idx():
    r_idx.year = 0
    r_idx.local_auth = 1
    r_idx.sic = 2
    r_idx.e_mid = 3
    r_idx.e_min = 4
    r_idx.e_max = 5
    r_idx.band_start = 6
    r_idx.count_start = 7


def setup_ch_idx():
    ch_idx.name = 1
    ch_idx.sic1 = 12
    ch_idx.close_date = 8
    ch_idx.active = 16


def setup_n_idx():
    n_idx.year = 0
    n_idx.local_auth = 1
    n_idx.sic = 2
    n_idx.employees = 3
    n_idx.band = 4
    n_idx.count = 5


def add_ch_to_database(filename, dc):
    with open(filename, newline='', encoding='cp437') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')

        setup_ch_idx()
        p = cf.load_postcodes()
        c = Counties()
        e = cf.EESpecification("definition.csv")

        update_count = 0
        insert_count = 0
        none_count = 0

        inserts = []
        for row in reader:

            if row[0] == "CompanyName":
                continue

            # get the current entry if the company is in the database
            sql_name = row[0].replace('"', '\\"')
            select_query = 'SELECT is_active, closure_date, incorporation_date, is_ee FROM companies WHERE name="{0}" LIMIT 1'
            dc.cursor.execute(select_query.format(sql_name))
            result = dc.cursor.fetchone()

            sd = cf.company_start_date(row)

            if str(sd) > "2021-10-01":
                print(str(sd.date()) + " - greater than october")
            if str(sd) == "1000-01-01":
                print(str(sd) + " - invalid?")

            if dc.cursor.rowcount > 0:
                active = cf.is_active(row)
                cd = cf.company_close_date(row).date()

                # if (result[0] != active) or (str(result[1]) != str(cd)):
                #     update_query = 'UPDATE companies SET closure_date="{0}", is_active="{1}" WHERE name="{2}" LIMIT 1'
                #
                #     dc.cursor.execute(update_query.format(cd, active, sql_name))

                is_ee = e.sic_is_ee(cf.company_sic1(row))
                if str(result[2]) != str(sd.date()) or is_ee != result[3]:
                    update_query = 'UPDATE companies SET incorporation_date="{0}", is_ee="{1}" WHERE name="{2}" LIMIT 1'
                    dc.cursor.execute(update_query.format(sd.date(), is_ee, sql_name))
                    dc.commit_to_db()

                    print(str(sd.date()) + " - " + str(result[2]) + " -- updated")

                    update_count += 1
                else:
                    none_count += 1
            else:
                insert_companies_query = """
                        INSERT INTO companies
                        (name, postcode, lat, lon, actual_location, local_authority,
                         incorporation_date, closure_date, is_ee, est_employees, est_value, sic1, sic2, sic3, sic4, is_active)
                        VALUES ( %s, %s ,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """

                data = cf.convert(row, p, c, e)
                dc.cursor.execute(insert_companies_query, data)
                dc.commit_to_db()

                if str(sd.date()) > "2021-10-01":
                    a = 1

                insert_count += 1

        dc.cursor.execute("ALTER TABLE companies DROP INDEX ch_names_asc")
        dc.cursor.execute("CREATE INDEX ch_names_asc ON companies(name ASC)")
        dc.commit_to_db()


def add_nomis_to_database(filename, dc):
    row_list = []

    with open(filename, newline='', encoding='cp437') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')

        setup_n_idx()
        setup_r_idx()

        inserts = []

        last_line = ["0", "0", "0"]
        for row in reader:
            if row[0] != last_line[0] or row[1] != last_line[1] or row[2] != last_line[2]:
                row_list.append(row)
                last_line = row

    counter = 0
    print("starting to read files @ " + datetime.now().strftime("%H:%M:%S"))
    while len(row_list) > 0:
        row = row_list[0]

        # get the current entry if the company is in the database
        select_query = 'SELECT * FROM nomis WHERE year={0} AND sic={1} AND local_auth="{2}" LIMIT 1'

        update_query = '''
        UPDATE nomis SET employees={3},employees_min={4},employees_max={5} 
        WHERE year={0} AND sic={1} AND local_auth="{2}" LIMIT 1
        '''

        dc.cursor.execute(select_query.format(row[r_idx.year], row[r_idx.sic], row[r_idx.local_auth]))
        result = dc.cursor.fetchone()

        if dc.cursor.rowcount < 1:
            inserts.append(cf.nomis_setup(row_list[0]))
        elif result[5] != int(row[r_idx.e_min]) or\
                result[6] != int(row[r_idx.e_max]) or\
                result[4] != int(row[r_idx.e_mid]):
            dc.cursor.execute(update_query.format(row[r_idx.year], row[r_idx.sic], row[r_idx.local_auth],
                                                  row[r_idx.e_mid], row[r_idx.e_min], row[r_idx.e_max]))
            dc.commit_to_db()

        # del row_list[:11]
        del row_list[0]

        if len(inserts) >= 500 or len(row_list) <= 0:
            counter += 1
            insert_companies_query = """
                    INSERT INTO nomis
                    (year, local_auth, sic, employees, employees_min, employees_max, total_count,
                    25k, 75k, 150k, 350k, 750k, 1500k, 3500k, 7500k, 25000k, 50000k_plus)
                    VALUES ( %s, %s ,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
            print("inserting companies " + str(counter))
            dc.cursor.executemany(insert_companies_query, inserts[:500])
            del inserts[:500]
            dc.cursor.execute("ALTER TABLE nomis DROP INDEX nomis_ysla_asc")
            dc.cursor.execute("CREATE INDEX nomis_ysla_asc ON nomis(year,sic,local_auth)")
            dc.commit_to_db()

    print("done :: " + " @ " + datetime.now().strftime("%H:%M:%S"))
    dc.cursor.execute("ALTER TABLE nomis DROP INDEX nomis_la_asc")
    dc.cursor.execute("CREATE INDEX nomis_la_asc ON nomis(local_auth)")
    dc.commit_to_db()
    return inserts


def save_file(filename, content):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerows(content)
