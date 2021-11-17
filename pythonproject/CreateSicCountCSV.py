import DataConnector
import csv

ee_sics = [70210, 73110, 73120, 71111, 32120, 32130, 74100, 59111, 59112, 59113, 59120, 59131, 59133, 59140,
           60100, 60200, 74201, 74202, 74209, 58210, 58290, 62011, 62020, 62090, 58110, 58120, 58130, 58141,
           58142, 58190, 91012, 91020, 59200, 85520, 90010, 90020, 90030, 90040, 26110, 26120, 26200, 26301,
           74300, 91011, 47630, 91030, 77351, 79110, 79120, 79901, 79909, 82300, 91040, 92000, 93110, 93120,
           26309, 26400, 26800, 46510, 46520, 61100, 61200, 61300, 61900, 62030, 63110, 63120, 63910, 63990,
           95110, 95120, 18201, 32200, 68202, 77110, 93130, 93199, 77210, 77220, 77291, 77341, 93210, 93290,
           49000, 50000, 51000, 55100, 55201, 55202, 55209, 55300, 55900, 56101, 56102, 56210, 56301, 56302,
           85510]


def produce_csv(dc, filename):
    print("start")
    # update the index for nomis_ysla
    # update the index for companies_la
    dc.update_indexes('nomis', 'nomis_ysla_asc', 'year,sic,local_auth')
    dc.update_indexes('companies', 'ch_la_asc', 'local_authority')

    # open file and write column titles
    f = open(filename, 'w', newline='', encoding='cp437')
    writer = csv.writer(f, delimiter=',', quotechar='"')

    row = ["Local Authority"]

    for sic in ee_sics:
        row.append(sic)

    writer.writerow(row)

    # for each distinct la
    dc.cursor.execute("SELECT DISTINCT local_authority FROM companies")
    local_auths = dc.cursor.fetchall()

    # this code lets you do specific ranges of local auths (mostly for testing)
    start_from = ""
    end_at = ""
    do_write = False

    for la in local_auths:
        if la[0] == start_from or start_from == "":
            do_write = True

        if not do_write:
            continue

        row = [la[0]]

        # returns [sic_1_total,sic_2_total,sic_3_total,etc]
        data = nomis_data(dc, la[0], 2021)

        for x in data:
            row.append(x)

        writer.writerow(row)

        if la[0] == end_at:
            break

    f.close()
    print("end")


def nomis_data(dc, la, year):
    # results are company count per local auth

    results = []
    for i in ee_sics:
        results.append(0)

    # get the num of companies
    search_query = 'SELECT total_count FROM nomis WHERE year={0} AND sic={1} AND local_auth="{2}"'

    for sic in ee_sics:
        dc.cursor.execute(search_query.format(year, sic, la))
        returneds = dc.cursor.fetchall()
        index = ee_sics.index(sic)
        for result in returneds:
            results[index] += result[0]

    return results


dc = DataConnector.DataBase()

dc.connect_to_db("root", "pw123", "companies_house")

produce_csv(dc, "2021SicCounts.csv")
