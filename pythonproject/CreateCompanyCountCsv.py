import DataConnector
import csv
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


def produce_csv(dc, filename):
    print("start")
    # update the index for nomis_ysla
    # update the index for companies_la
    dc.update_indexes('nomis', 'nomis_ysla_asc', 'year,sic,local_auth')
    dc.update_indexes('companies', 'ch_la_asc', 'local_authority')

    # open file and write column titles
    f = open(filename, 'w', newline='', encoding='cp437')
    writer = csv.writer(f, delimiter=',', quotechar='"')
    writer.writerow(["Local Authority", "Total Companies CH", "Total EE Companies CH", "2021 NOMIS Number of Companies",
                     "2021 NOMIS Turnover", "2021 NOMIS Employees", "Creators", "Primary suppliers",
                     "Secondary suppliers", "Distributors", "Providers", "Facilitators", "Business support",
                     "Creative Industries", "Digital sector sub-sector", "Cultural sector", "Tourism sector",
                     "Sports sector"])

    # for each distinct la
    dc.cursor.execute("SELECT DISTINCT local_authority FROM companies")
    local_auths = dc.cursor.fetchall()

    ch_select_q = 'SELECT is_ee FROM companies WHERE local_authority="{0}"'

    # this code lets you do specific ranges of local auths (mostly for testing)
    start_from = ""
    end_at = ""
    do_write = False

    for la in local_auths:
        if la[0] == start_from or start_from == "":
            do_write = True

        if not do_write:
            continue

        dc.cursor.execute(ch_select_q.format(la[0]))
        result = dc.cursor.fetchall()
        ch_total = ch_ee = 0

        for r in result:
            ch_total += 1
            if r[0] == "true":
                ch_ee += 1

        row = [la[0], ch_total, ch_ee]

        # returns [employee estimates, company counts, turnover estimates, category counts]
        data = nomis_data(dc, la[0], 2021)
        data[0] = data[0][0]
        data[2] = data[2][0]
        del data[3][0]

        for x in data:
            if isinstance(x, int):
                row.append(x)
            else:
                for i in x:
                    row.append(i)

        writer.writerow(row)

        if la[0] == end_at:
            break

    f.close()
    print("end")


def nomis_data(dc, la, year):
    # results are employees per year, companies per year, turnover total per year

    ee_specs = cf.EESpecification("definition.csv")

    category_counts = []
    for i in ee_specs.header:
        category_counts.append(0)

    results = [0, 0, 0]
    min_results = [0, 0, 0]
    max_results = [0, 0, 0]

    # get the num of companies
    search_query = 'SELECT employees, total_count, 25k, 75k, 150k, 350k, 750k, 1500k, 3500k, 7500k, 25000k,' \
                   '50000k_plus, employees_min, employees_max' \
                   ' FROM nomis WHERE year={0} AND sic={1} AND local_auth="{2}"'

    for sic in ee_sics:
        dc.cursor.execute(search_query.format(year, sic, la))
        returneds = dc.cursor.fetchall()

        for result in returneds:
            results[0] += result[0]
            results[1] += result[1]
            results[2] += result[2] * 25000
            results[2] += result[3] * 75000
            results[2] += result[4] * 150000
            results[2] += result[5] * 350000
            results[2] += result[6] * 750000
            results[2] += result[7] * 1500000
            results[2] += result[8] * 3500000
            results[2] += result[9] * 7500000
            results[2] += result[10] * 25000000
            results[2] += result[11] * 75000000

            min_results[0] += result[12]
            min_results[1] += result[1]
            min_results[2] += result[2] * 0
            min_results[2] += result[3] * 50000
            min_results[2] += result[4] * 100000
            min_results[2] += result[5] * 200000
            min_results[2] += result[6] * 500000
            min_results[2] += result[7] * 1000000
            min_results[2] += result[8] * 2000000
            min_results[2] += result[9] * 5000000
            min_results[2] += result[10] * 10000000
            min_results[2] += result[11] * 50000000

            max_results[0] += result[13]
            max_results[1] += result[1]
            max_results[2] += result[2] * 49000
            max_results[2] += result[3] * 99000
            max_results[2] += result[4] * 199000
            max_results[2] += result[5] * 499000
            max_results[2] += result[6] * 999000
            max_results[2] += result[7] * 1999000
            max_results[2] += result[8] * 4999000
            max_results[2] += result[9] * 9999000
            max_results[2] += result[10] * 49999000
            max_results[2] += result[11] * 100000000

        for index in ee_specs.check_ee_cat(sic):
            category_counts[index] += results[1]

    e = [results[0], min_results[0], max_results[0]]
    c = results[1]
    t = [results[2], min_results[2], max_results[2]]
    return [e, c, t, category_counts]


dc = DataConnector.DataBase()

dc.connect_to_db("root", "pw123", "companies_house")

produce_csv(dc, "2021CompanyCounts.csv")
