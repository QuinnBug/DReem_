from datetime import datetime
import DataConnector
import plotly.graph_objects as pgo
import seaborn as sb
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import json
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon, LinearRing
from PIL import Image
import geotiler
import geopandas as gp
from CompanyFunctions import EESpecification as ees

ee_sics = [70210, 73110, 73120, 71111, 32120, 32130, 74100, 59111, 59112, 59113, 59120, 59131, 59133, 59140,
           60100, 60200, 74201, 74202, 74209, 58210, 58290, 62011, 62020, 62090, 58110, 58120, 58130, 58141,
           58142, 58190, 91012, 91020, 59200, 85520, 90010, 90020, 90030, 90040, 26110, 26120, 26200, 26301,
           74300, 91011, 47630, 91030, 77351, 79110, 79120, 79901, 79909, 82300, 91040, 92000, 93110, 93120,
           26309, 26400, 26800, 46510, 46520, 61100, 61200, 61300, 61900, 62030, 63110, 63120, 63910, 63990,
           95110, 95120, 18201, 32200, 68202, 77110, 93130, 93199, 77210, 77220, 77291, 77341, 93210, 93290,
           49000, 50000, 51000, 55100, 55201, 55202, 55209, 55300, 55900, 56101, 56102, 56210, 56301, 56302,
           85510]


def make_line_graph(graph_data, title, filename, with_error, x_title, y_title):
    fig = pgo.Figure()

    if with_error:
        for x_val in graph_data[0][0]:
            index = graph_data[0][0].index(x_val)
            x_arr = [x_val]
            y_arr = np.linspace(graph_data[2][1][index], graph_data[1][1][index], 100)
            for y in y_arr:
                x_arr.append(x_val)
            colorscale = [[0, 'rgba(225, 225, 225, 1)'],
                          [0.5, 'rgba(75, 75, 75, 1)'],
                          [1, 'rgba(225, 225, 225, 1)']]
            fig.add_trace(pgo.Scatter(x=x_arr, y=y_arr,
                                      mode='markers', marker={'color': y_arr, 'colorscale': colorscale, 'size': 5}))

        fig.add_trace(pgo.Scatter(x=graph_data[0][0], y=graph_data[0][1], mode='lines+markers',
                                  marker={'color': 'black'}, line={'color': 'black'}))
    else:
        for data in graph_data:
            fig.add_trace(pgo.Scatter(x=data[0], y=data[1], mode='lines+markers',
                                      marker={'color': 'black'}, line={'color': 'black'}))

    # fig.write_html(filename + ".html")
    fig.update_xaxes(showgrid=True, gridcolor='gray')
    fig.update_yaxes(showgrid=True, gridcolor='gray')
    # fig.update_layout(showlegend=False, xaxis_title=x_title, yaxis_title=y_title)
    fig.update_layout(showlegend=False, xaxis_title=x_title, yaxis_title=y_title,
                      font=dict(size=18, color="black"))

    fig.update_layout(title=pgo.layout.Title(text=title, font=dict(size=16, color="black")),
                      plot_bgcolor='white')

    # fig.write_image(filename + ".pdf")
    fig.write_image(filename + ".png")
    # fig.write_image(filename + ".svg")


def make_pie_graph(graph_data, graph_labels, title, filename, as_percent):
    if as_percent:
        fig = pgo.Figure(data=[pgo.Pie(values=graph_data, labels=graph_labels, textinfo="percent")],
                         layout=pgo.Layout(title=pgo.layout.Title(text=title), plot_bgcolor='white'))
    else:
        fig = pgo.Figure(data=[pgo.Pie(values=graph_data, labels=graph_labels, textinfo="value")],
                         layout=pgo.Layout(title=pgo.layout.Title(text=title), plot_bgcolor='white'))

    fig.update_layout(showlegend=True, font=dict(size=18, color="black"))

    fig.update_layout(title=pgo.layout.Title(text=title, font=dict(size=16, color="black")),
                      plot_bgcolor='white')

    # fig.write_image(filename + ".pdf")
    fig.write_image(filename + ".png")
    # fig.write_image(filename + ".svg")


def make_histogram(graph_data, title, filename, x_title, y_title):
    fig = pgo.Figure(data=[pgo.Bar(x=graph_data[0], y=graph_data[1])],
                     layout=pgo.Layout(title=pgo.layout.Title(text=title), plot_bgcolor='white'))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor='gray')
    fig.update_traces(marker_color='rgba(75, 75, 75, 1.0)')
    # fig.update_layout(showlegend=False, xaxis_title=x_title, yaxis_title=y_title)
    fig.update_layout(showlegend=False, xaxis_title=x_title, yaxis_title=y_title,
                      font=dict(size=18, color="black"))

    fig.update_layout(title=pgo.layout.Title(text=title, font=dict(size=16, color="black")),
                      plot_bgcolor='white')

    # fig.write_image(filename + ".pdf")
    fig.write_image(filename + ".png")
    # fig.write_image(filename + ".svg")


def ee_v_all_pie_data(dc, year, la):
    # results are ee companies per year, all companies per year
    results = [0, 0]

    # get the num of companies
    search_query = 'SELECT sic,total_count FROM nomis WHERE year={0} AND local_auth="{1}"'

    dc.cursor.execute(search_query.format(year, la))
    returneds = dc.cursor.fetchall()

    for result in returneds:
        if ee_sics.__contains__(result[0]):
            results[0] += result[1]
        else:
            results[1] += result[1]

    return results


def percent_of_category_pie_data(dc, year, la, ee_specs):
    # results are ee companies per year, all companies per year

    results = []
    for i in ee_specs.header:
        results.append(0)

    # get the num of companies
    search_query = 'SELECT name, sic1 FROM companies WHERE is_ee="true" AND local_authority="{0}" AND is_active="true"'
    # search_query = 'SELECT sic, total_count FROM nomis WHERE year=2021 AND local_auth="{0}"'

    dc.cursor.execute(search_query.format(la))
    returneds = dc.cursor.fetchall()

    for result in returneds:
        for i in ee_specs.check_ee_cat(result[1]):
            results[i] += 1

    # for result in returneds:
    #     for i in ee_specs.check_ee_cat(result[0]):
    #         results[i] += result[1]

    del results[:8]

    total = 0
    for val in results:
        total += val

    return results


def lifespan_of_company_data(dc, start_date, la):
    x_vals = [0, 6, 12, 18, 24, 30, 36]
    x_vals_str = ["0 - 5", "6 - 11", "12 - 17", "18 - 23", "24 - 29", "30 - 35", "36+"]
    date_strings = ["2021-11-01", "2021-05-01", "2020-11-01", "2020-05-01", "2019-11-01", "2019-05-01", "2018-11-01"]

    values = []
    for i in x_vals:
        values.append(0)
        # x_vals_str.append(str(i) + " months")

    search_query = 'SELECT incorporation_date, closure_date FROM companies WHERE' \
                   ' is_ee="true" AND local_authority="{0}"'
    dc.cursor.execute(search_query.format(la))
    all_ee_companies = dc.cursor.fetchall()

    for company in all_ee_companies:
        sd = company[0]
        if str(company[1]) == "1000-01-01":
            ed = datetime.strptime(start_date, '%Y-%m-%d')
        else:
            # continue
            ed = company[1]

        months_open = (ed.year - sd.year) * 12 + ed.month - sd.month

        for i in range(len(x_vals)):
            # if open more than x and less than x+1 then
            if x_vals[i] == 36:
                values[i] += 1
                break

            if x_vals[i] <= months_open < x_vals[i+1]:
                values[i] += 1
                break

    return [x_vals_str, values]


def freelancer_estimate(dc, year, la, ee_specs):
    pop_query = 'SELECT population FROM nomis WHERE year={0} AND local_auth="{1}" LIMIT 1'
    dc.cursor.execute(pop_query.format(year, la))
    la_pop = dc.cursor.fetchall()
    if len(la_pop) < 1:
        return [1, 1]

    la_pop = la_pop[0][0]
    total_pop = 67081234
    pop_percent = la_pop / total_pop

    sections = ["A,B,D,E", "C", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S"]
    sections_total_fl_count = [175000, 180000, 801000, 333000, 266000, 141000, 191000,
                               98000, 69000, 557000, 306000, 40000, 244000, 299000, 576000, 576000]
    sections_sics = [[], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []]

    dc.cursor.execute("SELECT DISTINCT sic FROM nomis")
    allsics = dc.cursor.fetchall()

    # dc.cursor.execute('SELECT sic, total_count FROM nomis WHERE year=2021 AND local_auth="' + la + '"')
    sic_query = 'SELECT 1 FROM companies WHERE {0}="{1}" AND local_authority="{2}" AND is_active="true"'

    # get important sections ee_sics and all_sics (some sections have 0 ee sics)
    for sic in allsics:
        sic = sic[0]
        section = ee_specs.fetch_sic_section(sic)
        sections_sics[sections.index(section)].append(sic)

    sector_sics = []
    for i in ee_specs.header:
        sector_sics.append([])

    sector_sics_company_counts = []
    total_sector_company_counts = []

    for sic in ee_sics:
        indexes = ee_specs.check_ee_cat(sic)
        for i in indexes:
            sector_sics[i].append(sic)

    cat_list = ["sic1", "sic2", "sic3", "sic4"]

    for sector in sector_sics:
        sector_sics_company_counts.append([])
        total_sector_company_counts.append(0)
        for sic in sector:
            sector_sics_company_counts[sector_sics.index(sector)].append(0)
            index = len(sector_sics_company_counts[sector_sics.index(sector)]) - 1
            for cat in cat_list:
                dc.cursor.execute(sic_query.format(cat, sic, la))
                sic_counts = dc.cursor.fetchall()
                for i in sic_counts:
                    sector_sics_company_counts[sector_sics.index(sector)][index] += 1
                    total_sector_company_counts[sector_sics.index(sector)] += 1

    sector_ee_fl_counts = []

    for sector in sector_sics:
        index = sector_sics.index(sector)
        sector_ee_fl_counts.append(0)

        for sic in sector:
            section_i = sections.index(ee_specs.fetch_sic_section(sic))
            sfl = sections_total_fl_count[section_i]
            # sfl * percent of this sector that this sic is
            pcent_sic_of_sector = sector_sics_company_counts[index][sector.index(sic)]/total_sector_company_counts[index]
            pcent_of_section = 1/len(sections_sics[section_i])

            # value = sfl * percent_of_section * pop_percent
            value = sfl * pcent_of_section
            value *= pop_percent
            value *= pcent_sic_of_sector

            # if sector_sics_company_counts[index][sector.index(sic)] == 0:
            #     value *= 1
            # else:
            #     value *= sector_sics_company_counts[index][sector.index(sic)] * 1/total_sector_company_counts[index]

            # value *= sector_sics_company_counts[index][sector.index(sic)] / total_sector_company_counts[index]

            sector_ee_fl_counts[index] += value

    del sector_ee_fl_counts[:8]

    for i in range(len(sector_ee_fl_counts)):
        sector_ee_fl_counts[i] = int(sector_ee_fl_counts[i])

    return sector_ee_fl_counts


def nomis_graph_data(dc, la, years):
    x = years
    # results are employees per year, companies per year, turnover total per year
    results = [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]]
    min_results = [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]]
    max_results = [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]]

    # get the num of companies
    search_query = 'SELECT employees, total_count, 25k, 75k, 150k, 350k, 750k, 1500k, 3500k, 7500k, 25000k,' \
                   '50000k_plus, employees_min, employees_max' \
                   ' FROM nomis WHERE year={0} AND sic={1} AND local_auth="{2}"'

    for year in x:
        for sic in ee_sics:
            dc.cursor.execute(search_query.format(year, sic, la))
            returneds = dc.cursor.fetchall()

            for result in returneds:
                results[0][x.index(year)] += result[0]
                results[1][x.index(year)] += result[1]
                results[2][x.index(year)] += result[2] * 25000
                results[2][x.index(year)] += result[3] * 75000
                results[2][x.index(year)] += result[4] * 150000
                results[2][x.index(year)] += result[5] * 350000
                results[2][x.index(year)] += result[6] * 750000
                results[2][x.index(year)] += result[7] * 1500000
                results[2][x.index(year)] += result[8] * 3500000
                results[2][x.index(year)] += result[9] * 7500000
                results[2][x.index(year)] += result[10] * 25000000
                results[2][x.index(year)] += result[11] * 75000000

                min_results[0][x.index(year)] += result[12]
                min_results[1][x.index(year)] += result[1]
                min_results[2][x.index(year)] += result[2] * 0
                min_results[2][x.index(year)] += result[3] * 50000
                min_results[2][x.index(year)] += result[4] * 100000
                min_results[2][x.index(year)] += result[5] * 200000
                min_results[2][x.index(year)] += result[6] * 500000
                min_results[2][x.index(year)] += result[7] * 1000000
                min_results[2][x.index(year)] += result[8] * 2000000
                min_results[2][x.index(year)] += result[9] * 5000000
                min_results[2][x.index(year)] += result[10] * 10000000
                min_results[2][x.index(year)] += result[11] * 50000000

                max_results[0][x.index(year)] += result[13]
                max_results[1][x.index(year)] += result[1]
                max_results[2][x.index(year)] += result[2] * 49000
                max_results[2][x.index(year)] += result[3] * 99000
                max_results[2][x.index(year)] += result[4] * 199000
                max_results[2][x.index(year)] += result[5] * 499000
                max_results[2][x.index(year)] += result[6] * 999000
                max_results[2][x.index(year)] += result[7] * 1999000
                max_results[2][x.index(year)] += result[8] * 4999000
                max_results[2][x.index(year)] += result[9] * 9999000
                max_results[2][x.index(year)] += result[10] * 49999000
                max_results[2][x.index(year)] += result[11] * 100000000

    e = [[x, results[0], "Average"], [x, min_results[0], "Minimum"], [x, max_results[0], "Maximum"]]
    c = [[x, results[1], "Company Count"]]
    t = [[x, results[2], "Average"], [x, min_results[2], "Minimum"], [x, max_results[2], "Maximum"]]
    return [e, c, t]


def ch_graph_data(dc, la):
    date_strings = ["2020-12-01", "2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01", "2021-05-01",
                    "2021-06-01", "2021-07-01", "2021-08-01", "2021-09-01", "2021-10-01"]

    values = []
    for i in date_strings:
        values.append(0)

    # get the num of companies
    search_query = 'SELECT incorporation_date, closure_date FROM companies WHERE is_ee="true" and local_authority="{0}"'
    dc.cursor.execute(search_query.format(la))
    all_ee_companies = dc.cursor.fetchall()

    for company in all_ee_companies:
        i = 0
        while str(company[0]) > str(date_strings[i]):
            i += 1

        closed = False
        while (i < len(date_strings)) & (closed is False):
            closed = (str(company[1]) < str(date_strings[i])) and (str(company[1]) != "1000-01-01")
            if closed is False:
                values[i] += 1
                i += 1
            else:
                print(str(company[0]) + " - " + str(company[1]) + " :: " + str(i))

    if values[9] != values[10]:
        print(la)

    return [date_strings, values]


def find_p(z):
    lats = []
    lons = []
    for pt in z:
        lats.append(pt[0])
        lons.append(pt[1])
    lons_lats_vect = np.column_stack((lats, lons))
    return Polygon(lons_lats_vect)


def make_heatmap(df, bounds, counties, la):
    p_list = []
    ring_list = []
    found = False

    plt.clf()

    for c in counties['features']:
        if c['properties']['geo_label'] == la:
            if len(c['geometry']['coordinates']) > 1:
                p_list = c['geometry']['coordinates']
            else:
                p_list = [c['geometry']['coordinates']]

            found = True
            break

    if not found:
        print(la + " la -> geo_label missing")
        return

    for z1 in p_list:
        for z2 in z1:
            ring_list.append(z2)

    for x in ring_list:
        if len(x) < 3:
            print(la + " short ring list")
            return

    l = bounds[1][0]
    r = bounds[1][1]
    b = bounds[0][0]
    t = bounds[0][1]

    for x1 in ring_list:
        for x in x1:
            if x[0] > r:
                r = x[0]
            elif x[0] < l:
                l = x[0]

            if x[1] > t:
                t = x[1]
            elif x[1] < b:
                b = x[1]

    if r - l > t - b:
        diff = r - l
        mid = t - ((t - b) / 2)
        b = mid - (diff / 2)
        t = mid + (diff / 2)
    else:
        diff = t - b
        mid = r - ((r - l) / 2)
        l = mid - (diff / 2)
        r = mid + (diff / 2)

    poly_list = []

    for x in ring_list:
        poly_list.append(Polygon(x))

    mask_poly = Polygon(([l, b], [r, b], [r, t], [l, t]), [inner.exterior.coords for inner in poly_list])
    white_border = gp.GeoSeries(mask_poly)
    white_border.plot(zorder=2, cmap="Greys", alpha=0.8)

    for i in poly_list:
        x, y = i.exterior.xy
        plt.plot(x, y, c="red", zorder=3)

    sb.set_theme(style="dark")
    sb.kdeplot(data=df, x="longitude", y="latitude", fill=True, bw_adjust=.5, thresh=0.01, zorder=1, alpha=0.6)

    # current_map = geotiler.Map(center=(r - (diff / 2), mid), zoom=10, size=(600, 600))
    current_map = geotiler.Map(extent=(l, b, r, t), zoom=10)
    map_img = geotiler.render_map(current_map)
    map_img.save('Graphs/temp_1.png')
    plt.imshow(map_img, zorder=0, extent=([l, r, b, t]))
    # plt.savefig('Graphs/temp_2.png')

    plt.savefig("temp.png")

    hm_img = Image.open("temp.png")
    # l = 144,b = 53, r = 118,t = 59
    cropped_img = hm_img.crop((144, 59, 640 - 118, 480 - 53))
    cropped_img.save("Graphs/Heatmaps/" + la + ".png")
    plt.close()


def ch_heatmap_data(dc, local_auth):
    dc.update_indexes("companies", "ch_ee", "is_ee")
    dc.update_indexes("companies", "ch_la_asc", "local_authority")

    search_q = 'SELECT lat, lon FROM companies WHERE is_ee="true" and local_authority="{0}"'
    dc.cursor.execute(search_q.format(local_auth))
    all_ee_companies = dc.cursor.fetchall()

    lats = []
    longs = []

    for result in all_ee_companies:
        lats.append(float(result[0]))
        longs.append(float(result[1]))

    d = {'latitude': lats, 'longitude': longs}
    df = pd.DataFrame(data=d)

    lats.sort()
    longs.sort()

    bounds = [[lats[0], lats[len(lats) - 1]], [longs[0], longs[len(longs) - 1]]]

    return [df, bounds]
