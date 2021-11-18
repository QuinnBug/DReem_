import csv
from collections import namedtuple
from datetime import datetime
from collections import namedtuple
import LocalAuthFinder as laf

attributes = ['CompanyName', 'CompanyNumber', 'RegAddress.CareOf', 'RegAddress.POBox', 'RegAddress.AddressLine1',
              'RegAddress.AddressLine2', 'RegAddress.PostTown', 'RegAddress.County', 'RegAddress.Country',
              'RegAddress.PostCode', 'CompanyCategory', 'CompanyStatus', 'CountryOfOrigin', 'DissolutionDate',
              'IncorporationDate', 'Accounts.AccountRefDay', 'Accounts.AccountRefMonth', 'Accounts.NextDueDate',
              'Accounts.LastMadeUpDate', 'Accounts.AccountCategory', 'Returns.NextDueDate', 'Returns.LastMadeUpDate',
              'Mortgages.NumMortCharges', 'Mortgages.NumMortOutstanding', 'Mortgages.NumMortPartSatisfied',
              'Mortgages.NumMortSatisfied', 'SICCode.SicText_1', 'SICCode.SicText_2', 'SICCode.SicText_3',
              'SICCode.SicText_4', 'LimitedPartnerships.NumGenPartners', 'LimitedPartnerships.NumLimPartners', 'URI',
              'PreviousName_1.CONDATE', 'PreviousName_1.CompanyName', 'PreviousName_2.CONDATE',
              'PreviousName_2.CompanyName', 'PreviousName_3.CONDATE', 'PreviousName_3.CompanyName',
              'PreviousName_4.CONDATE', 'PreviousName_4.CompanyName', 'PreviousName_5.CONDATE',
              'PreviousName_5.CompanyName', 'PreviousName_6.CONDATE', 'PreviousName_6.CompanyName',
              'PreviousName_7.CONDATE', 'PreviousName_7.CompanyName', 'PreviousName_8.CONDATE',
              'PreviousName_8.CompanyName', 'PreviousName_9.CONDATE', 'PreviousName_9.CompanyName',
              'PreviousName_10.CONDATE', 'PreviousName_10.CompanyName', 'ConfStmtNextDueDate', 'ConfStmtLastMadeUpDate']

index = namedtuple('IndexList',
                   ('name', 'status', 'category', 'close_date', 'postcode', 'sic1', 'sic2', 'sic3', 'sic4',
                    'start_date'))


def company_close_date(company):
    index_setup()
    if company[index.close_date] != '':
        return datetime.strptime(company[index.close_date], '%d/%m/%Y')
    else:
        return datetime.strptime('01/01/1000', '%d/%m/%Y')


def company_start_date(company):
    index_setup()
    if company[index.start_date] != '':
        return datetime.strptime(company[index.start_date], '%d/%m/%Y')
    else:
        return datetime.strptime('01/01/1000', '%d/%m/%Y')


def company_sic1(company):
    index_setup()
    return company[index.sic1][:5]


def index_setup():
    index.status = attributes.index('CompanyStatus')
    index.category = attributes.index('Accounts.AccountCategory')
    index.close_date = attributes.index('DissolutionDate')
    index.name = attributes.index('CompanyName')
    index.postcode = attributes.index('RegAddress.PostCode')
    index.sic1 = attributes.index('SICCode.SicText_1')
    index.sic2 = attributes.index('SICCode.SicText_2')
    index.sic3 = attributes.index('SICCode.SicText_3')
    index.sic4 = attributes.index('SICCode.SicText_4')
    index.start_date = attributes.index('IncorporationDate')


def is_active(company):
    index_setup()
    if (company[index.status] == "Active" and company[index.category] != "DORMANT") and company[index.close_date] == '':
        return "true"
    else:
        return "false"


def convert(row, postcodes, counties, ee_spec):
    index_setup()

    al = 'true'
    try:
        ll = postcodes[row[index.postcode]]
    except:
        al = 'false'
        ll = find_nearest_postcode(row[index.postcode], postcodes)

    c = [row[index.name], row[index.postcode], ll[0], ll[1], al, laf.find_local_auth(ll, counties)]

    if row[index.start_date] != '':
        c.append(datetime.strptime(row[index.start_date], '%d/%m/%Y'))
    else:
        c.append(datetime.strptime('01/01/1000', '%d/%m/%Y'))

    if row[index.close_date] != '':
        c.append(datetime.strptime(row[index.close_date], '%d/%m/%Y'))
    else:
        c.append(datetime.strptime('01/01/1000', '%d/%m/%Y'))

    try:
        c.append(ee_spec.sic_is_ee(int(row[index.sic1][:5])))  # if the sic code is in the list of ee sics
    except:
        c.append("false")
        # print('INVALID SIC CODE')

    c.append(0)
    c.append(0)
    c.append(row[index.sic1][:5])
    c.append(row[index.sic2][:5])
    c.append(row[index.sic3][:5])
    c.append(row[index.sic4][:5])
    c.append(is_active(row))

    return c


def nomis_setup(row):
    product = row

    del product[6]
    del product[7]
    del product[8]
    del product[9]
    del product[10]
    del product[11]
    del product[12]
    del product[13]
    del product[14]
    del product[15]
    del product[16]

    return product


def load_postcodes():
    with open('GB_full.txt') as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    latlon_list = {}
    for x in content:
        data = x.split("	")
        latlon_list[data[1]] = (data[9], data[10])
    return latlon_list


def find_nearest_postcode(postcode, postcodes):
    res = []
    ncode = postcode[:-1]
    while len(res) == 0:
        res = [val for key, val in postcodes.items() if ncode in key]
        if len(res) > 0:
            break
        ncode = ncode[:-1]
        if len(ncode) == 0:
            return ('0', '0')
    return res[0]


class EESpecification:
    def __init__(self, filename):
        self.header = []
        self.data = []
        with open(filename, encoding='cp437') as file:
            reader = csv.reader(file, delimiter=',')
            i = 0
            for row in reader:
                if i > 0:
                    self.data.append(row)
                else:
                    self.header = row
                i += 1

    def sic_codes(self):
        siccodes = []
        for row in self.data:
            siccodes.append(row[0])
        return siccodes

    def filter(self, area):
        if area not in self.header:
            return None
        i = self.header.index(area)
        siccodes = []
        for row in self.data:
            if row[i] == "1":
                siccodes.append(row[0])
        return siccodes

    def fetch_sic_section(self, sic):
        sicstr = ""
        if sic < 10000:
            sicstr = "0"
        sicstr += str(sic)
        sicf2 = int(sicstr[0] + sicstr[1])

        if 10 <= sicf2 <= 33:
            return "C"
        elif 45 <= sicf2 <= 47:
            return "G"
        elif 49 <= sicf2 <= 53:
            return "H"
        elif 55 <= sicf2 <= 56:
            return "I"
        elif 58 <= sicf2 <= 63:
            return "J"
        elif sicf2 == 68:
            return "L"
        elif 69 <= sicf2 <= 75:
            return "M"
        elif 77 <= sicf2 <= 82:
            return "N"
        elif sicf2 == 85:
            return "P"
        elif 90 <= sicf2 <= 93:
            return "R"
        elif 94 <= sicf2 <= 96:
            return "S"
        else:
            return "A,B,D,E"

    def check_ee_cat(self, sic):
        cat_list = []
        sic_row = 0
        for row in self.data:
            if row[0] == str(sic):
                sic_row = row
                break

        if sic_row == 0:
            # print("error")
            return []

        for cat in self.header:
            if sic_row[self.header.index(cat)] == "1":
                cat_list.append(self.header.index(cat))

        return cat_list

    def options(self):
        return self.header

    def sic_is_ee(self, code):
        for row in self.data:
            if row[0] == code:
                return "true"

        return "false"
