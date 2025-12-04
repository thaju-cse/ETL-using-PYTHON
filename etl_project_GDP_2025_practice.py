import requests
import pandas as pd
import numpy as np
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime


def extract(url):
    print("Extracting process initiating..")
    
    raw_request = requests.get(url)
    print("Data pulled successfully..")
    
    raw_text = raw_request.text
    soup_raw = BeautifulSoup(raw_text, 'html.parser')
    tables = soup_raw.find_all('table')
    table = tables[2].find_all('tr')
    print("Almost there..")
    
    gdp_data = []
    r = []
    for element in table[0].find_all('th'):
        r.append(element.text)
    gdp_data.append(r)
    for row in table[1:]:
        r = []
        for element in row.find_all('td'):
            r.append(element.text)
        gdp_data.append(r)

    print("Task Completed.")
    
    return gdp_data
        
def transform(gdp_data_list):
    print("Started Transforming data..")

    transformed_gdp_data = [['Country/Territory','IMF_2025','World_Bank_22_24','United_Nations_23']]
    for row in gdp_data_list[1:]:
        r = []
        try:
            one = row[0].replace('\xa0','')
        except:
            one = ''
        try:
            two = int(row[1].replace(',','')) / 1000
        except:
            two = 0
        try:
            three = int(row[2].replace(',','')) / 1000
        except:
            three = 0
        try:
            four = int(row[3].replace(',','')) / 1000
        except:
            four = 0
        transformed_gdp_data.append([one,two,three,four])

    print("Transforming Task Completed.")

    return transformed_gdp_data
        
def loading(transformed_gdp_data):
    print("Initiating Loading process..")

    csv_path = 'gdp_data_2025_csv.csv'
    db_path = 'gdp_data_2025_db.db'
    table_path = 'gdp_data_2025_table'
    data_frame = pd.DataFrame(columns = ['Country/Territory', 'IMF_2025', 'World_Bank_22_24', 'United_Nations_23'])

    for row in transformed_gdp_data[1:]:
        row_dict = {'Country/Territory': row[0],
                    'IMF_2025': row[1],
                    'World_Bank_22_24': row[2],
                    'United_Nations_23': row[3]}
        temp_data_frame = pd.DataFrame(row_dict, index = [0])
        data_frame = pd.concat([data_frame, temp_data_frame], ignore_index = True)

    print("Inserting data into csv file..")
    with open(csv_path, 'w') as file:
        data_frame.to_csv(file, sep = ',')

    print("Inserting data into database table..")
    conn = sqlite3.connect(db_path)
    data_frame.to_sql(table_path, conn, if_exists = 'replace', index = False)
    conn.close

    print("Loading successfully Completed.")

    return { 'csv_file_name': csv_path, 'db_name': db_path, 'table_name': table_path}

def run_query(query, db_name, table_name):
    print('Query execution started..')
    try:
        sql_connection = sqlite3.connect(db_name)
        data_frame = pd.read_sql(query,sql_connection)

        print("Execution of the query completed, printing and returning Data Frame..")
        print(data_frame)
    
        log_process(query ,True)
        return True
    
    except:
        print("***Querying process FAILED***")
        log_process(query, False)
        return False

def log_process(query, value):
    print("Started Logging the process..")
    
    log_file = 'log_process.txt'
    if value:

        with open(log_file, 'a') as file:
            file.write('DATE and TIME:' +str(datetime.now())+'Query:\t'+ query +'Executed Successfully.')
    
    else:
        with open(log_file, 'a') as file:
            file.write('***DATE and TIME:' +str(datetime.now())+'Query:\t'+ query +'Execution Failed.')
    
        
    print("Logging process Completed.")
    return True


def main():
    url = 'https://web.archive.org/web/20250913062823/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    gdp_data_list = extract(url)
    #gdp_data_list = [['Country/Territory\n', 'IMF(2025)[1][6]\n', 'World Bank(2022–24)[7]\n', 'United Nations(2023)[8]\n'], ['\xa0World', '113,795,678', '111,326,370', '100,834,796\n'], ['\xa0United States', '30,507,217', '29,184,890', '27,720,700\n'], ['\xa0China[n 1][n 3]', '19,231,705', '18,743,803', '17,794,782\n'], ['\xa0Germany', '4,744,804', '4,659,929', '4,525,704\n'], ['\xa0India', '4,187,017', '3,912,686', '3,575,778\n'], ['\xa0Japan', '4,186,431', '4,026,211', '4,204,495\n'], ['\xa0United Kingdom', '3,839,180', '3,643,834', '3,380,855\n'], ['\xa0France', '3,211,292', '3,162,079', '3,051,832\n'], ['\xa0Italy', '2,422,855', '2,372,775', '2,300,941\n'], ['\xa0Canada', '2,225,341', '2,241,253', '2,142,471\n'], ['\xa0Brazil', '2,125,958', '2,179,412', '2,191,132\n'], ['\xa0Russia', '2,076,396', '2,173,836', '2,008,419\n'], ['\xa0Spain', '1,799,511', '1,722,746', '1,620,091\n'], ['\xa0South Korea', '1,790,322', '1,712,793', '1,839,058\n'], ['\xa0Australia', '1,771,681', '1,752,193', '1,775,628\n'], ['\xa0Mexico', '1,692,640', '1,852,723', '1,793,799\n'], ['\xa0Turkey', '1,437,406', '1,323,255', '1,118,253\n'], ['\xa0Indonesia', '1,429,743', '1,396,300', '1,371,171\n'], ['\xa0Netherlands', '1,272,011', '1,227,544', '1,154,361\n'], ['\xa0Saudi Arabia', '1,083,749', '1,237,530', '1,067,583\n'], ['\xa0Poland', '979,960', '914,696', '809,201\n'], ['\xa0Switzerland', '947,125', '936,564', '894,425\n'], ['\xa0Taiwan[n 4]', '804,889', '—', '—\n'], ['\xa0Belgium', '684,864', '664,564', '644,783\n'], ['\xa0Argentina', '683,533', '633,267', '646,075\n'], ['\xa0Sweden', '620,297', '610,118', '584,960\n'], ['\xa0Ireland', '598,840', '577,389', '551,395\n'], ['\xa0Israel', '583,361', '540,380', '513,611\n'], ['\xa0Singapore', '564,774', '547,387', '501,428\n'], ['\xa0United Arab Emirates', '548,598', '537,079', '514,130\n'], ['\xa0Thailand', '546,224', '526,411', '514,945\n'], ['\xa0Austria', '534,301', '521,642', '511,685\n'], ['\xa0Norway', '504,276', '483,727', '485,311\n'], ['\xa0Philippines', '497,495', '461,618', '437,146\n'], ['\xa0Vietnam', '490,970', '476,388', '429,717\n'], ['\xa0Bangladesh', '467,218', '450,119', '422,431\n'], ['\xa0Denmark', '449,940', '429,457', '407,092\n'], ['\xa0Malaysia', '444,984', '421,972', '399,649\n'], ['\xa0Colombia', '427,766', '418,542', '363,540\n'], ['\xa0Hong Kong[n 5]', '423,999', '407,107', '380,812\n'], ['\xa0South Africa', '410,338', '400,261', '377,782\n'], ['\xa0Romania', '403,395', '382,768', '350,776\n'], ['\xa0Pakistan', '—', '373,072', '299,864\n'], ['\xa0Czech Republic', '360,244', '345,037', '343,208\n'], ['\xa0Egypt', '347,342', '389,050', '331,590\n'], ['\xa0Chile', '343,823', '330,267', '335,533\n'], ['\xa0Iran', '341,013', '436,906', '401,596\n'], ['\xa0Portugal', '321,440', '308,683', '289,114\n'], ['\xa0Finland', '303,945', '299,836', '295,532\n'], ['\xa0Peru', '303,293', '289,222', '267,603\n'], ['\xa0Kazakhstan', '300,538', '288,406', '264,293\n'], ['\xa0Algeria', '268,885', '263,620', '247,626\n'], ['\xa0Greece', '267,348', '257,145', '243,498\n'], ['\xa0Iraq', '258,020', '279,641', '232,107\n'], ['\xa0New Zealand', '248,666', '260,236', '252,176\n'], ['\xa0Hungary', '237,070', '222,905', '212,657\n'], ['\xa0Qatar', '222,776', '217,983', '213,003\n'], ['\xa0Ukraine[n 6]', '205,742', '190,741', '178,757\n'], ['\xa0Cuba', '—', '—', '201,986\n'], ['\xa0Nigeria', '188,271', '187,760', '374,984\n'], ['\xa0Morocco', '165,835', '154,431', '144,438\n'], ['\xa0Kuwait', '153,101', '160,227', '163,705\n'], ['\xa0Slovakia', '147,031', '141,776', '132,908\n'], ['\xa0Uzbekistan', '132,484', '114,965', '90,889\n'], ['\xa0Kenya', '131,673', '124,499', '108,039\n'], ['\xa0Dominican Republic', '127,828', '124,282', '121,444\n'], ['\xa0Ecuador', '125,677', '124,676', '118,845\n'], ['\xa0Puerto Rico', '122,522', '125,842', '117,902\n'], ['\xa0Guatemala', '121,177', '113,200', '104,451\n'], ['\xa0Ethiopia', '117,457', '163,698', '159,746\n'], ['\xa0Bulgaria', '117,007', '112,212', '102,397\n'], ['\xa0Angola', '113,343', '80,397', '90,552\n'], ['\xa0Venezuela', '108,511', '—', '139,395\n'], ['\xa0Oman', '104,351', '106,943', '105,896\n'], ['\xa0Costa Rica', '102,591', '95,350', '86,498\n'], ['\xa0Sri Lanka', '—', '98,963', '84,364\n'], ['\xa0Croatia', '98,951', '92,526', '84,391\n'], ['\xa0Luxembourg', '96,613', '93,197', '85,755\n'], ['\xa0Ivory Coast', '94,483', '86,538', '78,789\n'], ['\xa0Serbia', '92,549', '89,084', '75,187\n'], ['\xa0Panama', '91,675', '86,260', '83,382\n'], ['\xa0Lithuania', '89,192', '84,869', '79,790\n'], ['\xa0Turkmenistan', '89,054', '64,240', '59,887\n'], ['\xa0Ghana', '88,332', '82,825', '76,370\n'], ['\xa0Tanzania[n 7]', '85,977', '78,780', '78,353\n'], ['\xa0Uruguay', '79,731', '80,962', '77,236\n'], ['\xa0DR Congo', '79,119', '70,749', '69,338\n'], ['\xa0Azerbaijan', '78,870', '74,316', '72,356\n'], ['\xa0Slovenia', '75,224', '72,485', '69,148\n'], ['\xa0Belarus', '71,561', '75,962', '71,857\n'], ['\xa0Myanmar', '64,944', '74,080', '62,084\n'], ['\xa0Uganda', '64,277', '53,652', '51,023\n'], ['\xa0Bolivia', '56,339', '49,668', '45,135\n'], ['\xa0Tunisia', '56,291', '53,410', '48,541\n'], ['\xa0Jordan', '56,102', '53,352', '50,814\n'], ['\xa0Cameroon', '56,011', '51,327', '49,279\n'], ['\xa0Macau[n 8]', '53,354', '50,183', '45,803\n'], ['\xa0Cambodia', '49,799', '46,353', '42,824\n'], ['\xa0Bahrain', '47,829', '47,737', '44,662\n'], ['\xa0Libya', '47,484', '46,636', '44,030\n'], ['\xa0Nepal', '46,080', '42,914', '40,484\n'], ['\xa0Latvia', '45,535', '43,521', '42,248\n'], ['\xa0Paraguay', '45,465', '44,458', '42,955\n'], ['\xa0Estonia', '45,004', '42,765', '41,291\n'], ['\xa0Cyprus[n 9]', '38,736', '36,333', '33,887\n'], ['\xa0Zimbabwe', '38,172', '44,188', '30,368\n'], ['\xa0Honduras', '38,172', '37,094', '34,401\n'], ['\xa0El Salvador', '36,749', '35,365', '34,016\n'], ['\xa0Georgia[n 10]', '35,353', '33,776', '30,536\n'], ['\xa0Iceland', '35,309', '33,463', '31,325\n'], ['\xa0Senegal', '34,728', '32,267', '30,408\n'], ['\xa0Haiti', '33,548', '25,224', '19,841\n'], ['\xa0Papua New Guinea', '32,835', '32,538', '31,020\n'], ['\xa0Sudan', '31,506', '49,910', '34,021\n'], ['\xa0Guinea', '30,094', '25,334', '23,006\n'], ['\xa0Zambia', '28,910', '26,326', '27,578\n'], ['\xa0Bosnia and Herzegovina', '28,807', '28,343', '27,515\n'], ['\xa0Albania', '28,372', '27,178', '22,978\n'], ['\xa0Burkina Faso', '27,056', '23,250', '20,325\n'], ['\xa0Trinidad and Tobago', '26,467', '26,429', '25,489\n'], ['\xa0Armenia', '26,258', '25,787', '24,219\n'], ['\xa0Guyana', '25,822', '24,836', '16,683\n'], ['\xa0Mongolia', '25,804', '23,586', '19,872\n'], ['\xa0Malta', '25,750', '24,322', '22,212\n'], ['\xa0Mozambique', '23,771', '22,417', '20,954\n'], ['\xa0Mali', '23,208', '26,588', '20,662\n'], ['\xa0Benin', '22,236', '21,483', '19,673\n'], ['\xa0Niger', '21,874', '19,538', '16,819\n'], ['\xa0Jamaica', '21,411', '19,930', '19,423\n'], ['\xa0Nicaragua', '21,155', '19,694', '17,829\n'], ['\xa0Gabon', '20,391', '20,867', '20,056\n'], ['\xa0Lebanon', '—', '20,079', '27,464\n'], ['\xa0Syria', '—', '19,993', '29,028\n'], ['\xa0Kyrgyzstan', '19,849', '17,478', '13,988\n'], ['\xa0Moldova[n 11]', '19,462', '18,200', '16,542\n'], ['\xa0Botswana', '19,400', '19,401', '19,396\n'], ['\xa0Chad', '18,792', '20,626', '17,643\n'], ['\xa0Madagascar', '18,708', '17,421', '15,870\n'], ['\xa0North Macedonia', '17,885', '16,685', '15,764\n'], ['\xa0Yemen', '17,401', '—', '8,758\n'], ['\xa0Afghanistan', '—', '17,152', '16,417\n'], ['\xa0North Korea', '—', '—', '16,447\n'], ['\xa0Laos', '16,322', '16,503', '15,008\n'], ['\xa0Brunei', '16,007', '15,463', '15,128\n'], ['\xa0Mauritius', '15,495', '14,953', '14,397\n'], ['\xa0Congo', '15,281', '15,720', '14,167\n'], ['\xa0Bahamas', '15,178', '15,833', '14,338\n'], ['\xa0Tajikistan', '14,836', '14,205', '12,061\n'], ['\xa0Rwanda', '14,771', '14,252', '14,097\n'], ['\xa0Namibia', '14,214', '13,372', '12,351\n'], ['\xa0Malawi', '13,959', '11,009', '12,627\n'], ['\xa0Palestine[n 12][n 13]', '—', '13,711', '17,396\n'], ['\xa0Somalia', '12,994', '12,109', '10,969\n'], ['\xa0Equatorial Guinea', '12,684', '12,766', '12,117\n'], ['\xa0Channel Islands', '—', '12,508', '—\n'], ['\xa0Mauritania', '11,470', '10,767', '10,652\n'], ['\xa0Kosovo', '11,274', '11,149', '10,467\n'], ['\xa0New Caledonia', '—', '9,623', '10,140\n'], ['\xa0Togo', '10,023', '9,926', '9,278\n'], ['\xa0Monaco', '—', '9,995', '9,995\n'], ['\xa0Bermuda', '—', '8,980', '8,142\n'], ['\xa0Montenegro', '8,562', '8,070', '7,530\n'], ['\xa0Sierra Leone', '8,386', '7,548', '6,412\n'], ['\xa0Liechtenstein', '—', '8,288', '7,965\n'], ['\xa0Barbados', '7,552', '7,165', '6,721\n'], ['\xa0Maldives', '7,480', '6,975', '6,591\n'], ['\xa0Isle of Man', '—', '7,431', '—\n'], ['\xa0Cayman Islands', '—', '7,139', '7,127\n'], ['\xa0Guam', '—', '6,910', '—\n'], ['\xa0Burundi', '6,745', '2,162', '3,960\n'], ['\xa0French Polynesia', '—', '6,402', '6,398\n'], ['\xa0Fiji', '6,257', '5,841', '5,442\n'], ['\xa0Eswatini', '5,483', '4,892', '4,574\n'], ['\xa0Liberia', '5,166', '4,750', '5,039\n'], ['\xa0U.S. Virgin Islands', '—', '4,672', '—\n'], ['\xa0Djibouti', '4,587', '4,086', '4,000\n'], ['\xa0Suriname', '4,506', '4,714', '3,759\n'], ['\xa0Aruba', '4,100', '3,649', '3,649\n'], ['\xa0Andorra', '4,035', '4,040', '3,785\n'], ['\xa0South Sudan', '3,998', '—', '4,629\n'], ['\xa0Faroe Islands', '—', '3,907', '—\n'], ['\xa0Belize', '3,611', '3,516', '3,079\n'], ['\xa0Bhutan', '3,422', '3,019', '3,019\n'], ['\xa0Greenland', '—', '3,327', '3,041\n'], ['\xa0Curaçao', '—', '3,281', '3,281\n'], ['\xa0Central African Republic', '2,932', '2,752', '2,555\n'], ['\xa0Cape Verde', '2,786', '2,768', '2,591\n'], ['\xa0Gambia', '2,771', '2,508', '2,401\n'], ['\xa0Saint Lucia', '2,632', '2,549', '2,430\n'], ['\xa0Zanzibar', '—', '—', '2,483\n'], ['\xa0Lesotho', '2,404', '2,272', '2,118\n'], ['\xa0Antigua and Barbuda', '2,373', '2,225', '2,033\n'], ['\xa0Eritrea', '—', '—', '2,275\n'], ['\xa0Guinea-Bissau', '2,274', '2,120', '1,841\n'], ['\xa0Seychelles', '2,198', '2,167', '2,141\n'], ['\xa0Timor-Leste', '2,115', '1,881', '2,080\n'], ['\xa0San Marino', '2,047', '1,832', '1,987\n'], ['\xa0Solomon Islands', '1,898', '1,761', '1,633\n'], ['\xa0Turks and Caicos Islands', '—', '1,745', '1,402\n'], ['\xa0Sint Maarten', '—', '1,735', '1,677\n'], ['\xa0Comoros', '1,548', '1,546', '1,448\n'], ['\xa0British Virgin Islands', '—', '—', '1,506\n'], ['\xa0Grenada', '1,464', '1,391', '1,317\n'], ['\xa0Vanuatu', '1,267', '1,161', '1,119\n'], ['\xa0Saint Vincent and the Grenadines', '1,242', '1,157', '1,091\n'], ['\xa0Northern Mariana Islands', '—', '1,096', '—\n'], ['\xa0Samoa', '1,160', '1,068', '1,032\n'], ['\xa0Saint Kitts and Nevis', '1,129', '1,067', '1,058\n'], ['\xa0American Samoa', '—', '871', '—\n'], ['\xa0São Tomé and Príncipe', '864', '764', '684\n'], ['\xa0Dominica', '742', '689', '651\n'], ['\xa0Saint Martin', '—', '649', '—\n'], ['\xa0Tonga', '568', '509', '508\n'], ['\xa0Micronesia', '500', '471', '460\n'], ['\xa0Anguilla', '—', '—', '416\n'], ['\xa0Cook Islands', '—', '—', '366\n'], ['\xa0Palau', '333', '282', '282\n'], ['\xa0Kiribati', '312', '308', '289\n'], ['\xa0Marshall Islands', '297', '280', '270\n'], ['\xa0Nauru', '169', '160', '176\n'], ['\xa0Montserrat', '—', '—', '80\n'], ['\xa0Tuvalu', '65', '62', '68\n']]
    #print(gdp_data_list)
    
    transformed_gdp_data = transform(gdp_data_list)
    
    #for i in transformed_gdp_data:
    #    print(i)
    #transformed_gdp_data = [['Country/Territory', 'IMF_2025', 'World_Bank_22_24', 'United_Nations_23'], ['World', 113795.678, 111326.37, 100834.796], ['United States', 30507.217, 29184.89, 27720.7], ['China[n 1][n 3]', 19231.705, 18743.803, 17794.782], ['Germany', 4744.804, 4659.929, 4525.704], ['India', 4187.017, 3912.686, 3575.778], ['Japan', 4186.431, 4026.211, 4204.495], ['United Kingdom', 3839.18, 3643.834, 3380.855], ['France', 3211.292, 3162.079, 3051.832], ['Italy', 2422.855, 2372.775, 2300.941], ['Canada', 2225.341, 2241.253, 2142.471], ['Brazil', 2125.958, 2179.412, 2191.132], ['Russia', 2076.396, 2173.836, 2008.419], ['Spain', 1799.511, 1722.746, 1620.091], ['South Korea', 1790.322, 1712.793, 1839.058], ['Australia', 1771.681, 1752.193, 1775.628], ['Mexico', 1692.64, 1852.723, 1793.799], ['Turkey', 1437.406, 1323.255, 1118.253], ['Indonesia', 1429.743, 1396.3, 1371.171], ['Netherlands', 1272.011, 1227.544, 1154.361], ['Saudi Arabia', 1083.749, 1237.53, 1067.583], ['Poland', 979.96, 914.696, 809.201], ['Switzerland', 947.125, 936.564, 894.425], ['Taiwan[n 4]', 804.889, 0, 0], ['Belgium', 684.864, 664.564, 644.783], ['Argentina', 683.533, 633.267, 646.075], ['Sweden', 620.297, 610.118, 584.96], ['Ireland', 598.84, 577.389, 551.395], ['Israel', 583.361, 540.38, 513.611], ['Singapore', 564.774, 547.387, 501.428], ['United Arab Emirates', 548.598, 537.079, 514.13], ['Thailand', 546.224, 526.411, 514.945], ['Austria', 534.301, 521.642, 511.685], ['Norway', 504.276, 483.727, 485.311], ['Philippines', 497.495, 461.618, 437.146], ['Vietnam', 490.97, 476.388, 429.717], ['Bangladesh', 467.218, 450.119, 422.431], ['Denmark', 449.94, 429.457, 407.092], ['Malaysia', 444.984, 421.972, 399.649], ['Colombia', 427.766, 418.542, 363.54], ['Hong Kong[n 5]', 423.999, 407.107, 380.812], ['South Africa', 410.338, 400.261, 377.782], ['Romania', 403.395, 382.768, 350.776], ['Pakistan', 0, 373.072, 299.864], ['Czech Republic', 360.244, 345.037, 343.208], ['Egypt', 347.342, 389.05, 331.59], ['Chile', 343.823, 330.267, 335.533], ['Iran', 341.013, 436.906, 401.596], ['Portugal', 321.44, 308.683, 289.114], ['Finland', 303.945, 299.836, 295.532], ['Peru', 303.293, 289.222, 267.603], ['Kazakhstan', 300.538, 288.406, 264.293], ['Algeria', 268.885, 263.62, 247.626], ['Greece', 267.348, 257.145, 243.498], ['Iraq', 258.02, 279.641, 232.107], ['New Zealand', 248.666, 260.236, 252.176], ['Hungary', 237.07, 222.905, 212.657], ['Qatar', 222.776, 217.983, 213.003], ['Ukraine[n 6]', 205.742, 190.741, 178.757], ['Cuba', 0, 0, 201.986], ['Nigeria', 188.271, 187.76, 374.984], ['Morocco', 165.835, 154.431, 144.438], ['Kuwait', 153.101, 160.227, 163.705], ['Slovakia', 147.031, 141.776, 132.908], ['Uzbekistan', 132.484, 114.965, 90.889], ['Kenya', 131.673, 124.499, 108.039], ['Dominican Republic', 127.828, 124.282, 121.444], ['Ecuador', 125.677, 124.676, 118.845], ['Puerto Rico', 122.522, 125.842, 117.902], ['Guatemala', 121.177, 113.2, 104.451], ['Ethiopia', 117.457, 163.698, 159.746], ['Bulgaria', 117.007, 112.212, 102.397], ['Angola', 113.343, 80.397, 90.552], ['Venezuela', 108.511, 0, 139.395], ['Oman', 104.351, 106.943, 105.896], ['Costa Rica', 102.591, 95.35, 86.498], ['Sri Lanka', 0, 98.963, 84.364], ['Croatia', 98.951, 92.526, 84.391], ['Luxembourg', 96.613, 93.197, 85.755], ['Ivory Coast', 94.483, 86.538, 78.789], ['Serbia', 92.549, 89.084, 75.187], ['Panama', 91.675, 86.26, 83.382], ['Lithuania', 89.192, 84.869, 79.79], ['Turkmenistan', 89.054, 64.24, 59.887], ['Ghana', 88.332, 82.825, 76.37], ['Tanzania[n 7]', 85.977, 78.78, 78.353], ['Uruguay', 79.731, 80.962, 77.236], ['DR Congo', 79.119, 70.749, 69.338], ['Azerbaijan', 78.87, 74.316, 72.356], ['Slovenia', 75.224, 72.485, 69.148], ['Belarus', 71.561, 75.962, 71.857], ['Myanmar', 64.944, 74.08, 62.084], ['Uganda', 64.277, 53.652, 51.023], ['Bolivia', 56.339, 49.668, 45.135], ['Tunisia', 56.291, 53.41, 48.541], ['Jordan', 56.102, 53.352, 50.814], ['Cameroon', 56.011, 51.327, 49.279], ['Macau[n 8]', 53.354, 50.183, 45.803], ['Cambodia', 49.799, 46.353, 42.824], ['Bahrain', 47.829, 47.737, 44.662], ['Libya', 47.484, 46.636, 44.03], ['Nepal', 46.08, 42.914, 40.484], ['Latvia', 45.535, 43.521, 42.248], ['Paraguay', 45.465, 44.458, 42.955], ['Estonia', 45.004, 42.765, 41.291], ['Cyprus[n 9]', 38.736, 36.333, 33.887], ['Zimbabwe', 38.172, 44.188, 30.368], ['Honduras', 38.172, 37.094, 34.401], ['El Salvador', 36.749, 35.365, 34.016], ['Georgia[n 10]', 35.353, 33.776, 30.536], ['Iceland', 35.309, 33.463, 31.325], ['Senegal', 34.728, 32.267, 30.408], ['Haiti', 33.548, 25.224, 19.841], ['Papua New Guinea', 32.835, 32.538, 31.02], ['Sudan', 31.506, 49.91, 34.021], ['Guinea', 30.094, 25.334, 23.006], ['Zambia', 28.91, 26.326, 27.578], ['Bosnia and Herzegovina', 28.807, 28.343, 27.515], ['Albania', 28.372, 27.178, 22.978], ['Burkina Faso', 27.056, 23.25, 20.325], ['Trinidad and Tobago', 26.467, 26.429, 25.489], ['Armenia', 26.258, 25.787, 24.219], ['Guyana', 25.822, 24.836, 16.683], ['Mongolia', 25.804, 23.586, 19.872], ['Malta', 25.75, 24.322, 22.212], ['Mozambique', 23.771, 22.417, 20.954], ['Mali', 23.208, 26.588, 20.662], ['Benin', 22.236, 21.483, 19.673], ['Niger', 21.874, 19.538, 16.819], ['Jamaica', 21.411, 19.93, 19.423], ['Nicaragua', 21.155, 19.694, 17.829], ['Gabon', 20.391, 20.867, 20.056], ['Lebanon', 0, 20.079, 27.464], ['Syria', 0, 19.993, 29.028], ['Kyrgyzstan', 19.849, 17.478, 13.988], ['Moldova[n 11]', 19.462, 18.2, 16.542], ['Botswana', 19.4, 19.401, 19.396], ['Chad', 18.792, 20.626, 17.643], ['Madagascar', 18.708, 17.421, 15.87], ['North Macedonia', 17.885, 16.685, 15.764], ['Yemen', 17.401, 0, 8.758], ['Afghanistan', 0, 17.152, 16.417], ['North Korea', 0, 0, 16.447], ['Laos', 16.322, 16.503, 15.008], ['Brunei', 16.007, 15.463, 15.128], ['Mauritius', 15.495, 14.953, 14.397], ['Congo', 15.281, 15.72, 14.167], ['Bahamas', 15.178, 15.833, 14.338], ['Tajikistan', 14.836, 14.205, 12.061], ['Rwanda', 14.771, 14.252, 14.097], ['Namibia', 14.214, 13.372, 12.351], ['Malawi', 13.959, 11.009, 12.627], ['Palestine[n 12][n 13]', 0, 13.711, 17.396], ['Somalia', 12.994, 12.109, 10.969], ['Equatorial Guinea', 12.684, 12.766, 12.117], ['Channel Islands', 0, 12.508, 0], ['Mauritania', 11.47, 10.767, 10.652], ['Kosovo', 11.274, 11.149, 10.467], ['New Caledonia', 0, 9.623, 10.14], ['Togo', 10.023, 9.926, 9.278], ['Monaco', 0, 9.995, 9.995], ['Bermuda', 0, 8.98, 8.142], ['Montenegro', 8.562, 8.07, 7.53], ['Sierra Leone', 8.386, 7.548, 6.412], ['Liechtenstein', 0, 8.288, 7.965], ['Barbados', 7.552, 7.165, 6.721], ['Maldives', 7.48, 6.975, 6.591], ['Isle of Man', 0, 7.431, 0], ['Cayman Islands', 0, 7.139, 7.127], ['Guam', 0, 6.91, 0], ['Burundi', 6.745, 2.162, 3.96], ['French Polynesia', 0, 6.402, 6.398], ['Fiji', 6.257, 5.841, 5.442], ['Eswatini', 5.483, 4.892, 4.574], ['Liberia', 5.166, 4.75, 5.039], ['U.S. Virgin Islands', 0, 4.672, 0], ['Djibouti', 4.587, 4.086, 4.0], ['Suriname', 4.506, 4.714, 3.759], ['Aruba', 4.1, 3.649, 3.649], ['Andorra', 4.035, 4.04, 3.785], ['South Sudan', 3.998, 0, 4.629], ['Faroe Islands', 0, 3.907, 0], ['Belize', 3.611, 3.516, 3.079], ['Bhutan', 3.422, 3.019, 3.019], ['Greenland', 0, 3.327, 3.041], ['Curaçao', 0, 3.281, 3.281], ['Central African Republic', 2.932, 2.752, 2.555], ['Cape Verde', 2.786, 2.768, 2.591], ['Gambia', 2.771, 2.508, 2.401], ['Saint Lucia', 2.632, 2.549, 2.43], ['Zanzibar', 0, 0, 2.483], ['Lesotho', 2.404, 2.272, 2.118], ['Antigua and Barbuda', 2.373, 2.225, 2.033], ['Eritrea', 0, 0, 2.275], ['Guinea-Bissau', 2.274, 2.12, 1.841], ['Seychelles', 2.198, 2.167, 2.141], ['Timor-Leste', 2.115, 1.881, 2.08], ['San Marino', 2.047, 1.832, 1.987], ['Solomon Islands', 1.898, 1.761, 1.633], ['Turks and Caicos Islands', 0, 1.745, 1.402], ['Sint Maarten', 0, 1.735, 1.677], ['Comoros', 1.548, 1.546, 1.448], ['British Virgin Islands', 0, 0, 1.506], ['Grenada', 1.464, 1.391, 1.317], ['Vanuatu', 1.267, 1.161, 1.119], ['Saint Vincent and the Grenadines', 1.242, 1.157, 1.091], ['Northern Mariana Islands', 0, 1.096, 0], ['Samoa', 1.16, 1.068, 1.032], ['Saint Kitts and Nevis', 1.129, 1.067, 1.058], ['American Samoa', 0, 0.871, 0], ['São Tomé and Príncipe', 0.864, 0.764, 0.684], ['Dominica', 0.742, 0.689, 0.651], ['Saint Martin', 0, 0.649, 0], ['Tonga', 0.568, 0.509, 0.508], ['Micronesia', 0.5, 0.471, 0.46], ['Anguilla', 0, 0, 0.416], ['Cook Islands', 0, 0, 0.366], ['Palau', 0.333, 0.282, 0.282], ['Kiribati', 0.312, 0.308, 0.289], ['Marshall Islands', 0.297, 0.28, 0.27], ['Nauru', 0.169, 0.16, 0.176], ['Montserrat', 0, 0, 0.08], ['Tuvalu', 0.065, 0.062, 0.068]]   
    
    files_dict = loading(transformed_gdp_data)

    # files_dict = {'csv_file_name': 'gdp_data_2025_csv.csv', 'db_name': 'gdp_data_2025_db.db', 'table_name': 'gdp_data_2025_table'}
    table_name = files_dict['table_name']
    db_name = files_dict['db_name']
    query = 'select * from ' + table_name
    #print(query, db_name, table_name)
    
    querying = run_query(query, db_name, table_name)

    print("*****************************")
    

    
if __name__ == '__main__':
    main()
