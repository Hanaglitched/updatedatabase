from datetime import datetime

import numpy as np
import pandas as pd
import json
import pymysql

start_time = datetime.now();
from Tools.scripts.dutree import display

# sql insert tutorial
# https://www.dataquest.io/blog/sql-insert-tutorial/
# ----------------------------------------------
# (1) database introduce area
# make connection to database
connection = pymysql.connect(host='some',
                             port=3306,
                             user='some',
                             password='so',
                             db='some',
                             charset='utf8')
# create cursor
cursor = connection.cursor()
# -----------------------------------------------

# -----------------------------------------------
# (2) dataframe introduce area
# open json file
with open('D:/my projects/jsonintodb/venv/data/json/AllPrices.json', 'r', encoding='utf-8') as file:
    getImport = json.load(file)

# get original dataframe
df = getImport.get("data")
uuid = pd.DataFrame(df);
uuid = uuid.transpose();

# uuid insertion

# Insert DataFrame by once

print(len(uuid.index))
val = ""
for j in range(0, len(uuid.index)):
    val = ""
    print(j)
    try:
        val = "(\"" + uuid.index[j] + "\")"
        sql_ = "INSERT INTO MagicnotifyUuidName (`key`) VALUES " + val + ";"
        cursor.execute(sql_)
    except:
        pass
connection.commit()
print("uuid insertion done")

uuid = uuid.replace({np.nan: None})

# push datas into database, by numbers
print("b")
for i in range(0, len(uuid.index)):
    # check the progress; you could make it progress bar to get it visualize.
    print(i)
    val = ""
    saveKey = ""
    key = uuid.index[i]
    if uuid.iloc[i].get("paper") != None:
        if uuid.iloc[i].get("paper").get("cardkingdom") != None:
            if uuid.iloc[i].get("paper").get("cardkingdom").get("retail") != None:
                tmp = uuid.iloc[i].get("paper").get("cardkingdom").get("retail")
                length_normal = 0
                length_foil = 0
                length = 0
                normalExistFlag = False
                foilExistFlag = False
                if tmp.get("normal") != None:
                    getNormalDict = tmp.get("normal")
                    length_normal = len(tmp.get("normal"))
                    normalExistFlag = True
                if tmp.get("foil") != None:
                    getFoilDict = tmp.get("foil")
                    length_foil = len(tmp.get("foil"))
                    foilExistFlag = True
                if length < length_normal:
                    length = length_normal
                if length < length_foil:
                    length = length_foil
                for j in range(0, 1):
                    if normalExistFlag and j < length_normal:
                        date = list(getNormalDict)[j]
                        normal = str(getNormalDict[date])
                    else:
                        normal = ""
                    if foilExistFlag and j < length_foil:
                        date = list(getFoilDict)[j]
                        foil = str(getFoilDict[date])
                    else:
                        foil = ""
                    saveKey = key;
                    val += "(NULLIF(\"" + foil + "\",\'\') , NULLIF(\"" + normal + "\",\'\') , \"" + date + "\",\"" + key + "\")"
                    if j < length - 1:
                        val += ","
    # go update
    if val != "":
        sql = "INSERT INTO MagicnotifyPrice (`foil`, `normal`, `date`, `key`) VALUES " + val + ";"
        cursor.execute(sql)
        sql = "DELETE FROM MagicnotifyPrice where `key`= " + saveKey + " order by `date` asc limit 1;"
        cursor.execute(sql)

connection.commit()
# -----------------------------------------------

# -----------------------------------------------
# (4) get elapsed time (for test)
end_time = datetime.now()
elapsed_time = end_time - start_time
print(elapsed_time)
# -----------------------------------------------
