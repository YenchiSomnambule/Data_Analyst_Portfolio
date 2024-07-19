import mysql.connector, csv, datetime

cnx = mysql.connector.connect(
  host="localhost",
  database="cerealcrop",
  user="root",
  password="Yen40solo!"
)

cursor = cnx.cursor()
counter = 0
current_time = datetime.datetime.now()
print(current_time)

#Create Tables
cursor = cnx.cursor()


cursor.execute('''
        CREATE TABLE Crop(
               Entity varchar(50),
               Year varchar(50),
               barley_attainable varchar(50),
               rye_attainable varchar(50),
               wheat_attainable varchar(50),
               wheat_yield_gap varchar(50),
               barley_yield_gap varchar(50),
               rye_yield_gap varchar(50),
               BarleyActualYield varchar(50),
               RyeActualYield varchar(50),
               WheatActualYield varchar(50),
               IncompleteWarning varchar(50),
               Decade varchar(50),
               InsertedBy varchar(50),
               InsertedDate datetime
        )
               ''')

cnx.commit()

print('Crop Table Created')

cursor.execute('''
        CREATE TABLE Fertilizer(
               Entity varchar(50),
               Code varchar(50),
               Year varchar(50),
               FertilizerConsumption varchar(50),
               Decade varchar(50),
               IncompleteWarning varchar(50),
               InsertedBy varchar(50),
               InsertedDate datetime
        )
               ''')

cnx.commit()

print('Fertilizer Table Created')

cursor.execute('''
        CREATE TABLE Pesticide(
               Entity varchar(50),
               Code varchar(50),
               Year varchar(50),
               PesticideConsumption varchar(50),
               Decade varchar(50),
               IncompleteWarning varchar(50),
               InsertedBy varchar(50),
               InsertedDate datetime
        )
               ''')

cnx.commit()

print('Pesticide Table Created')

cursor.execute('''
        CREATE TABLE CerealCropYieldData(
               CountryCode varchar(50),
               CountryDescription varchar(50),
               Decade varchar(50),
               IncompleteWarning varchar(50),
               BarleyActualYield varchar(50),
               RyeActualYield varchar(50),
               WheatActualYield varchar(50),
               FertilizerKgHa varchar(50),
               PesticideKgHa varchar(50),
               InsertedBy varchar(50),
               InsertedDate datetime
        )
               ''')
cnx.commit()

print('CerealCropYieldDate Table Created')



def decade(year):
    if 1960 <= year < 1970:
        return(60)
    elif 1970 <= year < 1980:
        return(70)
    elif 1980 <= year < 1990:
        return(80)
    elif 1990 <= year < 2000:
        return(90)
    elif 2000 <= year < 2010:
        return(str(00))
    elif 2010 <= year < 2020:
        return(10)
    elif 2020 <= year < 2030:
        return(20)
    else:
        pass

def actualyield(attainable,gap):
    actual = attainable - gap
    return(actual)



# open cropyield file for reading
with open('cropyield.csv', newline ='') as csvDataFile:
    # read file as csv file 
    csvReader = csv.reader(csvDataFile)
    next(csvReader)

    # for every row, insert a row in the database table
    for row in csvReader:
        Entity = row[0]
        BarleyActualYield = 0
        RyeActualYield = 0
        WheatActualYield = 0
        Decade = 0

        if Entity in ['Canada', 'United States', 'Mexico']:
            if row[2] == '' or row[3] == '' or row[4] == '' or row[5] == '' or row[6] == '' or row [7] == '':
                IncompleteWarning = 'Y'
                if row[2] =='':
                    BarleyActualYield = None
                elif row[6] =='':
                    BarleyActualYield = None
                elif row[3] =='':
                    RyeActualYield = None
                elif row[7] == '':
                    RyeActualYield = None
                elif row[4] == '':
                    WheatActualYield = None
                elif row[5] == '':
                    WheatActualYield = None
            else:
                IncompleteWarning = 'N'
                BarleyActualYield = str(actualyield(float(row[2]),float(row[6])))
                RyeActualYield = str(actualyield(float(row[3]),float(row[7])))
                WheatActualYield = str(actualyield(float(row[4]),float(row[5])))

            Decade = decade(int(row[1]))

            sql_query = '''
                INSERT INTO crop (Entity, Year, barley_attainable, rye_attainable, 
                            wheat_attainable, wheat_yield_gap, barley_yield_gap, 
                            rye_yield_gap, BarleyActualYield, RyeActualYield, WheatActualYield, IncompleteWarning, Decade, InsertedBy, InsertedDate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            '''

            print("SQL Query:", sql_query)
            print("Row Values:", row)

            # Pass the values as a tuple to the execute method
            cursor.execute(sql_query, (Entity, row[1], row[2], row[3], row[4], row[5], row[6], row[7], BarleyActualYield, RyeActualYield, WheatActualYield, IncompleteWarning, Decade, 'Yenchi Wang', current_time))
            counter += 1

cnx.commit()
print('Records Processed = ', counter)

counter = 0
# open fertilizer file for reading
with open('fertilizers.csv', newline ='') as csvDataFile:

    # read file as csv file 
    csvReader = csv.reader(csvDataFile)

    next(csvReader)

    # for every row, insert a row in the database table
    for row in csvReader:
        #row = [value if value != '' else float(0.0) for value in row]
        #row = [float(value) if value.replace('.', '', 1).isdigit() else float(0.0) for value in row]
        Entity = row[0]
        Decade = 0
        if Entity in ['Canada', 'United States', 'Mexico']:
          if row[3] == '':
              IncompleteWarning = 'Y'
          else:
              IncompleteWarning = 'N'
          Decade = decade(int(row[2]))
          sql_query ='''
              INSERT INTO fertilizer (Entity, Code, Year, FertilizerConsumption, Decade, IncompleteWarning,
                        InsertedBy, InsertedDate)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                      '''
          print("SQL Query:", sql_query)
          print("Row Values:", row)

          cursor.execute(sql_query,(Entity, row[1], row[2], row[3], Decade, IncompleteWarning,'Yenchi Wang', current_time))
          counter = counter + 1
cnx.commit()
print('Records Processed = ',(counter))

counter = 0
#open pesticide file for reading
with open('pesticide-use-per-hectare-of-cropland.csv', newline ='') as csvDataFile:

    # read file as csv file 
    csvReader = csv.reader(csvDataFile)

    next(csvReader)

    # for every row, insert a row in the database table
    for row in csvReader:
        Entity = row[0]
        Decade = 0
        if Entity in ['Canada', 'United States', 'Mexico']:
          if row[3] == '':
            IncompleteWarning = 'Y'
          else:
            IncompleteWarning = 'N'
        #row = [value if value != '' else float(0.0) for value in row]
        #row = [float(value) if value.replace('.', '', 1).isdigit() else float(0.0) for value in row]
          Decade = decade(int(row[2]))
          sql_query ='''
              INSERT INTO pesticide (Entity, Code, Year, PesticideConsumption, Decade, IncompleteWarning,
                        InsertedBy, InsertedDate)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                      '''
          print("SQL Query:", sql_query)
          print("Row Values:", row)

          cursor.execute(sql_query,(Entity, row[1], row[2], row[3], Decade, IncompleteWarning,'Yenchi Wang', current_time))
cnx.commit()



# Prepare the standard insert statement
sqlinsertstatement = "insert into CerealCropYieldData" 
"(CountryCode, CountryDescription, Decade, IncompleteWarning, "
"BarleyActualYield, RyeActualYield, WheatActualYield, "
"FertilizerKgHa, PesticideKgHa, InsertedBy, InsertedDate)"
"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "








# Turn off ONLY_FULL_GROUP_BY mode for the current session
cursor.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

# Your query here
cursor.execute('''
INSERT INTO CerealCropYieldData
(CountryCode, CountryDescription, Decade, IncompleteWarning,
BarleyActualYield, RyeActualYield, WheatActualYield,
FertilizerKgHa, PesticideKgHa, InsertedBy, InsertedDate)
SELECT
    fertilizer.Code,
    crop.Entity,
    crop.Decade,
    CASE WHEN crop.IncompleteWarning = 'Y' OR fertilizer.IncompleteWarning = 'Y' OR pesticide.IncompleteWarning = 'Y' THEN 'Y' ELSE 'N' END AS IncompleteWarning,
    CASE WHEN SUM(crop.BarleyActualYield) IS NULL THEN NULL ELSE SUM(crop.BarleyActualYield) END AS BarleyActualYield,
    CASE WHEN SUM(crop.RyeActualYield) IS NULL THEN NULL ELSE SUM(crop.RyeActualYield) END AS RyeActualYield,
    CASE WHEN SUM(crop.WheatActualYield) IS NULL THEN NULL ELSE SUM(crop.WheatActualYield) END AS WheatActualYield,
    CASE WHEN SUM(fertilizer.FertilizerConsumption) IS NULL THEN NULL ELSE SUM(fertilizer.FertilizerConsumption) END AS FertilizerKgHa,
    CASE WHEN SUM(pesticide.PesticideConsumption) IS NULL THEN NULL ELSE SUM(pesticide.PesticideConsumption) END AS PesticideKgHa,
    'Yenchi Wang' AS InsertedBy,
    CURRENT_TIME AS InsertedDate
FROM crop
LEFT JOIN fertilizer ON crop.Entity = fertilizer.Entity AND crop.Decade = fertilizer.Decade
LEFT JOIN pesticide ON crop.Entity = pesticide.Entity AND crop.Decade = pesticide.Decade
GROUP BY crop.Entity, crop.Decade;
'''
)

# Reset sql_mode to its original value
cursor.execute("SET SESSION sql_mode=(SELECT @@sql_mode);")

# Commit the changes
cnx.commit()


