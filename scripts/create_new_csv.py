import csv
import pickle

latitude_Idx = 21
longitude_Idx = 22

def defaultlistmaker(n, defualt_if_not_exist):
    listofzeros = [defualt_if_not_exist] * n
    return listofzeros

def getReadFile(readFileName):
    return csv.reader(open(readFileName, "rt"))

def getWriteFile(writeFileName):
    return csv.writer(open(writeFileName,'w'))


def isRowIndexEmpty(row, rowList):
    for r in rowList:
        if row[r] == '':
            return True
    return False

def meaningOfKey(key, meaning, key_meaning_dict):
    key_meaning_dict[key] = meaning

def createSetList(csvReader, rowList):
    feature_length = len(rowList)
    setList = [set() for _ in range(feature_length)]
    kycd_meanining_dict = {}
    pdcd_meaning_dict = {}
    #First row with headings will not be copied to set because it's already iterated in getColumnHeading
    for row in csvReader:
        # We won't use columns which don't have latitude or longitude or values for rows
        if not row[latitude_Idx] == '' and not row[longitude_Idx] == '' and ~isRowIndexEmpty(row, rowList):
            for i in range(feature_length):
                setList[i].add(row[rowList[i]])
                if rowList[i] == 6:
                    meaningOfKey(row[6], row[7], kycd_meanining_dict)
                elif rowList[i] == [8]:
                    meaningOfKey(row[8], row[9], pdcd_meaning_dict)
    pickle.dump(kycd_meanining_dict, open("../RTBDA/Unique_Values/kycdMeaning", "wb"))
    pickle.dump(pdcd_meaning_dict, open("../RTBDA/Unique_Values/pdcdMeaning", "wb"))
    pickle.dump(setList, open("../RTBDA/Unique_Values/setList.p", "wb"))
    return setList

#Use the return value if you also want to add column heading
#else just let the first row go if it's not useful but don't comment out the method
def getColumnHeading(csvReader, rowList):
    col_heading = []
    first_row = next(csvReader)
    for r in rowList:
        col_heading.append(first_row[r])
    return col_heading


def createColDictAndHeader(setList,rowList):
    col_val_dict = {}
    list = []
    col_val_idx = 0
    for i in range(len(setList)):
        for val in setList[i]:
            if not val == '':
                key = val + str(i)
                col_val_dict[key] = col_val_idx
                col_val_idx += 1
                list.append(val)
    return col_val_dict, list


def addLatLong(col_val_dict, list):
    col_val_idx = len(col_val_dict)
    val = 'Latitude'
    col_val_dict[val] = col_val_idx
    col_val_idx += 1
    list.append(val)
    val = 'Longitude'
    col_val_dict[val] = col_val_idx
    col_val_idx += 1
    list.append(val)
    return col_val_dict, list



def addPresenceAbsenceVal(csvReader2, csvWriter, key_dict, rowList, default_if_exist=1, defualt_if_not_exist=0):
    for row in csvReader2:
        list = defaultlistmaker(len(key_dict), defualt_if_not_exist)
        if not row[latitude_Idx] == '' and not row[longitude_Idx] == '' and ~isRowIndexEmpty(row, rowList):
            for r in rowList:
                if row[r] in key_dict:
                    list[key_dict[row[r]]] = default_if_exist
            list[key_dict['Latitude']] = row[latitude_Idx]
            list[key_dict['Longitude']] = row[longitude_Idx]
            csvWriter.writerow(list)


def create_new_csv(readFileName, writeFileName,rowList):
    csvReader = getReadFile(readFileName)
    csvWriter = getWriteFile(writeFileName)
    col_heading = getColumnHeading(csvReader,rowList)
    setList = createSetList(csvReader, rowList)
    print(setList)
    #key_dict, list = createColDictAndHeader(setList,rowList)
    #key_dict, list = addLatLong(key_dict, list)
    #csvWriter.writerow(list)
    #csvReader2 = getReadFile(readFileName)
    #col_heading_sec = getColumnHeading(csvReader,rowList)
    #addPresenceAbsenceVal(csvReader2, csvWriter, key_dict, rowList)

def main():
    readFileName = '../RTBDA/NYPD_Complaint_Data_Historic.csv'
    writeFileName = '../RTBDA/Unique_Values/VERBOSE_NYPD_Complaint_Data_Historic.csv'
    rowList = [6,8,10,11]
    create_new_csv(readFileName, writeFileName, rowList)

if __name__ == '__main__':
    main()












'''
#Column with multiple values as set
kycd = set()
pdcd = set()
tried = set()
law_cat = set()
for row in csvReader:
    if not row[21] == '' and not row[22] == '':
        kycd.add(row[7])
        pdcd.add(row[9])
        tried.add(row[10])
        law_cat.add(row[11])

#combine all the values into different column of a row
#add to dictionary with their index in list as location
list = []
key_idx = 0
key_dict = {}
for val in kycd:
    if not val == '' and not val == 'KY_CD':
        key_dict[val+'ky'] = key_idx
        key_idx +=1
        list.append(val)


for val in pdcd:
    if not val == '' and not val == 'PD_CD' :
        key_dict[val] = key_idx
        key_idx += 1
        list.append(val)

for val in tried:
    if not val == '' and not val == 'CRM_ATPT_CPTD_CD':
        key_dict[val] = key_idx
        key_idx += 1
        list.append(val)

for val in law_cat:
    if not val == '' and not val == 'LAW_CAT_CD':
        key_dict[val] = key_idx
        key_idx += 1
        list.append(val)

val = 'Latitude'
key_dict[val] = key_idx
key_idx += 1
list.append(val)

val = 'Longitude'
key_dict[val] = key_idx
key_idx += 1
list.append(val)

csvWriter.writerow(list)


csvReader2 = csv.reader(open('../RTBDA/NYPD_Complaint_Data_Historic.csv', "rt"))
for row in csvReader2:
    list = zerolistmaker(key_idx)
    if not row[21] =='' and not row[22] =='' and not row[7] =='' and not row[7] == 'OFNS_DESC' and not row[9] =='' and not row[9] == 'PD_DESC' and not row[10] =='' and not row[10] == 'CRM_ATPT_CPTD_CD' and not row[11] =='' and not row[11] == 'LAW_CAT_CD':
        if row[6] in key_dict:
            list[key_dict[row[6]]] = 1
        if row[8] in key_dict:
            list[key_dict[row[8]]] = 1
        if row[10] in key_dict:
            list[key_dict[row[10]]] = 1
        if row[11] in key_dict:
            list[key_dict[row[11]]] = 1
        list[key_dict['Latitude']] = row[21]
        list[key_dict['Longitude']] = row[22]
        csvWriter.writerow(list)

'''