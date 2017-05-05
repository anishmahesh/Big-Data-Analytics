import csv
"""
Agency,Complaint Type,Location Type,Incident Zip,Facility Type,Status,Latitude,Longitude
"""
__author__ = "Sanchit Mehta"

def zerolistmaker(n):
    listofzeros = [0] * n
    return listofzeros

def process_value(val):
    import re
    val = re.sub('\s+','_',val).lower()
    return val

csvReader = csv.reader(open('filtered.csv', "rt"))
csvWriter = csv.writer(open('featurized_filtered.csv','w'))

col_headers = {'agency':1,'complaint_type':2,'location_type':3,'facility_type':5}
col_unique_vals = {}
cols_not_to_include = {'incident_zip':0,'latitude':4,'longitude':6}

for col_name,_ in col_headers.iteritems():
    col_unique_vals[col_name] = set()

for row in csvReader:
    for col_name,index in col_headers.iteritems():
        if len(row)>5:
	    col_unique_vals[col_name].add(process_value(row[index]))

print(col_headers)



#combine all the values into different column of a row
#add to dictionary with their index in list as location
lst = []
key_idx = 0
key_dict = {}

for k,v in col_unique_vals.iteritems():
    for unique_col in v:
        key_dict[k+"_"+unique_col] = key_idx
        lst.append(k+"_"+unique_col)
        key_idx+=1

for k,_ in cols_not_to_include.iteritems():
    key_dict[k] = key_idx
    key_idx+=1
    lst.append(k)

print(len(key_dict))
csvWriter.writerow(lst)
lst = []
csvReader2 = csv.reader(open('filtered.csv', "rt"))
for row in csvReader2:
    lst2 = zerolistmaker(290)
    if len(row)>5:
        for col_name,index in col_headers.iteritems():
            lst2[key_dict[col_name+"_"+process_value(row[index])]] = 1
        for col_name,v in cols_not_to_include.iteritems():
            lst2[key_dict[col_name]] = row[v] 
    #print(lst2)
    csvWriter.writerow(lst2)
