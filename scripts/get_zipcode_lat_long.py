import csv
import requests as req
import time

lat_idx = 21
long_idx = 22
unique_idx = 0

def getReadFile(readFileName):
    return csv.reader(open(readFileName, "rt"))

def getWriteFile(writeFileName):
    return csv.writer(open(writeFileName,'w'))


def getZipCode(json_data):
    address_component = json_data["results"][0]["address_components"]
    for ac in address_component:
        types = ac["types"]
        if types[0] == "postal_code":
            return ac["long_name"]

#Use the return value if you also want to add column heading
#else just let the first row go if it's not useful but don't comment out the method
def jumpFirstRow(csvReader):
    first_row = next(csvReader)


def setColHeader(csv_writer):
    list = []
    list.append("Key")
    list.append("Latitude")
    list.append("Longitude")
    list.append("ZipCode")
    csv_writer.writerow(list)


def makeGetCall(read_file_name, write_file_name):
    csv_reader = getReadFile(read_file_name)
    csv_writer = getWriteFile(write_file_name)
    jumpFirstRow(csv_reader)
    url_elemantary = 'http://maps.google.com/maps/api/geocode/json?latlng='
    count = 0
    setColHeader(csv_writer)
    for row in csv_reader:
        if not row[lat_idx] == '' and not row[lat_idx] == '':
            list = []
            count += 1
            if count == 10:
                time.sleep(2)
                count = 0
            lat = row[lat_idx]
            long = row[long_idx]
            url = url_elemantary + lat + ',' + long
            result = req.get(url).json()
            zip_code = getZipCode(result)
            list.append(row[unique_idx])
            list.append(row[lat_idx])
            list.append(row[long_idx])
            list.append(zip_code)
            csv_writer.writerow(list)


def main():
    read_file_name = '../RTBDA/NYPD_Complaint_Data_Historic.csv'
    write_file_name = '../RTBDA/Unique_Values/Lat_Long_Zipcode.csv'
    makeGetCall(read_file_name, write_file_name)

if __name__ == '__main__':
    main()