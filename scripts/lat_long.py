import csv

lat_idx = 21
long_idx = 22

def getReadFile(read_file_name):
    return csv.reader(open(read_file_name, "rt"))

def getWriteFile(write_file_name):
    return csv.writer(open(write_file_name,'w', newline=''))


def create_new_csv(read_file_name, write_file_name):
    csv_reader = getReadFile(read_file_name)
    csv_writer = getWriteFile(write_file_name)
    lat_set = set()
    long_set = set()
    for row in csv_reader:
        list = []
        if not row[lat_idx] == '' and not row[lat_idx] in lat_set and not row[long_idx] == '' and not row[long_idx] in long_set:
            list.append(row[lat_idx])
            list.append(row[long_idx])
            csv_writer.writerow(list)


def main():
    read_file_name = '../../RTBDA/NYPD_Complaint_Data_Historic.csv'
    write_file_name = '../../RTBDA/Unique_Values/All_Lat_Long.csv'
    create_new_csv(read_file_name, write_file_name)

if __name__ == '__main__':
    main()