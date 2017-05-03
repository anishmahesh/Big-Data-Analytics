import csv
import numpy as np

def getReadFile(read_file_name):
    return csv.reader(open(read_file_name, "r"))

def getWriteFile(write_file_name):
    return open(write_file_name,'w')

def create_text(csv_reader, out):
    data = np.array(list(csv_reader)).astype("float")
    for i in range(len(data)):
        line = ''
        for idx, col in enumerate(data[i], start=1):
            line = str(i) + ' ' + str(idx) + ':' + str(col) + ' ' + str(idx) + ':' + str(col)
            line += '\n'
        out.write(line)
    out.truncate(out.tell() - 2)
    out.close()
    return out

def main():

    csv_reader = getReadFile(read_file_name='../../RTBDA/Unique_Values/lat_long_list.csv')
    out = getWriteFile(write_file_name='../../RTBDA/Unique_Values/list1.txt')
    create_text(csv_reader, out)

if __name__ == '__main__':
    main()
