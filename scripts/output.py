import csv
import numpy as np

def getReadFile(read_file_name):
    return csv.reader(open(read_file_name, "rt"))

def getWriteFile(write_file_name):
    return open(write_file_name,'w')

def create_text(csv_reader, out):
    data = np.array(list(csv_reader)).astype("int")
    for i in range(10):
        for j in range(data.shape[1]):
            out.write(str(j)+ ':' + str(data[i,j])+ '\t')
        out.write('\n')
    out.close()
    return out

def main():

    csv_reader = getReadFile(read_file_name='../../RTBDA/Unique_Values/merged.csv')
    out = getWriteFile(write_file_name='../../RTBDA/Unique_Values/merged1.txt')
    create_text(csv_reader, out)

if __name__ == '__main__':
    main()
