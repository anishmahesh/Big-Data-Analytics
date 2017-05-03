import csv
import numpy as np
from numpy import genfromtxt

On_Dumbo = True

def getReadFile(read_file_name):
    return csv.reader(open(read_file_name, "r"))

def getWriteFile(write_file_name):
    return open(write_file_name,'w')


def cleanData(data):
    new_data = []
    for i in range(len(data)):
        useRow = False
        for col in data[i]:
            try:
                float(col)
                if (35 <= float(col) <= 75):
                    useRow = True
            except:
                pass
        if(useRow):
            useRow = False
            new_data.append(data[i])
    return new_data


def create_text(out, csv_reader = None):
    data = np.array(list(csv_reader)).astype("str")
    #data = genfromtxt('../../RTBDA/Unique_Values/311_Cleaned_new.csv', delimiter=',')
    data = cleanData(data)
    for i in range(len(data)):
        line = ''
        line += str(i)
        for idx, col in enumerate(data[i], start=1):
            line += ' ' + str(idx) + ':' + str(col)
        line += '\n'
        out.write(line)
    out.truncate(out.tell() - 2)
    out.close()
    return out

def main():
    if On_Dumbo:
        csv_reader = getReadFile('../../FinalDatasets/cab_datasets/Lat_Long_Drop.csv')
        out = getWriteFile('../../Naman-Data/list2.txt')
    else:
        csv_reader = getReadFile(read_file_name='../../../RTBDA/Unique_Values/Lat_Long_Drop.csv')
        out = getWriteFile(write_file_name='../../RTBDA/Unique_Values/list2.txt')

    create_text(out, csv_reader)
    #create_text(out)
if __name__ == '__main__':
    main()
