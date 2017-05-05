import csv

read_file1 = csv.reader(open('../../RTBDA/Unique_Values/merged-Copy.csv','r'))
read_file2 = csv.reader(open('../../RTBDA/Unique_Values/2010+Census+Population+By+Zipcode+(ZCTA).csv','r'))
write_file = csv.writer(open('../../RTBDA/Unique_Values/merged-population.csv','w'))

count = 0
file1_zip_set = set()

for row_file1 in read_file1:
    file1_zip_set.add(row_file1[1])

for row_file2 in read_file2:
    count += 1 if row_file2[0] in file1_zip_set else 0