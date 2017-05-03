import csv

file1 = csv.reader(open('../../RTBDA/Unique_Values/merged-Copy.csv','r'))
file2 = csv.reader(open('../../RTBDA/Unique_Values/Demographic_Statistics_By_Zip_Code.csv','r'))
#file2 = csv.reader(open('../../RTBDA/Unique_Values/2010+Census+Population+By+Zipcode+(ZCTA).csv','r'))

count = 0
file1_zip_set = set()

for row_file1 in file1:
    file1_zip_set.add(row_file1[1])

for row_file2 in file2:
    count += 1 if row_file2[0] in file1_zip_set else 0

print(len(file1_zip_set))
print(count)