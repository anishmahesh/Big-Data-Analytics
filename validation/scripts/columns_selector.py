import csv

csv_reader = csv.reader(open('/Users/anish/NYU Coursework/RBDA/Project/Data/merged_with_headers.csv', "r"))
cols_to_keep = ['american','asian','continental','indian','italian','mexican','middle_eastern','vio_facilities','vio_food_handlin','vio_hygiene','vio_vermin','total_violations','critical_violations','sev1','sev2','sev3','sev4','severity1','severity2','attempted','completed','violation','misdemeanor','felony']

fw = open('/Users/anish/NYU Coursework/RBDA/Project/Data/spark_reg_input.txt', 'w')

col_names = dict()
first = next(csv_reader)

for i in range(len(first)):
    col_names[first[i]] = i

for row in csv_reader:
    line = ''
    line += row[col_names['score']]
    for idx, col in enumerate(cols_to_keep, start=1):
        line += ' ' + str(idx) + ':' + row[col_names[col]]
    line += '\n'
    fw.write(line)
fw.truncate(fw.tell()-1)
fw.close()