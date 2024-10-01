import csv

with open('sme300924.csv', 'r') as csv_file:
    csv_reader = csv.reader(csv_file)

    first_line = next(csv_reader)
    
    i = 1
    for line in csv_reader:
        file_name = line[2]+'.csv'
        with open(file_name, 'w') as new_file:
            csv_writer = csv.writer(new_file)
            csv_writer.writerow(first_line)
            csv_writer.writerow(line)
            print(line)
            if (i == 5):
                break
            i += 1
