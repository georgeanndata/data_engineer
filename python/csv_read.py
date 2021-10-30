import os
import csv

# Read csv in python
# with open('BPI-Sep2020.csv') as csv_file:
#     csv_reader = csv.reader(csv_file, delimiter=',')
#     line_count = 0
#     for row in csv_reader:
#         if line_count == 0:
#             print(f'Column names are {", ".join(row)}')
#             line_count += 1
#         else:
#             print(f"""\tSeries: {row[0]} 
#             Description: {row[1]} 
#             Period: {row[2]} 
#             Previous: {row[3]} 
#             Revised: {row[4]}""")
#             line_count += 1
#             if line_count > 1:
#                 break
#     print(f'Processed {line_count} lines.')

# with open('BPI-Sep2020.csv') as csv_file:
#     csv_reader = csv.DictReader(csv_file)
#     line_count = 0
#     for row in csv_reader:
#         if line_count == 0:
#             print(f'Column names are {", ".join(row)}')
#             line_count += 1
#         print(f"""\t{row['Series reference']} 
#             {row['Description']} 
#             {row['Period']} 
#             {row['Previously published']} 
#             {row['Revised']}""")            
#         line_count += 1
#         if line_count > 1:
#             break

#     print(f'Processed {line_count} lines.')

    # Write a csv file
with open('emp_file.csv', mode='w') as employee_file:
    employee_writer = csv.writer(employee_file, delimiter=',')
    employee_writer.writerow(['Name', 'Department', 'Month'])
    employee_writer.writerow(['Jack C', 'Marketing', 'November'])
    employee_writer.writerow(['Eric Smith', 'Software', 'March'])


with open('emp_file_dict.csv', mode='w') as csv_file:
    colnames = ['Name', 'Department', 'Month']
    writer = csv.DictWriter(csv_file, fieldnames=colnames)
    writer.writeheader()
    writer.writerow({'Name': 'Jack C', 'Department': 'Marketing', 'Month': 'November'})
    writer.writerow({'Name': 'Eric Smith', 'Department': 'Software', 'Month': 'March'})        