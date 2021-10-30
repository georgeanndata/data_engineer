import os
import json

# Convert JSON to Python dict
json_str = """
{
    "name": "Jack",
    "age": 29,
    "city": "Toronto",
    "country": "Canada"
}
"""
# parse out_json:
data = json.loads(json_str)
print (type(data))
print (data)

# Convert Python dict to JSON
data = {
    "name": "John",
    "age": 28,
    "city": "New York",
    "country": "USA"
}

# json_str = json.dumps(data)
# print (type(json_str))
# print (json_str)

DATA_FILE = "data_file.json"
# Write to file
with open(DATA_FILE, "w") as write_file:
    json.dump(data, write_file)
# Check if the file exists
print (f'File exists : {os.path.isfile(DATA_FILE)}')

# Read from file
with open(DATA_FILE, "r") as read_file:
    data = json.load(read_file)
print (data)
