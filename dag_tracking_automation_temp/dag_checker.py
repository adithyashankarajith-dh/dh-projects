# import libraries
from bs4 import BeautifulSoup
import json
import re
import pandas as pd
from dag_ids import dag_names


# specify file names
input_file = "input_html.html"
output_file = "dags_status_check.csv"

# open input html file, parse it and find all javascript tags
with open(input_file) as f:
    html_content = f.read()
soup = BeautifulSoup(html_content, 'html.parser')
script_tags = soup.find_all("script")


# search for "gridData" variable - variable which stores the dags and respective status'
pattern = r'gridData\s*=\s*"(\{.*?\})";'
match = re.search(pattern, html_content, re.DOTALL)


# if match exists clean the javascript object and convert to python dictionary
if match:
    grid_data_json = match.group(1)
    grid_data_json_corrected = grid_data_json.replace("\\", "")
    grid_data_dict = json.loads(grid_data_json_corrected)


# This iterates through each of the dag_names and if present in the list of dags assigns it to a dictionary
dag_list = []
for dag in dag_names:
    dag_dict = {}
    dag_dict["dag_name"] = dag
    for item in grid_data_dict["groups"]["children"]:
        if dag in item["id"]:
            dag_dict["id"] = item["id"]
            for count, child in enumerate(item["children"]):
                latest_instance = len(child["instances"]) - 1
                task_name = child["id"][child["id"].find(".")+1:(len(child["id"]))]
                task_name_date = task_name + "_latestupdatedate"
                dag_dict[task_name] = child["instances"][latest_instance]["state"]
                dag_dict[task_name_date] = child["instances"][latest_instance]["start_date"]

    dag_list.append(dag_dict)


# save dictionary as file 
df = pd.DataFrame(dag_list)
df.to_csv(output_file, index = False)
