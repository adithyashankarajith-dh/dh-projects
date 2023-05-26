--- This is a temporary(and hacky) solution until we get our own dag airflow access rights ---
--- Does not follow best coding practices as of now. PRs to update this are more than welcome ---
--- Please follow the steps outlined below to get started ---

1. If python is not installed on your machine, install and setup python as per this link -> https://confluence.deliveryhero.com/pages/viewpage.action?spaceKey=DINV&title=4.+Python
    1.1 To check if python is installed on your machine, in your terminal run the command 'python3 --version' or 'python --version'
2. Install the following libraries using the following commands(If pip install 'library' doesn't work, try pip3 install 'library')
    2.1 pip install pandas
    2.2 pip install beautifulsoup4
3. Git clone the following repository or put the files from the repository into a standalone folder
    3.1
4. In the dag_ids.py file, specify the dag names required to be tracked in the list called dag_names.
    4.1 If more dags are added to the dag tracker list, they needed to be added to the dag_names list.
5. Update the input.html file as per the below instructions
    5.1 Go to the airflow link for tracking the dags ->
    5.2 Right click and select 'View Page Source' (this can be different on different browsers)
    5.3 Copy all of the 'Page Source' code into the input.html file
6. Run the python script and you should get a file called 'dags_status_check.csv' in the same folder.
6. (Optional) If you want to chane the input_file and output_file names, you can change them in dag_checker.py
