ETL Using PYTHON : Top 25 Countries GPD in the World using Wikipedia.

Used Modules int this project (Python): 
    1. requests
    2. BeautifulSoup
    3. bs4
    4. pandas
    5. numpy
    6. datetime
Functionalities:
    1. main : Used to initialize variables, function calling of individual Extract, Transform, Loading and Etc.
    2. extract : This function takes input url as parameter the extract the html file of provided in the url and extract the raw data from it and retuns pandas DataFrame.
    3. transform : This function takes input dataframe that returned by extract function then it cleans up messy data like converting Millions into Billions and removed remaining unnecessary data and then returns clean dataframe.
    4. loading: This function takes input dataframe that tranformed by transform function then it creates two fiile one is .csv file in the same location and a table in provided database name then return those files locations.
    5. run_query: This function takes a query, database name and table_name as input then runs provided query and returns True.
    6. log_process: Last but not least, this function maitain a logbook of query execution details including timestamp, it automatically called by run_query function.
                    
Input: If required change the url in the main function.

Output: While execution of each function it will the real time updates of execution, if any error isn't occured while exection finally it prints a star line whic indicates successful completion of execution.

Note: This is my first practicing project of ETL process, there are many possibilities to have errors in the project.
      There are three files are available Original copy is the file that is provided by guides and remaining two are created by me while learning the ETL process.

I learnt a lot while doing this project, Thank You.
      
