📊 ETL Using Python: Top 25 Countries by GDP (Wikipedia)

This project demonstrates a complete ETL (Extract, Transform, Load) pipeline using Python.
Data is sourced from Wikipedia and processed into a clean, structured form for analysis.

You will find:

    Cleaned GDP data of the Top 25 Countries

    Automated CSV generation

    Automated SQL table creation

    Query execution & logging

This was my first practice project in ETL, and it helped me understand the complete workflow from extraction to loading.

🧰 Technologies & Python Modules Used
    1. requests
    2. BeautifulSoup
    3. bs4
    4. pandas
    5. numpy
    6. datetime

🧠 ETL Pipeline Overview

The project follows a standard ETL structure:

1. main()

    Initializes variables

    Calls all ETL functions (extract, transform, loading)

    Controls complete execution flow

2. extract(url)

    Takes a Wikipedia URL as input

    Downloads the raw HTML

    Parses the table using BeautifulSoup

    Converts extracted data into a pandas DataFrame

    Returns the raw DataFrame

3. transform(df)

    Cleans messy GDP data

    Converts values (e.g., Millions → Billions)

    Removes unnecessary symbols or characters

    Returns a clean, analysis-ready DataFrame

4. loading(df)

    Saves the transformed data as:

    A CSV file in the project folder

    A table in a database (provided by user)

    Returns the paths of the generated files

5. run_query(query, db_name, table_name)

    Executes a SQL query on the loaded table

    Returns True on successful execution

    Automatically calls the log function

6. log_process()

    Maintains a logbook of:

    Executed queries

    Status

    Timestamp

    Helps monitor execution history

🔧 Input Configuration

If needed, update the Wikipedia URL inside the main() function.

Example: Replace with any GDP-related or country data table URL from Wikipedia.

🟢 Output

During execution, the script prints real-time status updates from each stage.
On successful completion without errors, it prints a final star line indicating success.

Generated outputs:

    Cleaned CSV file

    SQL table inside chosen database

    Log file maintaining query history

📁 Project Files

    Original Copy → Provided by guides

    Two Modified Versions → Written by me while learning ETL

    This project helped me understand the complete ETL process in depth.

🙌 Learning Experience

    This was my first ETL practice project, and through it I learned:

    Web scraping

    Data cleaning

    Data transformations

    Structured loading into databases

    Logging & reproducibility

    Handling real-world messy data

Thank you.
