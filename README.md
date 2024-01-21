# Largests-banks-ETL-project
This is my python project that uses ETL for wikipedia List_of largest banks by market capitalization, You can check that in the link: 
https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks
The list is based on Forbes.com's ranking as of August 2023 based on an analysis of the bank's operations, financial performance, and overall impact on the global economy.
The data only has two features, the bank name and market capitalization in USD

# Techniques & Features used in the project:
1- Extraction of data in the website using Web Scarpping using BeautifulSoup package and requests
2- Transformation of data for market capitalization to EURO, GBP and INR currency using pandas package
3- Loading data to be saved to database and running some queries on it after step2, using sqlite3 package
4- Log process function that it associated to the time after running each process using Datetime package
