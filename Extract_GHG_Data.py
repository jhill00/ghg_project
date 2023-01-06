import asyncio
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
import os

# url that contains the table names of greenhouse gas summary data
ghg_url = 'https://www.epa.gov/enviro/greenhouse-gas-summary-model'
url_list = [ghg_url]

# base url for when we make api requests
base_url = 'https://data.epa.gov/efservice/'
request_format = 'csv'

# isolate the table names from the html document
# returns a list of the table names found
async def parse_html(html_text: str) -> list:

    html_soup = BeautifulSoup(html_text, 'html.parser')
    substring = 'p_table_name'
    # t is the text of all the href returns
    # finds where the substring is present in the html document
    tables = html_soup.find_all(href = lambda t: t and substring in t)

    table_list = []

    for i in range(0, len(tables)):
        parsed_href = tables[i]['href'].split('=')[1] # split each href link by '='. Second index contains the table name
        table_name = parsed_href.split('&')[0]
        table_list.append(table_name)

    return table_list

# fetch the html body from a given url
# returns a string of html text
async def fetch_url_data(session, url: str) -> str:
    try:
        async with session.get(url) as response:
            html_text = await response.text()
            return html_text

    except Exception as e:
        print(e)

# fetch the html body from a given url
# returns a list of the table names found in the html document
async def scrape_tables(session, url: str) -> list:

    html_text = await fetch_url_data(session = session, url = url)
    table_list = await parse_html(html_text = html_text)

    return table_list

# query the row count of a table
# returns a dictionary where key = table name, and value = row_count
async def query_count(session, table: str) -> dict:

    count_query = '/'.join([base_url,table,'count'])
    data = await fetch_url_data(session = session, url = count_query)

    r = data.replace('<','/').replace('>','/')
    row_count = int(r.split('/')[8])
    row_count_dict = {table:row_count}

    return row_count_dict

# creates urls based on a table's name and row count
# the EPA's API allows for a maximum 10,000 rows to be queried per request
async def create_urls(row_count_dict_list: list) -> list:

    all_url_list = []

    for i in row_count_dict_list:
        for table, row_count in i.items():
            row_min = 0
            row_max = 9999

            while(row_max < row_count):
                query_url_while = f'https://data.epa.gov/efservice/{table}/rows/{row_min}:{row_max}/{request_format}'
                all_url_list.append(query_url_while)
                row_min += 10000
                row_max += 10000
            else:
                query_url_else = f'https://data.epa.gov/efservice/{table}/rows/{row_min}:{row_count}/{request_format}'
                all_url_list.append(query_url_else)

    return all_url_list

# creates new files, or appends to exisitng ones
async def create_file(session, url: str):

    table_name = url.split('/')[4]
    file_name = '.'.join([table_name,'csv'])

    data = await fetch_url_data(session = session, url = url)

    if not os.path.exists(file_name):

        # creates a csv file
        async with aiofiles.open(file_name, mode = 'w') as file:
            await file.write(data)        

    else:

        # appends to the existing file
        with open(file_name, 'a', newline = '') as csvfile:
            split_data_rows = data.split('\n') # '\n' represents a new line in the csv data, so we want to isolate the first row (header)
            skip_header_row = split_data_rows[1:]
            skipped_header_data = '\n'.join(skip_header_row) # join elements of the list with '\n' to create new rows
            csvfile.write(skipped_header_data)

# main function to execute everything
async def main():

    async with aiohttp.ClientSession() as session:

        # scrapes table names from url
        # return a list of table names
        web_scrape_tasks = []
        for i in url_list:
            task = asyncio.create_task(scrape_tables(session = session, url = i))
            web_scrape_tasks.append(task)
        url_tables = await asyncio.gather(*web_scrape_tasks)
        url_tables = [item for sublist in url_tables for item in sublist] # uses list comprehesnsion to flatten nested list (from [[t1, t2,..]] to [t1, t2,..])

        # queries row counts for different tables
        # returns a list of dictionaries where key = table name, and key = row count
        query_count_tasks = []
        for i in url_tables:
            task = asyncio.create_task(query_count(session = session, table = i))
            query_count_tasks.append(task)
        row_count_dict_list = await asyncio.gather(*query_count_tasks)

        # produces a list of urls to query
        all_url_list = await create_urls(row_count_dict_list = row_count_dict_list)

        # saves the data to csv files
        save_csv_tasks = []
        for url in all_url_list:
            task = asyncio.create_task(create_file(session = session, url = url))
            save_csv_tasks.append(task)
        await asyncio.gather(*save_csv_tasks)

if __name__ == '__main__':
    asyncio.run(main())