import requests
import argparse
import sys
import os
import bs4
import csv
import re
import enum
from datetime import datetime

class LogType(enum.Enum):
    crime = "crime"
    noncrime = "noncrime"

def _process_html(string, output_writer, log_type):
    soup = bs4.BeautifulSoup(string, features="lxml")
    table = soup.find(class_="articletext")

    header_row = table.find(class_="White")
    data1_list = []
    data2_list = []

    # For some reason each table is formatted slightly differently, so we have to account for that.
    if log_type == LogType.crime:
        data1_list = [data1 for data1 in table.find_all("tr") if len(data1.attrs) == 1 and "bgcolor" in data1.attrs]
        data2_list = [data2 for data2 in table.find_all("tr", class_="body")]
    elif log_type == LogType.noncrime:
        data1_list = [data1 for data1 in table.find_all("tr") if len(data1.attrs) == 2 and "id" in data1.attrs and "row" in data1.attrs["id"]]
        data2_list = [data2 for data2 in table.find_all("tr", class_="body")]

    for data1, data2 in zip(data1_list, data2_list):
        row = [re.sub(r'[\r\n\t]', '', value.text) for value in data1.find_all("td", limit=5)]

        # Format the date column into year-first format.
        try:
            row[1] = datetime.strptime(row[1], "%m/%d/%Y").strftime("%y/%m/%d")
        except ValueError:
            pass

        data2str = data2.find("td").text
        location_str = re.search(r'Location:(.*)\n', data2str).group(1)
        nature_str = re.search(r'Nature:(.*)\n', data2str).group(1)
        row.append(re.sub(r'[\r\n\t]', '', location_str).strip())
        row.append(re.sub(r'[\r\n\t]', '', nature_str).strip())
        output_writer.writerow(row)

    total_entry_count = table.find("span", style="font-weight: bold", string="of").parent.text.split()[-1]
    return int(total_entry_count)

def scrape(scrape_output_dir="./scraped",
           starting_offset=0,
           max_entries=0,
           max_retries=0,
           local_files_path=None):
    out_dir = os.path.join(scrape_output_dir, str(datetime.now()))  # the unique output dir we'll use
    os.makedirs(out_dir)

    process_chunk = 100  # how many entries (we think) are getting retrieved per request
    retry_count = 0

    for log_type in LogType:
        total_entries = max_entries
        entries_processed = starting_offset
        with open(os.path.join(out_dir, 'scrape-result-{}.csv'.format(log_type.value)), 'w') as output_file:
            output_writer = csv.writer(output_file)
            output_writer.writerow(["Case #", "Date Reported", "Occurrence Interval", "Disposition", "Status", "Location", "Nature"])

            if local_files_path is None:
                while total_entries == 0 or entries_processed < total_entries:
                    print("Processing entries {} through {}".format(entries_processed, entries_processed + process_chunk))

                    params = {"offset": str(entries_processed)}

                    try:
                        result = requests.get("http://police.gatech.edu/{}log.php".format(log_type.value), params)
                        result.raise_for_status()
                    except requests.exceptions.HTTPError:
                        if max_retries != 0 and retry_count >= max_retries:
                            print("Exceeded maximum retry count, aborting.")
                            exit(1)
                        else:
                            print("Request for entries starting at {} failed, retrying...".format(entries_processed))
                            retry_count += 1
                            continue

                    retry_count = 0
                    # Write fetched html to file, naming it [start-entry]-[end-entry]
                    with open(os.path.join(out_dir, "{}-{}-{}.html"
                            .format(log_type.value, entries_processed, entries_processed + process_chunk)), 'w') as result_html_file:
                        result_html_file.write(result.text)

                    reported_total_entries = _process_html(result.text, output_writer, log_type)
                    if total_entries == 0:
                        total_entries = reported_total_entries

                    entries_processed += process_chunk
            else:
                # We have a local set of previously-fetched files to use.
                local_files = [file for file in os.listdir(local_files_path)
                               if os.path.isfile(os.path.join(local_files_path, file)) and file.startswith(log_type.value) and file.endswith(".html")]
                for filename in local_files:
                    with open(os.path.join(local_files_path, filename), 'r') as file:
                        _process_html(file.read(), output_writer, log_type)

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description="Scrape GTPD crime/non-crime logs")
    argparser.add_argument("--scrape-output-dir", help="Where to save the scraped data.", default="./scraped")
    argparser.add_argument("--starting-offset", help="Collection offset from the most recent log entry.", default=0)
    argparser.add_argument("--max-entries", help="Maximum number of entries to gather from starting point."
                                                 "0 means no limit.", default=0, type=int)
    argparser.add_argument("--max-retries", help="Maximum number of retries. 0 means no limit.", default=5)
    argparser.add_argument("--local-files", help="Directory containing a set of local html files that contain GTPD"
                                                 "crime tables.")

    arg_list = sys.argv.copy()
    arg_list.pop(0)
    args = argparser.parse_args(arg_list)

    scrape(scrape_output_dir=args.scrape_output_dir,
           starting_offset=args.starting_offset,
           max_entries=args.max_entries,
           max_retries=args.max_retries,
           local_files_path=args.local_files)
