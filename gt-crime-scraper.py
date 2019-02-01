import requests
import argparse
import sys
import os
from datetime import datetime
from bs4 import BeautifulSoup


def scrape(scrape_output_dir="./scraped",
           starting_offset=0,
           max_entries=0,
           max_retries=0):
    out_dir = os.path.join(scrape_output_dir, str(datetime.now()))  # the unique output dir we'll use
    os.makedirs(out_dir)

    process_chunk = 100  # how many entries (we think) are getting retrieved per request
    entries_processed = 0
    retry_count = 0

    with open(os.path.join(out_dir, 'scrape-result.csv'), 'a') as output_file:
        if max_entries == 0: max_entries = sys.maxsize

        while entries_processed < max_entries:
            print("Processing entries {} through {}".format(entries_processed, entries_processed + process_chunk))

            params = {"offset": str(entries_processed)}

            try:
                result = requests.get("http://police.gatech.edu/crimelog.php", params)
                result.raise_for_status()
            except requests.exceptions.HTTPError:
                if retry_count >= max_retries:
                    print("Exceeded maximum retry count, aborting.")
                    exit(1)
                else:
                    print("Request for entries starting at {} failed, retrying...".format(entries_processed))
                    retry_count += 1
                    continue

            # Write fetched html to file, naming it [start-entry]-[end-entry]
            with open(os.path.join(out_dir, "{}-{}.html".format(entries_processed, entries_processed + process_chunk)),
                      'w') as result_html_file:
                result_html_file.write(result.text)

            entries_processed += process_chunk


if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description="Scrape GTPD crime/non-crime logs")
    argparser.add_argument("--scrape-output-dir", help="Where to save the scraped data.", default="./scraped")
    argparser.add_argument("--starting-offset", help="Collection offset from the most recent log entry.", default=0)
    argparser.add_argument("--max-entries", help="Maximum number of entries to gather from starting point."
                                                 "0 means no limit.", default=0)
    argparser.add_argument("--max-retries", help="Maximum number of retries. 0 means no limit.", default=5)

    arg_list = sys.argv.copy()
    arg_list.pop(0)
    args = argparser.parse_args(arg_list)

    scrape(scrape_output_dir=args.scrape_output_dir,
           starting_offset=args.starting_offset,
           max_entries=args.max_entries,
           max_retries=args.max_retries)
