import requests
import bs4
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sys
from tqdm import tqdm
from tqdm import trange
import time
from time import sleep
from IPython.display import clear_output
import random
import string 
from WebScraperDB import WebScraperDB
import re
import uuid
import config


def extract_id_from_result(results_page):
    try:
        job_attr = results_page.get('id').replace(' ','')
        job_attr = job_attr.lstrip().rstrip()
        job_attr = job_attr
    except:
        job_attr = str(uuid.uuid4())
    return job_attr

def extract_salary_from_result(results_page):
    try:
        if results_page.find('span', attrs={'class': 'salaryText'}) is not None:
            job_attr = results_page.find('span', attrs={'class': 'salaryText'}).text.replace('\n','')
        else:
            job_attr = 'NULL'
    except:
        job_attr = 'NULL'
    return job_attr

def extract_company_from_result(results_page):
    try:
        job_attr = results_page.find('span', attrs={'class': 'company'}).text.replace('\n','')
    except:
        job_attr = 'NULL'
    return job_attr

def extract_location_from_result(results_page):
    try:
        job_attr = results_page.find('span', attrs={'class': 'location'}).text.replace('\n','')
    except:
        job_attr = 'NULL'
    return job_attr

def extract_summary_from_result(results_page):
    try:
        job_page_url = 'https://www.indeed.co.uk'+results_page.find('a')['href']
        r = requests.get(job_page_url, 'text')
        soup = BeautifulSoup(r.text, 'lxml')
        summary_paragraphs = soup.find_all('div', attrs={'id':'jobDescriptionText'})
        job_attr = [s.text.replace('\n','') for s in summary_paragraphs][0]
    except:
        job_attr = 'NULL'
    return job_attr

def extract_jobtitle_from_result(results_page):
    try:
        job_attr = results_page.find('a').text.replace('\n','')
        pattern = re.compile(r'[0-9-][^ ]+')
        job_attr = job_attr.replace(''.join(re.findall(pattern, job_attr)), '').lstrip().rstrip()
    except:
        job_attr = 'NULL'
    return job_attr


def slack_payload(payload_type, attempt_num, timetaken=None, 
                  num_results=None, interval=None, 
                  jobs_scanned=None, max_iters=None, 
                  error=None):

    
    if payload_type == 'interval':
        payload = { "attachments": [
                                        {
                                            "color": "#59bfff",
                                            "blocks": [
                                                {
                                                    "type": "section",
                                                    "text": {
                                                        "text": f"The Indeed web scraper is *{interval}% completed*",
                                                        "type": "mrkdwn"
                                                    },
                                                    "fields": [
                                                        {
                                                            "type": "mrkdwn",
                                                            "text": "*Job titles scanned*"
                                                        },
                                                        {
                                                            "type": "plain_text",
                                                            "text": f"{jobs_scanned}"
                                                        },
                                                        {
                                                            "type": "mrkdwn",
                                                            "text": "*Time taken*"
                                                        },
                                                        {
                                                            "type": "plain_text",
                                                            "text": f"{timetaken:0.2f} minutes"
                                                        },
                                                        {
                                                            "type": "mrkdwn",
                                                            "text": "*Attempt Number*"
                                                        },
                                                        {
                                                            "type": "plain_text",
                                                            "text": f"{attempt_num}"
                                                        },
                                                        {
                                                            "type": "mrkdwn",
                                                            "text": "*Max results*"
                                                        },
                                                        {
                                                            "type": "plain_text",
                                                            "text": f"{max_iters}"
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
        
    elif payload_type == 'success':
        payload = { "attachments": [
                                        {
                                            "color": "#00FF00",
                                            "blocks": [
                                                {
                                                    "type": "section",
                                                    "text": {
                                                        "text": f"The web scraping was *successful*",
                                                        "type": "mrkdwn"
                                                    },
                                                    "fields": [
                                                        {
                                                            "type": "mrkdwn",
                                                            "text": "*Time taken*"
                                                        },
                                                        {
                                                            "type": "plain_text",
                                                            "text": f"{timetaken:0.1f} minutes ({timetaken/60:0.2f} hours)"
                                                        },
                                                        {
                                                            "type": "mrkdwn",
                                                            "text": "*Attempt number*"
                                                        },
                                                        {
                                                            "type": "plain_text",
                                                            "text": f"{attempt_num}"
                                                        },
                                                        {
                                                            "type": "mrkdwn",
                                                            "text": "*Numer of results*"
                                                        },
                                                        {
                                                            "type": "plain_text",
                                                            "text": f"{num_results}"
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
            
    elif payload_type == 'failure':
        payload = { "attachments": [
                                        {
                                            "color": "#FF0000",
                                            "blocks": [
                                                {
                                                    "type": "section",
                                                    "text": {
                                                        "text": f"There is a problem with the web scraping",
                                                        "type": "mrkdwn"
                                                    },
                                                    "fields": [
                                                        {
                                                            "type": "mrkdwn",
                                                            "text": "*Time taken*"
                                                        },
                                                        {
                                                            "type": "plain_text",
                                                            "text": f"{timetaken:0.1f} minutes"
                                                        },
                                                        {
                                                            "type": "mrkdwn",
                                                            "text": "*Attempt number*"
                                                        },
                                                        {
                                                            "type": "plain_text",
                                                            "text": f"{attempt_num}"
                                                        },
                                                        {
                                                            "type": "mrkdwn",
                                                            "text": "*Error message*"
                                                        },
                                                        {
                                                            "type": "plain_text",
                                                            "text": f"{error}"
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }

    return payload

def scrape_job_sites(webscraper_db, attempt=0):

    position_titles = ['data+scientist', 'data+analyst', 'bi+analyst', 'nlp+engineer', 
                    'nlp+scientist', 'business+intelligence', 'machine+learning', 'research+scientist']

    # slack address for confirming if web scraping has completely successfully or not
    hook_url = config.SLACK_HOOK_URL
    
    # instantiate webscraper_db class and create table if it hasn't already been created
    webscraper_db._create_table()

    tic = time.perf_counter()
    try:
        all_results = []
        num_positions = len(position_titles)
        for i, role in enumerate(position_titles, start=1):
            perc_remaining = ((num_positions - i+1) / num_positions) * 100
            toc_update = (time.perf_counter() - tic) / 60

            # generate max number of jobs available on page to avoid unnecessary scraping
            URL = f"https://www.indeed.co.uk/jobs?q={role}+%C2%A320,000&l=united+kingdom"
            r = requests.get(URL, 'text')
            soup = BeautifulSoup(r.text, 'lxml')
            num_of_jobs = soup.find('div', attrs={'id': 'searchCountPages'}).text
            pattern = re.compile('(\d+)(?!.*\d)')
            max_results = int(''.join(re.findall(pattern, num_of_jobs)))

            # call webhook for updates to progress
            payload = slack_payload('interval', attempt, interval=100-perc_remaining, jobs_scanned=f"{(i)}/{num_positions}",
                                    timetaken=toc_update, max_iters=max_results)
            requests.post(hook_url, json=payload)

            # start html scanning
            with tqdm(range(0, max_results, 10), file=sys.stdout) as pbar:
                for start in range(0, max_results, 10):
                    URL = f"https://www.indeed.co.uk/jobs?q={role}+%C2%A320,000&l=united+kingdom&start={start}"
                    r = requests.get(URL, 'text')
                    soup = BeautifulSoup(r.text, 'lxml')
                    
                    # go through each element in the results page and add to list 
                    job_results = []
                    for elem in soup.find_all('div', attrs={'data-tn-component': 'organicJob'}):
                        
                        # discard all jobs without salary info
                        if extract_salary_from_result(elem) == 'NULL':
                            continue
                        else:
                            job_results.append(elem)
                            
                    # find elements and organise for inserting into database
                    rows_to_insert = tuple((extract_id_from_result(elem), extract_jobtitle_from_result(elem), extract_company_from_result(elem),
                                                extract_salary_from_result(elem), extract_location_from_result(elem), 
                                            extract_summary_from_result(elem), URL) for elem in job_results)

                    try:
                        # add data to database
                        webscraper_db.commit_rows_to_db(rows_to_insert)
                    except:
                        print(rows_to_insert)
                        continue

                    # current job counts to list for summing at end
                    all_results.append(rows_to_insert)
                    
                    # print tqdm-related info
                    pbar.set_description(f'Remaining: total = [{round(perc_remaining,1)}%]; jobs = [{num_positions-i+1}/{num_positions}]')
                    pbar.update(1)
                    sleep(random.uniform(0.3, 1))
                clear_output(wait=True)
            clear_output(wait=True)
        print(f'Finished scraping United Kingdom.')
        print(f'Found {sum(len(elem) for elem in all_results)} potential jobs.')

        toc = time.perf_counter()
        time_taken = ((toc - tic) / 60)
        payload = slack_payload('success', attempt, num_results=sum(len(elem) for elem in all_results), timetaken=time_taken)       
    except Exception as e:
        toc = time.perf_counter()
        time_taken = ((toc - tic) / 60)
        payload = slack_payload('failure', attempt, timetaken=time_taken, error=e)   
        all_results  = None
    
    # post success or failure messages to slack
    requests.post(hook_url, json=payload)
    
    return all_results


def main():
    webscraper_db = WebScraperDB('connection.cfg')
    attempts = 10
    counter = 1
    while counter <= attempts:
        results = scrape_job_sites(webscraper_db, attempt=counter)
        if results is None:
            print(f'Something went wrong: trying again. {attempts-counter} attempts remaining.')
            counter+=1
            continue
        else:
            break

if __name__ == "__main__":
    main()
