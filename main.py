import re
import time
import urllib
import random
import requests
import mysql.connector
from urllib.parse import urlparse
from bs4 import BeautifulSoup

mydb = mysql.connector.connect(
    host="localhost",
    user="mysql",
    password="password",
    database="db"
)


def has_more_pages(soup):
    dupetext = soup.find(class_="dupetext")
    if dupetext is not None:
        return False
    else:
        return True


def parse_listing_page(results):
    jobs = []
    for result in results:
        href = result.find(class_="jobTitle").find("a").attrs["href"]
        href = "https://www.indeed.com" + href
        jobs.append({"href": href})
    return jobs


def parse_description_page(href):
    if href is None or href["href"] is None:
        return None

    sleep = random.randrange(1, 6)
    time.sleep(sleep)
    job = {}
    print("scraping description page:", href["href"])
    page = requests.get(href["href"])

    soup = BeautifulSoup(page.content, 'html.parser')
    job["href"] = href["href"]

    job_title = soup.find(class_="jobsearch-JobInfoHeader-title-container")
    if job_title is not None:
        job["job_title"] = job_title.getText()
    else:
        job["job_title"] = "undefined"

    comp_link = soup.find(href=re.compile("https://www.indeed.com/cmp/"))
    if comp_link is not None:
        job["company_link"] = comp_link.getText()
    else:
        job["company_link"] = "unknown"

    title = soup.find("title")
    if title is not None:
        job["page_title"] = title.getText()
    else:
        job["page_title"] = ""

    subtitle = soup.find(id="salaryInfoAndJobType")
    if subtitle is not None:
        job["subtitle"] = subtitle.getText()
    else:
        job["subtitle"] = ""

    jd = soup.find(id="jobDescriptionText")
    if jd is not None:
        job["job_description"] = jd.prettify()
    else:
        job["job_description"] = "No Description Found"

    return job


def save_job(job):
    href = job.get("href")

    sql = "SELECT * FROM jobs WHERE url = '" + href + "'"
    curs = mydb.cursor()
    try:
        curs.execute(sql)
        res = curs.fetchone()
        if res is not None:
            return
    except:
        return

    title = job.get("page_title")

    if job.get("company_link") is not None:
        company = "" + job.get("company_link")
    else:
        company = ""

    sub = "" + job.get("subtitle")
    desc = "" + job.get("job_description")

    mycursor = mydb.cursor()
    sql = "INSERT INTO jobs(title, url, company, subtitle, description) VALUES(%s, %s, %s, %s, %s)"
    val = (title, href, company, sub, desc)
    try:
        mycursor.execute(sql, val)
        mydb.commit()
    except Exception:
        print("fail")


def remove_new():
    mycursor = mydb.cursor()
    sql = "DELETE FROM jobs WHERE status = 'new'"
    try:
        mycursor.execute(sql)
        mydb.commit()
    except Exception:
        print("fail")


def scrape(search):
    start = 0
    params = {'q': search.get("term"), 'start': str(start)}
    url = "https://www.indeed.com/jobs?" + urllib.parse.urlencode(params)
    while True:
        print('scraping main page:', url)
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        ul = soup.find("ul", class_="jobsearch-ResultsList")
        results = ul.findAll(class_="result")
        hrefs = parse_listing_page(results)
        for href in hrefs:
            job = parse_description_page(href)
            if job is not None:
                save_job(job)
        if has_more_pages(soup) and start < 1000:
            start += 10
            params = {'q': search.get("term"), 'start': str(start)}
            url = "https://www.indeed.com/jobs?" + urllib.parse.urlencode(params)
        else:
            print('done scraping')
            break


def run():
    remove_new()
    for search in [{'term': 'C++ Engineer'}]:
        scrape(search)


if __name__ == '__main__':
    run()
