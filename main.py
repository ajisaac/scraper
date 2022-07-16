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


def mark_new_jobs_old():
    mycursor = mydb.cursor()
    sql = "UPDATE jobs SET status = 'old' WHERE status = 'new'"
    try:
        mycursor.execute(sql)
        mydb.commit()
    except Exception:
        print("fail")


def remove_old_jobs():
    mycursor = mydb.cursor()
    sql = "DELETE FROM jobs WHERE status = 'old'"
    try:
        mycursor.execute(sql)
        mydb.commit()
    except Exception:
        print("fail")


def mark_job_new(href):
    mycursor = mydb.cursor()
    sql = "UPDATE jobs SET status = 'new' WHERE url = '" + href + "'"
    try:
        mycursor.execute(sql)
        mydb.commit()
    except Exception:
        print("fail")


def is_blacklisted(company):
    curs = mydb.cursor()
    sql = "SELECT * FROM blacklist_companies WHERE name = '" + company + "'"
    try:
        curs.execute(sql)
        res = curs.fetchone()
        if res is not None:
            return True
        return False
    except:
        return False


def job_exists(href):
    curs = mydb.cursor()
    sql = "SELECT * FROM jobs WHERE url = '" + href + "'"
    try:
        curs.execute(sql)
        res = curs.fetchone()
        if res is not None:
            return True
        return False
    except:
        return False


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
    sleep = random.randrange(1, 6)
    time.sleep(sleep)
    print("sleeping: " + str(sleep))
    job = {}
    print("scraping description page:", href)
    page = requests.get(href)

    soup = BeautifulSoup(page.content, 'html.parser')
    job["href"] = href

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

    if job_exists(href):
        return

    title = job.get("page_title")

    if job.get("company_link") is not None:
        company = "" + job.get("company_link")
    else:
        company = ""

    sub = "" + job.get("subtitle")
    desc = "" + job.get("job_description")
    term = "" + job.get("term")

    if is_blacklisted(company):
        return

    mycursor = mydb.cursor()
    sql = "INSERT INTO jobs(title, url, company, subtitle, description, search_term) VALUES(%s, %s, %s, %s, %s, %s)"
    val = (title, href, company, sub, desc, term)
    try:
        mycursor.execute(sql, val)
        mydb.commit()
    except Exception:
        print("failed to save job")


def scrape_job(url, term):
    href = url["href"]
    if href is None:
        return
    if not job_exists(href):
        job = parse_description_page(href)
        if job is not None:
            job["term"] = term
            print("Saving job " + job.get("job_title"))
            save_job(job)
    else:
        print("Job already exists " + href)
        mark_job_new(href)


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
            scrape_job(href, search.get("term"))

        # 1000 is a good amount of pages
        if has_more_pages(soup) and start < 1000:
            start += 10
            params = {'q': search.get("term"), 'start': str(start)}
            url = "https://www.indeed.com/jobs?" + urllib.parse.urlencode(params)
        else:
            print('done scraping')
            break

        sleep = random.randrange(1, 6)
        time.sleep(sleep)
        print("sleeping: " + str(sleep))


def run():
    mark_new_jobs_old()
    for search in [{'term': 'Java Developer'}]:
        scrape(search)
    remove_old_jobs()


if __name__ == '__main__':
    run()
