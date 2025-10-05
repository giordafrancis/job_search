#!/usr/bin/env python3
import pandas as pd
import httpx
from bs4 import BeautifulSoup as bs
import json
from urllib.parse import urlencode
from datetime import datetime
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText



class JobSource:
    "Base class for all job sources"
    def __init__(self, keywords, location=None, distance=10, lat=None, lon=None):
        self.keywords = keywords
        self.location = location
        self.distance = distance
        self.lat = lat
        self.lon = lon
        
    def search(self): 
        "Search for jobs and return raw data"
        raise NotImplementedError
        
    def normalize(self, raw_data): 
        "Convert source-specific data to standard format"
        raise NotImplementedError
    
    def filter_design_tech_jobs(self, df):
        "Filter jobs containing design/technology keywords"
        if df.empty: return df
        import re
        pattern = r'(design|technology|d\s*&\s*t)'
        # Check both title and description columns
        title_match = df.get('title', pd.Series()).fillna('').str.contains(pattern, case=False, regex=True)
        desc_match = df.get('shortDescription', pd.Series()).fillna('').str.contains(pattern, case=False, regex=True)
        
        return df[title_match | desc_match]
        
    def get_jobs(self):
        "Main method to get normalized job listings"
        raw_data = self.search()
        return self.filter_design_tech_jobs(self.normalize(raw_data))


class TesJobSource(JobSource):
    "TES job source implementation"
    def __init__(self, keywords, location="CR5 1SS", distance=10, 
                 lat="51.30662208651764", lon="-0.1133822439545745", max_pages=2):
        super().__init__(keywords, location, distance, lat, lon)
        self.max_pages = max_pages
        
    def extract_page_data(self, url):
        "Extract job data from a TES jobs page"
        response = httpx.get(url)
        soup = bs(response.text, "html.parser")
        next_data_script = soup.find('script', id='__NEXT_DATA__')
        if not next_data_script: return None
        json_data = json.loads(next_data_script.string)
        try: return json_data['props']['pageProps']['trpcState']['json']['queries'][0]['state']['data']
        except (KeyError, IndexError): return None
    
    def extract_job_data(self, job):
        "Extract and flatten relevant job data from a job listing"
        result = {}
        for k in ['id', 'title', 'promoted', 'shortDescription', 'displayLocation', 'quickApply']:
            if k in job: result[k] = job[k]
        if 'contractTerms' in job: result['contractTerms'] = ', '.join(job['contractTerms'])
        if 'contractTypes' in job: result['contractTypes'] = ', '.join(job['contractTypes'])
        if 'employer' in job and 'name' in job['employer']: result['employer_name'] = job['employer']['name']
        if 'salary' in job:
            if job['salary'].get('description'): result['salary_description'] = job['salary']['description']
            if job['salary'].get('range'): result['salary_range'] = job['salary']['range']
        if 'advert' in job:
            if 'startDate' in job['advert']: result['advert_startDate'] = job['advert']['startDate']
            if 'endDate' in job['advert']: result['advert_endDate'] = job['advert']['endDate']
        if 'application' in job and 'closeDate' in job['application']:
            result['application_closeDate'] = job['application']['closeDate']
        if 'canonicalUrl' in job: result['url'] = job['canonicalUrl']
        result['source'] = 'TES'
        return result
        
    def search(self, max_pages=2):
        "Search TES jobs and return raw data"
        params = dict(keywords=self.keywords, displayLocation=self.location, lat=self.lat, lon=self.lon,
                     distance=self.distance, distanceUnit="mi", sort="distance")
        base_url = "https://www.tes.com/jobs/search"
        all_jobs = []
        total_found = 0
        
        for page in range(1, max_pages + 1):
            params["page"] = page
            url = f"{base_url}?{urlencode(params)}"
            print(f"Fetching page {page}")        
            page_data = self.extract_page_data(url)
            if not page_data or 'jobs' not in page_data or not page_data['jobs']: break
            page_jobs = [self.extract_job_data(job) for job in page_data['jobs']]
            all_jobs.extend(page_jobs)
            if page == 1: total_found = page_data.get('numFound', 0)
            print(f"Found {len(page_jobs)} jobs on page {page}")
            if len(page_jobs) < 20: break
        
        print(f"Total jobs collected: {len(all_jobs)} out of {total_found} available")
        return all_jobs
        
    def normalize(self, raw_data):
        "Convert TES data to standard format"
        if not raw_data: return pd.DataFrame()
        df = pd.DataFrame(raw_data)
        df['advert_startDate'] = pd.to_datetime(df['advert_startDate'])
        df['application_closeDate'] = pd.to_datetime(df['application_closeDate'])
        now = pd.Timestamp.utcnow()
        seven_days_ago = now - pd.Timedelta(days=7)
        df['status'] = df['advert_startDate'].apply(lambda x: 'recent' if x >= seven_days_ago else 'older')
        df['days_to_apply'] = (df['application_closeDate'] - now).dt.days
        df['full_url'] = df['url'].apply(lambda x: f"https://www.tes.com{x}" if pd.notna(x) else "No URL available")
        df['advert_startDate_formatted'] = df['advert_startDate'].dt.strftime('%d %B %Y, %H:%M')
        df['application_closeDate_formatted'] = df['application_closeDate'].dt.strftime('%d %B %Y, %H:%M')
        if 'advert_endDate' in df.columns:
            df['advert_endDate'] = pd.to_datetime(df['advert_endDate'])
            df['advert_endDate_formatted'] = df['advert_endDate'].dt.strftime('%d %B %Y, %H:%M')
        return df


class GovJobSource(JobSource):
    "GOV.UK Teaching Vacancies job source implementation"
    def __init__(self, keywords, location="The Glade, Coulsdon CR5 1SS", distance=10):
        super().__init__(keywords, location, distance)
        self.base_url = "https://teaching-vacancies.service.gov.uk/jobs"
        
    def search(self):
        "Search GOV.UK Teaching Vacancies and return raw data"
        params = dict(
            keyword=self.keywords,
            location=self.location,
            radius=self.distance,
            sort_by="distance",
            teaching_job_roles=["teacher"],
            phases=["secondary", "sixth_form_or_college"]
        )
        url = f"{self.base_url}?{urlencode(params, doseq=True)}"
        print(f"Fetching jobs from Teaching Vacancies: {url}")
        response = httpx.get(url)
        soup = bs(response.text, "html.parser")
        return self._extract_jobs_from_soup(soup)
    
    def _extract_jobs_from_soup(self, soup):
        "Extract job listings from the page HTML"
        jobs = []
        job_items = soup.select(".search-results__item")
        for item in job_items:
            job = {}
            # Extract job title and URL
            title_elem = item.select_one(".govuk-heading-m a")
            if title_elem:
                job['title'] = title_elem.text.strip()
                job['url'] = title_elem.get('href')
                
            # Extract location
            location_elem = item.select_one(".address")
            if location_elem:
                job['displayLocation'] = location_elem.text.strip()
                
            # Extract other details from summary list
            details = item.select(".govuk-summary-list__row")
            for detail in details:
                key_elem = detail.select_one(".govuk-summary-list__key")
                val_elem = detail.select_one(".govuk-summary-list__value")
                if key_elem and val_elem:
                    key = key_elem.text.strip().lower().replace(' ', '_')
                    job[key] = val_elem.text.strip()
            
            # Add source identifier
            job['source'] = 'GOV.UK'
            jobs.append(job)
            
        print(f"Found {len(jobs)} jobs on Teaching Vacancies")
        return jobs
    
    def normalize(self, raw_data):
        "Convert GOV.UK Teaching Vacancies data to standard format"
        if not raw_data: return pd.DataFrame()
        df = pd.DataFrame(raw_data)
        
        # Current time - keep it timezone naive to match the parsed dates
        now = pd.Timestamp.now()
        
        if 'closing_date' in df.columns:
            # Parse dates without timezone information
            df['application_closeDate'] = pd.to_datetime(df['closing_date'], errors='coerce')
            df['application_closeDate_formatted'] = df['closing_date']
        
        # Calculate days to apply with consistent timezone handling
        if 'application_closeDate' in df.columns and not df['application_closeDate'].isna().all():
            df['days_to_apply'] = (df['application_closeDate'] - now).dt.days
        
        # Format URLs
        if 'url' in df.columns:
            df['full_url'] = df['url'].apply(lambda x: f"https://teaching-vacancies.service.gov.uk{x}" if x.startswith('/') else x)
        
        # Set status (we don't have advert_startDate, so use a placeholder)
        df['status'] = 'current'
        
        # Rename some columns to match TES format
        if 'school_type' in df.columns:
            df['employer_name'] = df['school_type'].str.split(',').str[0]
        
        if 'pay_scale' in df.columns:
            df['salary_description'] = df['pay_scale']
            
        if 'working_pattern' in df.columns:
            df['contractTypes'] = df['working_pattern']
            
        return df
    
class RaaJobSource(JobSource):
    "Royal Alexandra & Albert School job source implementation"
    def __init__(self, keywords=None, max_pages=3):
        super().__init__(keywords=keywords)
        self.base_url = "https://raaschool.face-ed.co.uk/vacancies"
        self.max_pages = max_pages
        
    def search(self):
        "Search RAA School and return raw data across multiple pages"
        all_jobs = []
        
        for page in range(1, self.max_pages + 1):
            url = f"{self.base_url}?filter=&currentpage={page}"
            print(f"Fetching jobs from RAA School: {url}")
            response = httpx.get(url)
            soup = bs(response.text, "html.parser")
            
            # Check if the page has no results
            no_results_text = soup.select_one(".text-center b")
            if no_results_text and "No results found" in no_results_text.text:
                print(f"No results found on page {page}. Stopping pagination.")
                break
                
            jobs = self._extract_jobs_from_soup(soup)
            
            # If no jobs found, assume we've reached the end
            if not jobs:
                print(f"No jobs found on page {page}. Stopping pagination.")
                break
                
            all_jobs.extend(jobs)
            
            # Check if we've reached the last page by looking for pagination
            pagescount = soup.select_one("#pagescount")
            if pagescount and page >= int(pagescount.get('value', '1')):
                print(f"Reached last page ({page}). Stopping pagination.")
                break
        
        print(f"Found {len(all_jobs)} total jobs on RAA School")
        return all_jobs
    
    def _extract_jobs_from_soup(self, soup):
        "Extract job listings from the page HTML"
        jobs = []
        job_cards = soup.select(".card")
        
        for card in job_cards:
            job = {}
            
            # Extract title
            title_elem = card.select_one(".card-title")
            if title_elem: job['title'] = title_elem.text.strip()
            
            # Extract all card text elements
            card_texts = card.select(".card-text")
            if card_texts and len(card_texts) > 0:
                # First card-text contains details
                details_text = card_texts[0].get_text()
                
                # Extract details using string operations
                for line in details_text.split('\n'):
                    line = line.strip()
                    if 'Establishment:' in line: job['employer_name'] = line.split('Establishment:')[1].strip()
                    if 'Location:' in line: job['displayLocation'] = line.split('Location:')[1].strip()
                    if 'Salary:' in line: job['salary_description'] = line.split('Salary:')[1].strip()
                    if 'Department:' in line: job['department'] = line.split('Department:')[1].strip()
                    if 'Job Type:' in line: job['contractTypes'] = line.split('Job Type:')[1].strip()
                    if 'Closing Date:' in line: job['closing_date'] = line.split('Closing Date:')[1].strip()
                    if 'Ref:' in line: job['reference'] = line.split('Ref:')[1].strip()
            
            # Extract description from second card-text if it exists
            if len(card_texts) > 1: job['shortDescription'] = card_texts[1].text.strip()
            
            # Extract URL
            url_elem = card.select_one("a.btn-primary")
            if url_elem and url_elem.has_attr('href'): job['url'] = url_elem['href']
            
            # Add source identifier
            job['source'] = 'RAA School'
            
            # Only add non-empty jobs
            if job and 'title' in job: jobs.append(job)
        
        print(f"Found {len(jobs)} jobs on page")
        return jobs
    
    def normalize(self, raw_data):
        "Convert RAA School data to standard format"
        if not raw_data: return pd.DataFrame()
        df = pd.DataFrame(raw_data)
        
        # Current time
        now = pd.Timestamp.now()
        
        # Parse closing date
        if 'closing_date' in df.columns:
            df['application_closeDate'] = pd.to_datetime(df['closing_date'], format='%d/%m/%Y %H:%M', errors='coerce')
            df['application_closeDate_formatted'] = df['closing_date']
        
        # Calculate days to apply
        if 'application_closeDate' in df.columns and not df['application_closeDate'].isna().all():
            df['days_to_apply'] = (df['application_closeDate'] - now).dt.days
        
        # Format URLs
        if 'url' in df.columns:
            df['full_url'] = df['url'].apply(lambda x: f"https://raaschool.face-ed.co.uk{x}" if x.startswith('/') else x)
        
        # Set status (we don't have advert_startDate, so use a placeholder)
        df['status'] = 'current'
        
        # Set contract terms (permanent/temporary)
        if 'contractTypes' in df.columns:
            df['contractTerms'] = 'Permanent'  # Default value based on observed data
            
        return df

class DunottarJobSource(JobSource):
    "Dunottar School job source implementation"
    def __init__(self, keywords=None):
        super().__init__(keywords=keywords)
        self.base_url = "https://www.dunottarschool.com/about-us/vacancies/"
        
    def search(self):
        "Search Dunottar School and return raw data"
        print(f"Fetching jobs from Dunottar School: {self.base_url}")
        response = httpx.get(self.base_url)
        soup = bs(response.text, "html.parser")
        return self._extract_jobs_from_soup(soup)
    
    def _extract_jobs_from_soup(self, soup):
        "Extract job listings from the page HTML"
        jobs = []
        vacancy_listings = soup.select("a.vacancy-listing")
        
        for listing in vacancy_listings:
            job = {}
            
            # Extract title
            title_elem = listing.select_one(".vacancy-listing-title")
            if title_elem: job['title'] = title_elem.text.strip()
            
            # Extract URL
            job['url'] = listing.get('href', '')
            
            # Extract details
            details = listing.select(".vacancy-listing-detail")
            for detail in details:
                label = detail.select_one(".vacancy-listing-label")
                if not label: continue
                
                label_text = label.text.strip().lower()
                value_text = detail.get_text().replace(label.get_text(), '').strip()
                
                if 'closing date' in label_text: job['closing_date'] = value_text
                elif 'salary' in label_text: job['salary_description'] = value_text
                elif 'location' in label_text: job['displayLocation'] = value_text
                
            # Extract hours and basis from description if available
            desc_elem = listing.select_one(".listing-desc")
            if desc_elem:
                job['shortDescription'] = desc_elem.get_text().strip()
                # Parse hours and basis from description using text search instead of CSS selectors
                for p_tag in desc_elem.find_all('p'):
                    p_text = p_tag.get_text().strip()
                    if 'Hours :' in p_text:
                        job['hours'] = p_text.replace('Hours :', '').strip()
                    if 'Basis :' in p_text:
                        job['contractTypes'] = p_text.replace('Basis :', '').strip()
            
            # Add employer name and source
            job['employer_name'] = 'Dunottar School'
            job['source'] = 'Dunottar School'
            
            # Only add non-empty jobs
            if job and 'title' in job: jobs.append(job)
        
        print(f"Found {len(jobs)} jobs on Dunottar School")
        return jobs
    
    def normalize(self, raw_data):
        "Convert Dunottar School data to standard format"
        if not raw_data: return pd.DataFrame()
        df = pd.DataFrame(raw_data)
        
        # Current time
        now = pd.Timestamp.now()
        
        # Convert closing date to datetime - format might be "11th May 2025"
        if 'closing_date' in df.columns:
            # Try to parse the date, but with error handling for different formats
            df['application_closeDate'] = pd.to_datetime(df['closing_date'], errors='coerce')
            df['application_closeDate_formatted'] = df['closing_date']
        
        # Calculate days to apply
        if 'application_closeDate' in df.columns and not df['application_closeDate'].isna().all():
            df['days_to_apply'] = (df['application_closeDate'] - now).dt.days
        
        # URLs are already complete in this case
        if 'url' in df.columns:
            df['full_url'] = df['url']
        
        # Set status as current
        df['status'] = 'current'
        
        # Set contract terms based on contractTypes
        if 'contractTypes' in df.columns:
            df['contractTerms'] = df['contractTypes'].apply(
                lambda x: 'Permanent' if 'permanent' in str(x).lower() else 
                         ('Temporary' if 'temporary' in str(x).lower() else 'Not specified')
            )
            
        return df
    
class WoldinghamJobSource(JobSource):
    "Woldingham School job source implementation"
    def __init__(self, keywords=None):
        super().__init__(keywords=keywords)
        self.base_url = "https://www.woldinghamschool.co.uk/vacancies.html"
        
    def search(self):
        "Search Woldingham School and return raw data"
        print(f"Fetching jobs from Woldingham School: {self.base_url}")
        response = httpx.get(self.base_url)
        soup = bs(response.text, "html.parser")
        return self._extract_jobs_from_soup(soup)
    
    def _extract_jobs_from_soup(self, soup):
        "Extract job listings from the page HTML"
        jobs = []
        job_titles = ["Housemistress", "Chaplain", "Head of French"]
        
        for title in job_titles:
            title_elem = soup.find(string=title)
            if title_elem:
                job = {'title': title}
                parent = title_elem.parent
                while parent and parent.name != 'body':
                    text = parent.get_text()
                    if 'Start Date:' in text or 'Start date:' in text:
                        for line in text.split('\n'):
                            line = line.strip()
                            if line.startswith('Start Date:') or line.startswith('Start date:'): 
                                job['start_date'] = line.split(':', 1)[1].strip()
                            elif 'Salary:' in line: 
                                job['salary_description'] = line.split('Salary:', 1)[1].strip()
                            elif 'close at 09.00am on' in line:
                                import re
                                match = re.search(r'close at 09\.00am on (.+?)\.', line)
                                if match: job['closing_date'] = match.group(1)
                        break
                    parent = parent.parent
                
                job.update({'employer_name': 'Woldingham School', 'displayLocation': 'Woldingham, Surrey', 
                        'source': 'Woldingham School', 'url': self.base_url})
                jobs.append(job)
        
        print(f"Found {len(jobs)} jobs on Woldingham School")
        return jobs

    def normalize(self, raw_data):
        "Convert Woldingham School data to standard format"
        if not raw_data: return pd.DataFrame()
        df = pd.DataFrame(raw_data)
        
        now = pd.Timestamp.now()
        
        if 'closing_date' in df.columns:
            df['application_closeDate'] = pd.to_datetime(df['closing_date'], errors='coerce')
            df['application_closeDate_formatted'] = df['closing_date']
        
        if 'application_closeDate' in df.columns and not df['application_closeDate'].isna().all():
            df['days_to_apply'] = (df['application_closeDate'] - now).dt.days
        
        if 'url' in df.columns: df['full_url'] = df['url']
        
        df['status'] = 'current'
        df['contractTypes'] = 'Permanent'
        df['contractTerms'] = 'Permanent'
        
        return df

class SuttonHighJobSource(JobSource):
    "Sutton High School job source implementation via TES embed"
    def __init__(self, keywords=None):
        super().__init__(keywords=keywords)
        self.base_url = "https://www.tes.com/jobs/search/embed/1039809"
        
    def search(self):
        "Search Sutton High School jobs and return raw data"
        params = dict(keywords=self.keywords or "", frameHeight=742, frameWidth=1244)
        url = f"{self.base_url}?{urlencode(params)}"
        print(f"Fetching jobs from Sutton High School: {url}")
        response = httpx.get(url)
        soup = bs(response.text, "html.parser")
        return self._extract_jobs_from_soup(soup)
    
    def _extract_jobs_from_soup(self, soup):
        "Extract job listings from the embedded TES page"
        jobs = []
        job_titles = soup.select("h3.tds-job-card__content-title")
        
        for title_elem in job_titles:
            job = {'title': title_elem.text.strip()}
            
            card = title_elem.find_parent()
            while card and 'job-card' not in card.get('class', []): card = card.find_parent()
            
            if card:
                link = card.select_one("a[href*='/jobs/']")
                if link: job['url'] = link.get('href')
                
                text = card.get_text()
                if 'Apply by' in text:
                    import re
                    date_match = re.search(r'Apply by (\d{1,2} \w+ \d{4})', text)
                    if date_match: job['closing_date'] = date_match.group(1)
                
                if '£' in text:
                    salary_match = re.search(r'£[\d,]+(?:\.\d{2})?(?:\s*-\s*£[\d,]+(?:\.\d{2})?)?', text)
                    if salary_match: job['salary_description'] = salary_match.group(0)
            
            job.update({'employer_name': 'Sutton High School', 'displayLocation': 'Sutton', 'source': 'Sutton High School'})
            jobs.append(job)
        
        print(f"Found {len(jobs)} jobs on Sutton High School")
        return jobs

        
    def normalize(self, raw_data):
        "Convert Sutton High School data to standard format"
        if not raw_data: return pd.DataFrame()
        df = pd.DataFrame(raw_data)
        now = pd.Timestamp.now()
        
        if 'closing_date' in df.columns:
            df['application_closeDate'] = pd.to_datetime(df['closing_date'], errors='coerce')
            df['application_closeDate_formatted'] = df['closing_date']
            df['days_to_apply'] = (df['application_closeDate'] - now).dt.days
        
        if 'url' in df.columns:
            df['full_url'] = df['url'].apply(lambda x: f"https://www.tes.com{x}" if x.startswith('/') else x)
        
        df['status'] = 'current'
        return df

class GdstJobSource(JobSource):
    "Girls Day School Trust job source implementation"
    def __init__(self, keywords=None):
        super().__init__(keywords=keywords)
        self.base_url = "https://www.gdst.net/careers/vacancies/"
        
    def search(self):
        "Search GDST jobs and return raw data"
        print(f"Fetching jobs from GDST: {self.base_url}")
        response = httpx.get(self.base_url)
        soup = bs(response.text, "html.parser")
        return self._extract_jobs_from_soup(soup)
    
    def _extract_jobs_from_soup(self, soup):
        "Extract job listings from GDST page"
        jobs = []
        vacancy_container = soup.select_one('.js-vacancies-container')
        if not vacancy_container: return jobs
        
        job_items = vacancy_container.select('.cell')
        
        for item in job_items:
            job = {}
            # Extract title (h2 inside media-block__content)
            title_elem = item.select_one('h2.media-block__text')
            if title_elem: job['title'] = title_elem.text.strip()
            
            spans = item.select('.media-block__content span')
            if len(spans) > 1: job['employer_name'] = spans[1].text.strip()
            if len(spans) > 2 and 'Closing date:' in spans[2].text:
                job['closing_date'] = spans[2].text.replace('Closing date:', '').strip()
            # Extract URL
            link_elem = item.select_one('a.media-block__button')
            if link_elem: job['url'] = link_elem.get('href')
            
            job['source'] = 'GDST'
            if job.get('title'): jobs.append(job)
        print(f"Found {len(jobs)} jobs on GDST")
        jobs = [j for j in jobs if "croydon" in j['employer_name'].lower() or "sutton" in j['employer_name'].lower()]
        print(f"Found {len(jobs)} jobs on GDST for Croydon and Sutton")
        return jobs
    
    def normalize(self, raw_data):
        "Convert GDST data to standard format"
        if not raw_data: return pd.DataFrame()
        df = pd.DataFrame(raw_data)
        now = pd.Timestamp.now()
        
        if 'closing_date' in df.columns:
            df['application_closeDate'] = pd.to_datetime(df['closing_date'], errors='coerce')
            df['application_closeDate_formatted'] = df['closing_date']
            df['days_to_apply'] = (df['application_closeDate'] - now).dt.days
        
        if 'url' in df.columns: df['full_url'] = df['url']
        df['status'] = 'current'
        return df

def standardize_column_names(df, source_type):
    "Rename columns to standard format based on source type"
    if source_type == 'tes':
        return df.rename(columns={
            'title': 'title',
            'employer_name': 'employer_name',
            'displayLocation': 'location',
            'contractTypes': 'contract_type',
            'contractTerms': 'contract_term',
            'salary_description': 'salary',
            'application_closeDate_formatted': 'closing_date',
            'days_to_apply': 'days_remaining',
            'shortDescription': 'description',
            'full_url': 'url_',
            'source': 'source'
        })
    
    elif source_type == 'gov':
        return df.rename(columns={
            'title': 'title',
            'employer_name': 'employer_name',
            'displayLocation': 'location',
            'working_pattern': 'contract_type',
            'school_type': 'employer_type',
            'salary_description': 'salary',
            'application_closeDate_formatted': 'closing_date',
            'days_to_apply': 'days_remaining',
            'full_url': 'url_',
            'source': 'source'
        })
    
    elif source_type == 'raa':
        return df.rename(columns={
            'title': 'title',
            'employer_name': 'employer_name',
            'displayLocation': 'location',
            'contractTypes': 'contract_type',
            'contractTerms': 'contract_term',
            'salary_description': 'salary',
            'application_closeDate_formatted': 'closing_date',
            'days_to_apply': 'days_remaining',
            'shortDescription': 'description',
            'full_url': 'url_',
            'source': 'source'
        })
    
    elif source_type == 'dunottar':
        return df.rename(columns={
            'title': 'title',
            'employer_name': 'employer_name',
            'displayLocation': 'location',
            'contractTypes': 'contract_type',
            'contractTerms': 'contract_term',
            'salary_description': 'salary',
            'application_closeDate_formatted': 'closing_date',
            'days_to_apply': 'days_remaining',
            'shortDescription': 'description',
            'full_url': 'url_',
            'source': 'source'
        })
    elif source_type == 'woldingham':
        return df.rename(columns={
            'title': 'title',
            'employer_name': 'employer_name',
            'displayLocation': 'location',
            'contractTypes': 'contract_type',
            'contractTerms': 'contract_term',
            'salary_description': 'salary',
            'application_closeDate_formatted': 'closing_date',
            'days_to_apply': 'days_remaining',
            'shortDescription': 'description',
            'full_url': 'url_',
            'source': 'source'
        })
    elif source_type in ['suttonhigh', 'gdst']:
        return df.rename(columns={
            'title': 'title',
            'employer_name': 'employer_name',
            'displayLocation': 'location',
            'contractTypes': 'contract_type',
            'contractTerms': 'contract_term',
            'salary_description': 'salary',
            'application_closeDate_formatted': 'closing_date',
            'days_to_apply': 'days_remaining',
            'shortDescription': 'description',
            'full_url': 'url_',
            'source': 'source'
        })
    return df

def generate_master_email_content(dfs_dict, max_jobs_per_source=10):
    "Generate complete HTML email content with all job sources"
    today = datetime.now().strftime('%d %B %Y at %H:%M')
    
    # Search URLs for each source and notes
    search_urls = {
        'TES': "https://www.tes.com/jobs/search?keywords=Design+and+Technology+Teacher&displayLocation=CR5+1SS",
        'GOV.UK': "https://teaching-vacancies.service.gov.uk/jobs?keyword=Design+and+technology&location=The+Glade%2C+Coulsdon+CR5+1SS",
        'RAA School': "https://raaschool.face-ed.co.uk/vacancies",
        'Dunottar School': "https://www.dunottarschool.com/about-us/vacancies/",
        'Woldingham School': "https://www.woldinghamschool.co.uk/vacancies.html",
        'Sutton High School': "https://www.tes.com/jobs/search/embed/1039809",
        'GDST': "https://www.gdst.net/careers/vacancies/"
    }
    
    source_notes = {
        'RAA School': "Note: Only jobs with design and technology words in title or description are displayed.",
        'Dunottar School': "Note: Only jobs with design and technology words in title or description are displayed.",
        'Woldingham School': "Note: Only jobs with design and technology words in title or description are displayed.",
        'Sutton High School': "Note: Only jobs with design and technology words in title or description are displayed.",
        'GDST': "Note: Shows jobs from Sutton High School and Croydon High School only."
    }
    
    # Start building HTML content
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 900px; margin: 0 auto; padding: 20px; }}
            h1 {{ color: #2c3e50; border-bottom: 2px solid #eee; padding-bottom: 10px; }}
            h2 {{ color: #3498db; margin-top: 30px; }}
            h3 {{ color: #2c3e50; margin-top: 25px; border-bottom: 1px solid #eee; padding-bottom: 5px; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 10px; margin-bottom: 30px; }}
            th, td {{ text-align: left; padding: 8px; }}
            th {{ background-color: #3498db; color: white; }}
            tr:nth-child(even) {{ background-color: #f2f2f2; }}
            tr:hover {{ background-color: #ddd; }}
            .job-title {{ font-weight: bold; }}
            .source-section {{ margin-bottom: 40px; }}
            .summary {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
            .search-link {{ margin: 5px 0; }}
            .search-link a {{ color: #3498db; text-decoration: none; }}
            .search-link a:hover {{ text-decoration: underline; }}
            .note {{ font-style: italic; color: #666; margin-top: 5px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Teaching Job Listings</h1>
            <p>Search performed on {today}</p>
            
            <div class="summary">
                <h2>Summary</h2>
                <ul>
    """
    
    # Add summary counts with search links
    total_jobs = 0
    for source_name, df in dfs_dict.items():
        count = len(df)
        total_jobs += count
        html += f"""<li>
            <strong>{source_name}:</strong> {count} jobs found
            <div class="search-link"><a href="{search_urls[source_name]}" target="_blank">View all on {source_name} →</a></div>
        </li>"""
    
    html += f"""
                <li><strong>Total:</strong> {total_jobs} jobs found</li>
                </ul>
            </div>
    """
    
    # Add each source section
    for source_name, df in dfs_dict.items():
        html += f'<div class="source-section">'
        html += f'<h3>{source_name} Jobs ({len(df)} found)</h3>'
        
        # Add note for specific sources
        if source_name in source_notes:
            html += f'<div class="note">{source_notes[source_name]}</div>'
            
        # Add search link
        html += f"""<div class="search-link">
            <a href="{search_urls[source_name]}" target="_blank">View all on {source_name} →</a>
        </div>"""
        
        # Add table
        html += generate_email_content_for_source(df, source_name, max_jobs_per_source)
        html += '</div>'
    
    # Close the HTML
    html += """
        </div>
    </body>
    </html>
    """
    
    return html

def generate_email_content_for_source(df, source_name, max_jobs=20):
    "Generate HTML table for a specific job source"
    if df.empty: return "<p>No jobs found</p>"
    
    # Limit to top jobs
    df_display = df.head(min(len(df), max_jobs))
    
    # Start building HTML content
    html = f"""
    <table style="border-collapse: collapse; width: 100%; margin-top: 10px;">
        <tr style="background-color: #3498db; color: white;">
            <th style="text-align: left; padding: 8px;">Job Title</th>
            <th style="text-align: left; padding: 8px;">School</th>
            <th style="text-align: left; padding: 8px;">Location</th>
            <th style="text-align: left; padding: 8px;">Contract</th>
            <th style="text-align: left; padding: 8px;">Salary</th>
            <th style="text-align: left; padding: 8px;">Closing Date</th>
            <th style="text-align: left; padding: 8px;">Link</th>
        </tr>
    """
    
    # Add rows for each job
    for i, (_, job) in enumerate(df_display.iterrows()):
        # Alternate row colors
        bg_color = "#f2f2f2" if i % 2 == 0 else "#ffffff"
        
        # Handle missing values
        title = job.get('title', 'Not specified')
        employer = job.get('employer_name', 'Not specified')
        location = job.get('location', job.get('displayLocation', 'Not specified'))
        contract = f"{job.get('contract_term', '')} {job.get('contract_type', '')}".strip() or 'Not specified'
        salary = job.get('salary', job.get('salary_description', 'Not specified'))
        closing = job.get('closing_date', job.get('application_closeDate_formatted', 'Not specified'))
        
        # Fix URL issue by ensuring we get a string, not a Series
        url = job.get('url_', job.get('full_url', '#'))
        if hasattr(url, 'iloc') and len(url) > 0:  # It's a Series
            url = url.iloc[-1] if isinstance(url.iloc[-1], str) else '#'
        
        # Build the row
        html += f"""
        <tr style="background-color: {bg_color};">
            <td style="padding: 8px; font-weight: bold;">{title}</td>
            <td style="padding: 8px;">{employer}</td>
            <td style="padding: 8px;">{location}</td>
            <td style="padding: 8px;">{contract}</td>
            <td style="padding: 8px;">{salary}</td>
            <td style="padding: 8px;">{closing if not isinstance(closing, pd.Series) else closing.iloc[0]}</td>
            <td style="padding: 8px;"><a href="{url}" target="_blank">View Job</a></td>
        </tr>
        """
    
    # Close the table
    html += "</table>"
    
    return html

def main(keywords="Design and Technology Teacher", distance=10, max_pages=2, 
         to_emails=None, send_email=True):
    """Main function to orchestrate the entire job search and email process"""
    print(f"Starting job search at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Searching for: {keywords} within {distance} miles")
    
    # Create source instances
    sources = {
        'TES': TesJobSource(keywords=keywords, distance=distance, max_pages=max_pages),
        'GOV.UK': GovJobSource(keywords=keywords.replace(' Teacher', ''), distance=distance),
        'RAA School': RaaJobSource(max_pages=3),
        'Dunottar School': DunottarJobSource(),
        'Woldingham School': WoldinghamJobSource(),
        'Sutton High School': SuttonHighJobSource(),
        'GDST': GdstJobSource()
    }
    
    # Fetch and standardize jobs from each source
    all_jobs = {}
    total_jobs = 0
    
    for source_name, source in sources.items():
        print(f"\nFetching jobs from {source_name}...")
        try:
            # Get jobs and standardize column names
            jobs_df = source.get_jobs()
            std_df = standardize_column_names(jobs_df, source_name.lower().replace(' ', '').replace('.', ''))
            
            # Store in our dictionary
            all_jobs[source_name] = std_df
            job_count = len(std_df)
            total_jobs += job_count
            print(f"Successfully fetched {job_count} jobs from {source_name}")
            
        except Exception as e:
            print(f"Error fetching jobs from {source_name}: {str(e)}")
            all_jobs[source_name] = pd.DataFrame()  # Empty dataframe as fallback
    
    print(f"\nTotal jobs found across all sources: {total_jobs}")
    
    # Generate email content
    print("\nGenerating email content...")
    email_html = generate_master_email_content(all_jobs)
    
    # Determine if we should send email or just save to file
    if send_email and total_jobs > 0:
        # Get recipient email addresses
        recipients = to_emails or os.environ.get("EMAIL_RECIPIENTS", "").split(",")
        if not recipients or recipients == [""]:
            print("No recipients specified. Set to_emails parameter or EMAIL_RECIPIENTS env var.")
            # Save to file as fallback
            with open("job_listings.html", "w") as f:
                f.write(email_html)
            print("Email content saved to job_listings.html")
            return
        
        # Send email
        print(f"Sending email to {len(recipients)} recipient(s)...")
        subject = f"Design Technology Teaching Jobs - {total_jobs} positions found"
        
        try:
            send_email_to_recipients(
                html_content=email_html,
                subject=subject,
                to_emails=recipients
            )
            print("Email sent successfully!")
        except Exception as e:
            print(f"Error sending email: {str(e)}")
            # Save to file as fallback
            with open("job_listings.html", "w") as f:
                f.write(email_html)
            print("Email content saved to job_listings.html")
    else:
        # Save to file
        with open("job_listings.html", "w") as f:
            f.write(email_html)
        print("Email content saved to job_listings.html")
    
    print(f"Job search completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return all_jobs

def send_email_to_recipients(html_content, subject, to_emails):
    """Send HTML email with job listings via Gmail"""
    # Get credentials from environment variables
    username = os.environ.get("GMAIL_USER")
    password = os.environ.get("GMAIL_PASS")
    
    if not username or not password:
        raise ValueError("GMAIL_USER and GMAIL_PASS environment variables must be set")
    
    # Create message
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = username
    msg["To"] = ", ".join(to_emails) if isinstance(to_emails, list) else to_emails
    
    # Attach HTML content
    msg.attach(MIMEText(html_content, "html"))
    
    # Send email via Gmail
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(username, password)
        server.send_message(msg)
    
    return True

if __name__ == "__main__": main()




