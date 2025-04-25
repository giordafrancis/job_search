#!/usr/bin/env python3
import pandas as pd
import httpx
from bs4 import BeautifulSoup as bs
from urllib.parse import urlencode
from datetime import datetime
import json
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def extract_job_data(job):
    """Extract and flatten relevant job data from a job listing"""
    result = {}
    for k in ['id', 'title', 'promoted', 'shortDescription', 'displayLocation', 'quickApply']:
        if k in job: result[k] = job[k]
    if 'contractTerms' in job: result['contractTerms'] = ', '.join(job['contractTerms'])
    if 'contractTypes' in job: result['contractTypes'] = ', '.join(job['contractTypes'])
    if 'employer' in job and 'name' in job['employer']: 
        result['employer_name'] = job['employer']['name']
    if 'salary' in job:
        if job['salary'].get('description'): result['salary_description'] = job['salary']['description']
        if job['salary'].get('range'): result['salary_range'] = job['salary']['range']
    if 'advert' in job:
        if 'startDate' in job['advert']: result['advert_startDate'] = job['advert']['startDate']
        if 'endDate' in job['advert']: result['advert_endDate'] = job['advert']['endDate']
    if 'application' in job and 'closeDate' in job['application']:
        result['application_closeDate'] = job['application']['closeDate']
    if 'canonicalUrl' in job: result['url'] = job['canonicalUrl']
    return result

def extract_page_data(url):
    """Extract job data from a TES jobs page"""
    response = httpx.get(url)
    soup = bs(response.text, "html.parser")
    next_data_script = soup.find('script', id='__NEXT_DATA__')
    if not next_data_script: return None
    json_data = json.loads(next_data_script.string)
    try:
        return json_data['props']['pageProps']['trpcState']['json']['queries'][0]['state']['data']
    except (KeyError, IndexError):
        return None

def extract_all_jobs(page_data):
    """Extract all jobs from page data"""
    if not page_data or 'jobs' not in page_data: return []
    return [extract_job_data(job) for job in page_data['jobs']]

def get_all_jobs(keywords, distance=10, sort_by="distance", max_pages=2):
    """Get all jobs across multiple pages with pagination"""
    location = "CR5 1SS"
    lat = "51.30662208651764"
    lon = "-0.1133822439545745"
    params = {
        "keywords": keywords,
        "displayLocation": location,
        "lat": lat,
        "lon": lon,
        "distance": distance,
        "distanceUnit": "mi",
        "sort": sort_by}
    base_url = "https://www.tes.com/jobs/search"
    all_jobs = []
    total_found = 0
    
    for page in range(1, max_pages + 1):
        params["page"] = page
        url = f"{base_url}?{urlencode(params)}"
        print(f"Fetching page {page}")        
        page_data = extract_page_data(url)
        if not page_data or 'jobs' not in page_data or not page_data['jobs']:
            print(f"No more jobs found on page {page}")
            break
        page_jobs = extract_all_jobs(page_data)
        all_jobs.extend(page_jobs)
        if page == 1: total_found = page_data.get('numFound', 0)
        print(f"Found {len(page_jobs)} jobs on page {page}")
        if len(page_jobs) < 20: break
    
    jobs_df = pd.DataFrame(all_jobs)
    print(f"Total jobs collected: {len(jobs_df)} out of {total_found} available")
    return jobs_df

def add_job_status_flags(jobs_df):
    """Add status flags for job recency and application deadline"""
    df = jobs_df.copy()
    df['advert_startDate'] = pd.to_datetime(df['advert_startDate'])
    df['application_closeDate'] = pd.to_datetime(df['application_closeDate'])
    now = pd.Timestamp.utcnow()
    seven_days_ago = now - pd.Timedelta(days=7)
    df['status'] = df['advert_startDate'].apply(lambda x: 'recent' if x >= seven_days_ago else 'older')
    df['days_to_apply'] = (df['application_closeDate'] - now).dt.days
    return df

def add_full_url_prefix(jobs_df):
    """Add 'https://www.tes.com' prefix to job URLs and handle missing URLs"""
    df = jobs_df.copy()
    df['full_url'] = df['url'].apply(
        lambda x: f"https://www.tes.com{x}" if pd.notna(x) else "No URL available"
    )
    return df

def format_datetime_columns(jobs_df):
    """Convert datetime columns to a more human-readable format"""
    df = jobs_df.copy()
    datetime_columns = ['advert_startDate', 'application_closeDate']
    for col in datetime_columns:
        if col in df.columns:
            df[f'{col}_formatted'] = df[col].dt.strftime('%d %B %Y, %H:%M')
    if 'advert_endDate' in df.columns:
        df['advert_endDate'] = pd.to_datetime(df['advert_endDate'])
        df['advert_endDate_formatted'] = df['advert_endDate'].dt.strftime('%d %B %Y, %H:%M')
    return df

def generate_email_content(jobs_df, columns_to_show, log_content="", max_jobs=10, distance=10):
    """Generate HTML email content with job listings"""
    from urllib.parse import urlencode
    
    # Limit to top jobs (closest first, already sorted)
    top_jobs = jobs_df.head(min(len(jobs_df), max_jobs))
    
    # Create the search URL that users can click to see all results
    base_url = "https://www.tes.com/jobs/search"
    search_params = {
        "keywords": "Design and Technology Teacher",
        "displayLocation": "CR5 1SS",
        "lat": "51.30662208651764",
        "lon": "-0.1133822439545745",
        "distance": distance,
        "distanceUnit": "mi",
        "sort": "distance",
        "page": 1
    }
    search_url = f"{base_url}?{urlencode(search_params)}"
    
    # Start building HTML content
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 800px; margin: 0 auto; padding: 20px; }}
            h1 {{ color: #2c3e50; border-bottom: 2px solid #eee; padding-bottom: 10px; }}
            h2 {{ color: #3498db; margin-top: 30px; }}
            pre {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; overflow: auto; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
            th, td {{ text-align: left; padding: 12px; }}
            th {{ background-color: #3498db; color: white; }}
            tr:nth-child(even) {{ background-color: #f2f2f2; }}
            tr:hover {{ background-color: #ddd; }}
            .recent {{ background-color: #e8f4f8; border-left: 4px solid #3498db; }}
            .urgent {{ background-color: #fff8e8; border-left: 4px solid #f39c12; }}
            .job-title {{ font-weight: bold; color: #2c3e50; }}
            .apply-by {{ font-weight: bold; color: #e74c3c; }}
            .search-link {{ margin-top: 20px; margin-bottom: 20px; }}
            .search-link a {{ color: #3498db; text-decoration: none; font-weight: bold; }}
            .search-link a:hover {{ text-decoration: underline; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Design Technology Teacher Job Listings</h1>
            <p>Search performed on {datetime.now().strftime('%d %B %Y at %H:%M')}, based on your home postcode and jobs within {distance} mi radius</p>
            
            <div class="search-link">
                <a href="{search_url}" target="_blank">View all results on TES Jobs →</a>
            </div>
            
            <h2>A total of {len(top_jobs)} jobs found (Sorted by Closest First)</h2>
    """
    
    # Add table with job listings
    html += """
            <table>
                <tr>
                    <th>Job Title</th>
                    <th>Location</th>
                    <th>School</th>
                    <th>Contract</th>
                    <th>Salary</th>
                    <th>Apply By</th>
                    <th>Link</th>
                </tr>
    """
    
    # Add rows for each job
    for _, job in top_jobs.iterrows():
        # Determine row class based on status
        row_class = "recent" if job['status'] == 'recent' else ""
        
        # Build the row
        html += f"""
                <tr class="{row_class}">
                    <td class="job-title">{job['title']}</td>
                    <td>{job['displayLocation']}</td>
                    <td>{job['employer_name']}</td>
                    <td>{job['contractTerms']} / {job['contractTypes']}</td>
                    <td>{job['salary_range'] if pd.notna(job['salary_range']) else job['salary_description'] if pd.notna(job['salary_description']) else 'Not specified'}</td>
                    <td class="apply-by">{job['application_closeDate_formatted']}</td>
                    <td><a href="{job['full_url']}" target="_blank">View Job</a></td>
                </tr>
        """
    
    # Close the table and HTML
    html += f"""
            </table>
            <p><small>Recent jobs are highlighted in blue. Jobs are sorted by distance (closest first).</small></p>
            <div class="search-link">
                <a href="{search_url}" target="_blank">View all results on TES Jobs →</a>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html

def send_email(html_content, subject="Design Technology Job Listings", 
               from_email=None, to_emails=None):
    """Send HTML email with job listings via Gmail"""
    # Get credentials from environment variables
    username = os.environ.get("GMAIL_USER", from_email)
    password = os.environ.get("GMAIL_PASS", "")
    
    # Default recipient if none provided
    if not to_emails:
        to_emails = ["your_email@example.com"]
    elif isinstance(to_emails, str):
        to_emails = [to_emails]
        
    # Use username as from_email if not specified
    from_email = from_email or username
    
    # Create message
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = ", ".join(to_emails)
    
    # Attach HTML content
    msg.attach(MIMEText(html_content, "html"))
    
    # Send email via Gmail
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(username, password)
        server.send_message(msg)
        print(f"Email sent to {len(to_emails)} recipients")
        
    return True

def search_design_tech_jobs(distance=10, max_pages=2):
    """Search for Design Technology teaching jobs and process results"""
    all_jobs = get_all_jobs("Design and Technology Teacher", distance=distance, max_pages=max_pages)
    jobs_with_flags = add_job_status_flags(all_jobs)
    jobs_with_urls = add_full_url_prefix(jobs_with_flags)
    jobs_with_formatted_dates = format_datetime_columns(jobs_with_urls)
    return jobs_with_formatted_dates

def main():
    """Main function to run the job search and email process"""
    # Define recipient email addresses
    recipients = os.environ.get("EMAIL_RECIPIENTS", "").split(",")
    if not recipients or recipients == [""]:
        print("No recipients specified in EMAIL_RECIPIENTS env var")
        return
    
    # Define columns to show in the email
    columns_to_show = ['title', 'shortDescription', 'displayLocation',
                      'quickApply', 'contractTerms', 'contractTypes', 'employer_name',
                      'salary_description', 'salary_range', 'status', 'days_to_apply',
                      'full_url', 'advert_startDate_formatted', 'application_closeDate_formatted']
    
    # Run job search
    print(f"Starting job search at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    jobs_df = search_design_tech_jobs(distance=10, max_pages=2)
    
    # Check if jobs were found
    if len(jobs_df) == 0:
        print("No jobs found")
        return
    
    # Generate email content
    html_content = generate_email_content(jobs_df, columns_to_show, distance=10)
    
    # Send email
    send_email(
        html_content=html_content,
        subject="Design Technology Teacher Jobs Near You",
        to_emails=recipients
    )
    
    print(f"Job search completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
