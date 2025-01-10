from setup import cleanCompanyId, driverInit, getAuthentication
from editPageNavigate import getFromDateBox, getToDateBox, clickSave, setDate, setCompany, setCountry, setList,clearSettings, selectDateRange
from mainPageNavigate import clickSavedSearch, clickQuickSearch, clickEdit, clickAdvancedSearch, clickRun
from downloadScraping import companyFilter, downloadFiles, scrapeTable, getTable, singleDownload, selectRows, extractVersionId, extractCompanyId
from formatHandler import clearAllTabsExceptMain, convertDate, sanitize_title
from fileManage import unZip, removeHTML, moveToDir, extractFolder, removeDownloadingZip, toPdf
from progressTrack import checkDownloaded, updateDataFrame
from s3Redshift import create_sqlalchemy_connection, create_wrangler_connection, get_snowflake_companyid, write_to_redshift, \
                        write_to_s3, allPdfS3, get_missing_report_df
from emailNotif import send_email_to_analyst
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementNotInteractableException
from selenium.webdriver.common.by import By
from pathlib import Path
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed
from datetime import datetime, timedelta
import pandas as pd
import time
import logging
import boto3
import ast
import os
import traceback
from botocore.exceptions import ClientError
import random
from warnings import filterwarnings

load_dotenv()
filterwarnings("ignore", category = UserWarning, message = ".*pandas only supports SQLAlchemy connectable.*")


def get_secret():

    secret_name = os.getenv("CAPIQ_USER") # TODO: Each analyst will have their own secret path on secrets manager. Probably use env var for this since each process might use a different set of credentials
    region_name = "ap-southeast-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    auth = ast.literal_eval(get_secret_value_response['SecretString'])
    username = auth['username']
    password = auth['password']
    return username, password


def mainDDownload(driver, engine, conn, wrconn, s3, download_path, country, countryId):
    primary_key = os.getenv("primary_keys").strip("][").split(", ")
    dtype = ast.literal_eval(os.getenv("dtype"))
    listId = os.getenv("company_list")
    wait = WebDriverWait(driver, 5)
    time.sleep(3)
    clickEdit(driver)
    clearSettings(driver)
    setCountry(driver, [countryId])
    setList(driver, [listId])
    # selectDateRange(driver)
    setDate(driver, (datetime.today() - timedelta(2)).strftime("%m/%d/%Y"), datetime.today().strftime("%m/%d/%Y"))
    print(f"Sleep for", os.getenv("delay"), "second(s)")
    time.sleep(int(os.getenv("delay")))
    clickSave(driver)
    page = 1
    download_count = 0

    while True:
        while True:
            try:
                WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.NAME, "_gridSection$Displaysection1$_gV$_gridView$ctl02$ResearchGridViewCheckBox")))
            except TimeoutException:
                soup = BeautifulSoup(driver.page_source, "html.parser")
                no_results_td = soup.find('td', colspan="16", string="No results found. Please modify your search.")
                if no_results_td:
                    print("No results found. Stopping the function.")
                    break
                else:
                    print("Timeout, refreshing the page")
                    driver.refresh()
                    continue
            page += 1
            soup = BeautifulSoup(driver.page_source, "html.parser")
            table = soup.find("table", id = "_gridSection_Displaysection1__gV__gridView")
            check = soup.find_all("td", {"class" : "cColHeaderRegularTxt"})
            
            for td in check:
                span = td.find("span", {"class" : "cLabel"})
                if span and span.text.strip() == "Watch Lists:":
                    confirm = td.text.split(":")[-1].strip()
                    print("Filtered country (confirmation):", confirm)
                    if confirm != country + " Investible":
                        print("There is a problem in filtering the country. Filter country again")
                        driver.quit()
                        time.sleep(15)
                        raise Exception("Country filtered does not match the environment configuration")
                    break
                if span and span.text.strip() == "Geographies:":
                    confirm = td.text.split(":")[-1].strip()
                    print("Filtered country (confirmation):", confirm)
                    if (confirm != "Korea, South" and country != "South Korea") and confirm != country:
                        print("There is a problem in filtering the country. Filter country again")
                        driver.quit()
                        time.sleep(15)
                        raise Exception("Country filtered does not match the environment configuration")
                    break

            tr_elements = table.find_all("tr")
            for i in range(2, len(tr_elements)-1):
                rowEntry = scrapeTable(tr_elements, i)
                # Skip report from Citi Group
                if rowEntry[0].text.strip() == "Citigroup Inc":
                    continue
                company = rowEntry[3].text.strip()
                snowflake_companyid = -1
                # Check if the company is in our universe
                if company != "":
                    company_url = "https://capitaliq.com" + rowEntry[3].find("a")["href"]
                    snowflake_companyid = extractCompanyId(company_url)
                    snowflake_companyid = get_snowflake_companyid(conn, snowflake_companyid, country)
                    if len(snowflake_companyid) != 0:
                        snowflake_companyid = int(snowflake_companyid[0])
                    else:
                        continue
                # Check if the file is pdf
                if 'formatType=4&' not in rowEntry[len(rowEntry) - 1].find("a")["href"]:
                    continue
                value = rowEntry[6].text.strip()
                # Check if the page is a positive number
                if not value.isdigit() or int(value) < 1:
                    continue
                entry = []
                for j, cell in enumerate(rowEntry):
                    if j == 2:
                        date = cell.text.strip().split("\n")[0]
                        date = convertDate(date)
                        entry.append(date)
                    elif j == 3:
                        entry.append(snowflake_companyid)
                        entry.append(country)
                        # look up company id
                    elif j != len(rowEntry) - 1:
                        entry.append(cell.text.strip())
                    else:
                        download_url = "https://capitaliq.com" + cell.find("a")["href"] + "#"
                        versionId = extractVersionId(download_url)

                        query = f"""
                            SELECT *
                            FROM report.broker_research
                            WHERE country = '{country}' AND versionid = '{versionId}';
                        """
                        df_version = pd.read_sql_query(query, con = conn.connection)
                        if len(df_version.index) != 0:
                            print("Reached downloaded record")
                            return download_count

                        print(f"Download {versionId}.pdf")
                        singleDownload(driver, download_path, download_url, versionId)
                        entry.append(str(versionId))
                        entry.append(f"{country}/{snowflake_companyid}/{versionId}.pdf")
                entry = pd.DataFrame([entry], columns = ["contributor", "analyst", "date_published", "snowflake_companyid", "country", 
                                                         "headline", "report_type", "pages", "versionid", "path_to_S3"])
                versionId = entry["versionid"][0]
                path_to_S3 = entry["path_to_S3"][0]
                flag = toPdf(download_path)
                if not flag:
                    # Handle conversion errors (delete the file if necessary)
                    Path(download_path).unlink(missing_ok=True)
                    continue
                entry = removeHTML(download_path, entry)
                if len(entry) == 0:
                    continue
                # country_dir = Path(download_path) / country
                # moveToDir(download_path, Path(country_dir) / (str(snowflake_companyid) + "/")) # this for local machine
                local_file_path = Path(download_path) / f"{versionId}.pdf"
                with open(local_file_path, "rb") as f:
                    write_to_s3(s3, f, path_to_S3)
                local_file_path.unlink()
                download_count += 1
                csv_location = Path(download_path) / "BrokerResearch.csv"
                old_df = pd.DataFrame(columns = ["contributor", "analyst", "date_published", "snowflake_companyid", "country", 
                                                "headline", "report_type", "pages", "versionid", "path_to_S3"])
                if Path(csv_location).exists():
                    old_df = pd.read_csv(csv_location, encoding = "utf-16")
                    # print("Reading existing csv")
                new_df = pd.concat([old_df, entry])
                updateDataFrame(download_path, "BrokerResearch.csv", new_df, True)
            df_path = Path(download_path) / "BrokerResearch.csv"
            if Path(df_path).exists():
                df = pd.read_csv(df_path, encoding = "utf-16")
                print(df)
                write_to_redshift(df, dtype, "report", "broker_research", primary_key)
                Path(df_path).unlink()
            try:
                wait.until(
                    EC.presence_of_element_located((By.XPATH, "//a[contains(@href, 'Page$" + str(page) + "')]"))
                ).click()
                print("Changing page....")
                WebDriverWait(driver, 300).until(
                    EC.presence_of_element_located((By.XPATH, "//a[contains(@href, 'Page$" + str(page-1) + "')]"))
                )
            except TimeoutException:
                print("Finished this section. Finding the next arrow '>'")
                break
        try:
            wait.until(
                EC.element_to_be_clickable((By.XPATH, "//a[contains(@href, '__doPostBack') and text()='>']"))
            ).click()
            print("Clicking next...")
            WebDriverWait(driver, 300).until(
                EC.invisibility_of_element_located((By.XPATH, "//a[contains(@href, 'Page$" + str(page-2) + "')]"))
            )
        except TimeoutException:
            break
    return download_count

def downloadMissingReport(driver, conn, s3, download_path, no_report):
    # Step 1: Read the DataFrame
    time.sleep(3)
    missing_report_df = get_missing_report_df(conn).head(no_report)  # Modify as needed
    download_count = 0

    # Step 3: Loop through each row in the DataFrame
    for index, row in missing_report_df.iterrows():
        versionId = row["versionid"]
        download_url = "https://capitaliq.com" + row['path_to_s3'] + "#"
        
        print(f"Downloading {versionId}.pdf")

        # Step 3: Download the file
        local_file_path = Path(download_path) / f"{versionId}.pdf"
        try:
            # Assuming singleDownload performs the download
            singleDownload(driver, download_path, download_url, versionId)
            
            # Step 4: Modify the `path_to_s3` column
            path_to_S3 = f"{row['country']}/{row['snowflake_companyid']}/{row['versionid']}.pdf"
            row["path_to_s3"] = path_to_S3
            
            # Step 4: Write the row to S3 (after file conversion)
            flag = toPdf(download_path)
            if not flag:
                # Handle conversion errors (delete the file if necessary)
                Path(download_path).unlink(missing_ok=True)
                continue
            
            # Upload the file to S3
            with open(local_file_path, "rb") as f:
                write_to_s3(s3, f, path_to_S3)
                
            # Remove the local file after upload
            local_file_path.unlink()

            # Step 5: Append the modified row to the CSV file
            entry = pd.DataFrame([row])  # Convert the single row to a DataFrame
            
            # Append the row to the CSV (or create it if it doesn't exist)
            csv_location = Path(download_path) / "BrokerResearch.csv"
            entry.to_csv(csv_location, encoding="utf-16", mode='a', index=False, header=not csv_location.exists())

            # Increment the download count
            download_count += 1

        except Exception as e:
            print(f"Error downloading {versionId}: {e}")
            continue  # Skip to the next file if any error occurs

    print(f"Total files downloaded: {download_count}")
    return download_count

# @retry(wait = wait_fixed(10), stop = stop_after_attempt(5), reraise = True)
def main():
    download_path = os.getenv("download_path_daily")
    url = os.getenv("url")
    country_list = ast.literal_eval(os.getenv("country_list"))
    dtype = ast.literal_eval(os.getenv("dtype"))
    boto3.setup_default_session(region_name="ap-southeast-1")
    session = boto3.Session()
    s3 = session.resource('s3')
    driver = driverInit(url, download_path)
    engine, conn = create_sqlalchemy_connection()
    wrconn = create_wrangler_connection()
    usr, pw = get_secret()
    getAuthentication(driver, usr, pw)
    # getAuthentication(driver, os.getenv("capiq_username"), os.getenv("capiq_password"))
    try:
        clickSavedSearch(driver)
    except ElementNotInteractableException:
        print("Already reached saved search")
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "onetrust-reject-all-handler"))).click()
    driver.maximize_window()
    time.sleep(5)
    start_time = time.time()

    num_report = dict()
    total_download = 0
    for country, countryId in country_list.items():
        if country != 'Missing':
            print("COUNTRY: ", country)
            num_report[country] = mainDDownload(driver, engine, conn, wrconn, s3, download_path, country, countryId)
            total_download += num_report[country]
    random_number = int(os.getenv("download_count")) - random.randint(0, 10) + 5
    no_report = random_number - total_download
    if no_report > 0:
        print(f'Downloading additinal {no_report} missing reports')
        num_report['missing'] = downloadMissingReport(driver, conn, s3, download_path, no_report)
        total_download += num_report['missing']
    df_path = Path(download_path) / "BrokerResearch.csv"
    if Path(df_path).exists():
        df = pd.read_csv(df_path, encoding = "utf-16")
        print(df)
        write_to_redshift(df, dtype, "report", "broker_research", os.getenv("primary_keys").strip("][").split(", "))
        Path(df_path).unlink()
    elasped = time.time() - start_time
    print(f"Number of Files: {total_download}.")
    print(f"Download finished for today's reports. Time elasped: {elasped}.")
    return num_report, total_download, elasped


def handler(event, context):
    recipients = os.getenv("main_email").strip("][").split(", ")
    sender = os.getenv("sender_email")
    cc = os.getenv("cc_email").strip("][").split(", ")
    broker = os.getenv("BROKER_LIST")
    SUBJECT = "[COMPLETED] Daily Broker Research Alert"
    try:
        num_report, total_download, elasped = main()
        msg = f"""<html>
            <head></head>
            <body>
                <p>Your daily broker research download script has completed today.</p>
                <p>Summary:</p>
                <p>Time elapsed: {round(elasped // 60)} minutes {round(elasped % 60)} seconds</p>
                <p>{total_download} reports are downloaded in this process</p>
            """
        msg += "<ul>"
        for country, num in num_report.items():
            SUBJECT += f": {country} ({broker})"
            # msg += f"<li>{country}: {num} ({round(num*100/total_download, 2)}%)</li>"
        msg += "</ul>"
        msg += "</body>\n</html>"

    except Exception as e:
        country_list = ast.literal_eval(os.getenv("country_list"))
        for country, tmp in country_list.items():
            SUBJECT = f"[ERROR] Daily Broker Research Alert: {country} ({broker})"
        msg = f"""
            <p>An error occurred in your broker research download script:</p>
            <p>Please check the ColudWatch for the error messages</p>
        """
        traceback.print_exc()
    send_email_to_analyst(msg, SUBJECT)
    return {
            'statusCode': 200,
            'body': 'Completed'
        }

if __name__ == "__main__":
    main()