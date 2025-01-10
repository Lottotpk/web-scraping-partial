from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import ElementClickInterceptedException
from selenium import webdriver
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import datetime
import time
import pandas as pd

from formatHandler import clearAllTabsExceptMain, convertDate, renameDownloadedFile

def extractVersionId(url):
    parsed = urlparse(url)
    query_parsed = parse_qs(parsed.query)
    return query_parsed.get("versionId", [None])[0]

def extractCompanyId(url):
    parsed = urlparse(url)
    query_parsed = parse_qs(parsed.query)
    return query_parsed.get("companyId", [None])[0]

def companyFilter(driver, company):
    wait = WebDriverWait(driver, 30)
    try:
        filter_company = wait.until(
            EC.element_to_be_clickable((By.ID, "_gridSection_Displaysection1__gV__gridView_ctl02_companyFilterIcon_filterIcon"))
        )
        filter_company.click()
    except TimeoutException:
        print("No reports found by not being able to filter")
        return False
    while True:
        try:
            filter_bar = wait.until(
                EC.visibility_of_element_located((By.ID, "_gridSection_Displaysection1__gV_companyPopUpFilter_txtFilterOptions"))
            )
            break
        except TimeoutException:
            filter_company.click()
            time.sleep(3)
    filter_bar.clear()
    filter_bar.send_keys(company)
    time.sleep(1)
    try:
        WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.XPATH, "//img[@src='/CIQDOTNET/images/ico_dataItemPicker_additems.png']"))
        ).click()
    except TimeoutException:
        print("No reports found")
        return False
    wait.until(
        EC.element_to_be_clickable((By.ID, "_gridSection_Displaysection1__gV_companyPopUpFilter_companyPopUpFilter_applyButton"))
    ).click()
    return True

def downloadFiles(driver, ALL, listIdx = []):
    wait = WebDriverWait(driver, 30)
    time.sleep(5)
    if ALL:
        checkBox = wait.until(
            EC.element_to_be_clickable((By.NAME, "_gridSection$Displaysection1$_gV$_gridView$ctl02$ResearchGridViewCheckBox"))
        )
        while True:
            try:
                checkBox.click()
                break
            except ElementClickInterceptedException:
                print("Found an error. Trying to click the checkbox again...")
                time.sleep(3)
                continue
    else:
        for idx in listIdx:
            if idx < 9:
                wait.until(
                    EC.presence_of_element_located((By.NAME, "_gridSection$Displaysection1$_gV$_gridView$ctl0" + str(idx + 1) +"$ResearchGridViewCheckBox"))
                ).click()
            else:
                wait.until(
                    EC.presence_of_element_located((By.NAME, "_gridSection$Displaysection1$_gV$_gridView$ctl" + str(idx + 1) + "$ResearchGridViewCheckBox"))
                ).click()
    
    wait.until(
        EC.presence_of_element_located((By.ID, "_gridSection_Displaysection1__gV__batchDownload"))
    ).click()
    wait.until(
        EC.presence_of_element_located((By.XPATH, "//div[@class='listTypeOptionFrame']//span[text()='Download as ZIP file']"))
    ).click()
    time.sleep(10)
    clearAllTabsExceptMain(driver)

def scrapeTable(tr_elements, i):
    return [tr_elements[i].find("td", class_ = "contributor-column"),
                    tr_elements[i].find("td", class_ = "analyst-column"),
                    tr_elements[i].find("td", class_ = "date-column"),
                    tr_elements[i].find("td", class_ = "company-column"),
                    tr_elements[i].find("td", class_ = "headline-column"),
                    tr_elements[i].find("td", class_ = "doctype-column"),
                    tr_elements[i].find("td", class_ = "pages-column"),
                    tr_elements[i].find("td", class_ = "docimg-column"),
            ]

def getTable(driver, download_path, country, snowflake_companyid, ALL, research_df, listIdx = [], seq = False):
    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = soup.find("table", id = "_gridSection_Displaysection1__gV__gridView")
    tr_elements = table.find_all("tr")
    # citiIdx = []
    allEntries = []
    if ALL:
        listIdx = range(2, len(tr_elements)-1)

    for i in listIdx:
        rowEntry = scrapeTable(tr_elements, i)
        if rowEntry[0].text.strip() == "Citigroup Inc":
            continue
        entry = []
        success = True
        for j, cell in enumerate(rowEntry):
            if j == 2:
                date = cell.text.strip().split("\n")[0]
                date = convertDate(date)
                entry.append(date)
            elif j == 3:
                entry.append(snowflake_companyid)
                entry.append(country)
            elif j != len(rowEntry) - 1:
                entry.append(cell.text.strip())
            else:
                download_url = "https://capitaliq.com" + cell.find("a")["href"] + "#"
                versionId = extractVersionId(download_url)
                if seq:
                    success = singleDownload(driver, download_path, download_url, versionId)
                entry.append(int(versionId))
                entry.append(f"{country}/{snowflake_companyid}/{versionId}.pdf") 
        # if entry[0] == "Citigroup Inc": # handle citigroup's reports only
        #     citiIdx.append(i - 2)
        #     download_url = "https://capitaliq.com" + cell.find("a")["href"]
        #     driver.switch_to.new_window("tab")
        #     driver.get(download_url)
        #     driver.switch_to.window(driver.window_handles[0])
        if success:
            allEntries.append(entry)
    time.sleep(5)
    # downloadCiti(driver, wait, allEntries, citiIdx, download_path)
    clearAllTabsExceptMain(driver)
    df = pd.DataFrame(allEntries, columns = ["contributor", "analyst", "date_published", "snowflake_companyid", "country", 
                                             "headline", "report_type", "pages", "versionid", "path_to_S3"])
    BrokerResearch_df = pd.concat([research_df, df])
    return BrokerResearch_df

def singleDownload(driver, download_path, download_url, versionId):
    timeout = 30
    dir1 = Path(Path(download_path) / (versionId + ".pdf"))
    dir2 = Path(Path(download_path) / (versionId + ".html"))
    dir3 = Path(Path(download_path) / (versionId + ".htm"))
    dir4 = Path(Path(download_path) / (versionId + ".docx"))
    dir5 = Path(Path(download_path) / (versionId + ".xlsx"))
    attempt = 0

    while not (dir1.exists() or dir2.exists() or dir3.exists() or dir4.exists() or dir5.exists()):
        attempt += 1
        start = time.time()
        driver.switch_to.new_window("tab")
        driver.get(download_url)
        while time.time() - start < timeout:
            if dir1.exists() or dir2.exists() or dir3.exists() or dir4.exists() or dir5.exists():
                clearAllTabsExceptMain(driver)
                return True
            time.sleep(3)
        if attempt == 4:
            print("Download link is corrupted. Skip this record")
            return False
        print("Timeout. Attempting to redownload")
        clearAllTabsExceptMain(driver)

def selectRows(driver, non_specific = True):
    WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By.TAG_NAME, "table")))
    listIdx = []
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = soup.find("table", id = "_gridSection_Displaysection1__gV__gridView")
    tr_elements = table.find_all("tr")
    for i in range(2, len(tr_elements)-1):
        rowEntry = scrapeTable(tr_elements, i)
        if (non_specific and rowEntry[3].text.strip() != "") or rowEntry[0].text.strip() == "Citigroup Inc":
            continue
        listIdx.append(i)
    return listIdx
