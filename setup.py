from pathlib import Path
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import pandas as pd
from tempfile import mkdtemp

def cleanCompanyId(folder_path, filename, country_list):
    df = pd.read_excel(Path(folder_path) / filename)
    df[df["listingcountry"].isin(country_list)].to_csv("companyid_cleaned.csv", index = False)
    print("The company file has been filtered to only interested countries")

def driverInit(url, download_path):
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs",{
        "plugins.always_open_pdf_externally" : True,
        "profile.default_content_setting_values.automatic_downloads": 1,
        "download.default_directory" : download_path,
        "browser.download.dir" : download_path,
        "directory_upgrade" : True
    })
    options.binary_location = "/opt/chrome/stable/chrome"
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--single-process")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-dev-tools")
    # options.add_argument("--no-zygote")
    # options.add_argument(f"--user-data-dir={mkdtemp()}")
    # options.add_argument(f"--data-path={mkdtemp()}")
    # options.add_argument(f"--disk-cache-dir={mkdtemp()}")
    # options.add_argument("--remote-debugging-pipe")
    # options.add_argument("--verbose")
    # options.add_argument("--log-path=/tmp/")

    driver = webdriver.Chrome(options = options, service = Service("/opt/chromedriver/stable/chromedriver"))
    # driver = webdriver.Chrome(options = options)
    driver.get(url)
    print("Driver Initialized")
    return driver

def getAuthentication(driver, username, password):
    wait = WebDriverWait(driver, 10)
    wait.until(
        EC.presence_of_element_located((By.ID, "input28"))
    ).send_keys(username)
    wait.until(
        EC.presence_of_element_located((By.CLASS_NAME, "button-primary"))
    ).click()
    wait.until(
        EC.presence_of_element_located((By.ID, "input60"))
    ).send_keys(password)
    wait.until(
        EC.presence_of_element_located((By.CLASS_NAME, "button-primary"))
    ).click()