from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time

def getFromDateBox(driver):
    wait = WebDriverWait(driver, 10)
    fromBox = wait.until(
        EC.presence_of_element_located((By.ID, "_dateRange_myFromBox"))
    )
    fromBox.clear()
    return fromBox

def getToDateBox(driver):
    wait = WebDriverWait(driver, 10)
    toBox = wait.until(
        EC.presence_of_element_located((By.ID, "_dateRange_myToBox"))
    )
    toBox.clear()
    return toBox

def clickSave(driver):
    cur = driver.current_url
    wait = WebDriverWait(driver, 10)
    wait.until(
        EC.presence_of_element_located((By.ID, "_savetop__savesearch"))
    ).click()
    time.sleep(1)
    wait.until(
        EC.presence_of_element_located((By.ID, "_savetop_float_mps__savecancel__saveBtn"))
    ).click()

    timeout = 120
    start = time.time()
    while time.time() - start < timeout:
        if driver.current_url != cur:
            print("Main page arrived")
            return
        time.sleep(3)
    print(f"Take too long to reach main page (Timeout: {timeout} seconds)")
    raise TimeoutError("Timeout")

def setDate(driver, fromDate, toDate):
    getFromDateBox(driver).send_keys(fromDate)
    getToDateBox(driver).send_keys(toDate)

def setCompany(driver, company):
    wait = WebDriverWait(driver, 30)
    wait.until(
        EC.presence_of_element_located((By.ID, "_company__company__entitySearch_searchbox"))
    ).send_keys(company)
    wait.until(
        EC.presence_of_element_located((By.CLASS_NAME, "entitysearch-search"))
    ).click()
    time.sleep(5)

def setCountry(driver, countryId):
    js_code = """document.getElementById('_country__geographyTree_myTree_TreeValues').value ='"""
    for id in countryId:
        js_code += id + ","
    js_code = js_code.rstrip(",")
    js_code += "';"
    driver.execute_cdp_cmd("Runtime.evaluate", {
        "expression": js_code,
        "awaitPromise": True,
        "returnByValue": True
    })

def setList(driver, countryId):
    js_code = """document.getElementById('_company__entitySearch_selList_sortHi').value ='"""
    for id in countryId:
        js_code += id + ","
    js_code = js_code.rstrip(",")
    js_code += "';"
    driver.execute_cdp_cmd("Runtime.evaluate", {
        "expression": js_code,
        "awaitPromise": True,
        "returnByValue": True
    })

def clearSettings(driver):
    wait = WebDriverWait(driver, 30)
    wait.until(
        EC.element_to_be_clickable((By.XPATH, "//a[@href=\"javascript:ClearTickers(EntitySearch__company__entitySearch, document.getElementById('_company__entitySearch_selList_sortHi'));\"]"))
    ).click()
    wait.until(
        EC.alert_is_present()
    ).accept()
    js_code = """document.getElementById('_country__geographyTree_myTree_TreeValues').value='';""" 
    driver.execute_cdp_cmd("Runtime.evaluate", {
        "expression": js_code,
        "awaitPromise": True,
        "returnByValue": True
    })

def selectDateRange(driver):
    wait = WebDriverWait(driver, 10)
    dateRange = wait.until(EC.presence_of_element_located((By.ID, "_dateRange_PeriodMenu")))
    dateRange.click()
    dateRange.send_keys("Last 24 Hours")