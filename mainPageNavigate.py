from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

def clickSavedSearch(driver):
    wait = WebDriverWait(driver, 20)
    wait.until(
        EC.presence_of_element_located((By.ID, "_researchFilterTabs__searchTabs__saved_tabLink"))
    ).click()

def clickQuickSearch(driver):
    wait = WebDriverWait(driver, 20)
    wait.until(
        EC.presence_of_element_located((By.ID, "_researchFilterTabs__searchTabs__quick_tabLink"))
    ).click()

def clickAdvancedSearch(driver):
    wait = WebDriverWait(driver, 20)
    wait.until(
        EC.presence_of_element_located((By.ID, "_researchFilterTabs__searchTabs__advanced_tabLink"))
    ).click()

def clickRun(driver):
    wait = WebDriverWait(driver, 20)
    wait.until(
        EC.presence_of_element_located((By.ID, "_section2_ct3__savedSearchFilterControl__runButton"))
    ).click()

def clickEdit(driver):
    wait = WebDriverWait(driver, 20)
    wait.until(
        EC.presence_of_element_located((By.ID, "_section2_ct3__savedSearchFilterControl__editButton"))
    ).click()