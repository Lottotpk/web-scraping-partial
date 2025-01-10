from datetime import datetime
from pathlib import Path
import re

def clearAllTabsExceptMain(driver):
    main_window = driver.window_handles[0]
    window_list = driver.window_handles
    for i in range(1, len(window_list)):
        driver.switch_to.window(window_list[i])
        driver.close()
    driver.switch_to.window(main_window)

def convertDate(date):
    try:
        parsed_date = datetime.strptime(date, "%b %d, %Y %I:%M %p")
        return parsed_date.strftime("%Y-%m-%d")
    except ValueError:
        today = datetime.today()
        return today.strftime("%Y-%m-%d")
    
def sanitize_title(filename):
    sanitized_filename = re.sub(r'[<>:"/\\|?*]', "", filename)
    return sanitized_filename.replace(" ", "")

def renameDownloadedFile(download_path, filename, versionId):
    dir = Path(Path(download_path) / filename)
    newDir = Path(Path(download_path) / (versionId + ".pdf"))
    dir.rename(newDir)