from pathlib import Path
import pandas as pd
import time

def checkDownloaded(download_path, filename, timeout):
    dir = Path(download_path) / filename
    start = time.time()
    
    while time.time() - start < timeout:
        if Path(dir).exists():
            return True
        time.sleep(3)
    print("Cannot detect the downloading file. Trying to click download again.")
    return False

def updateDataFrame(storage_path, filename, df : pd.DataFrame, utf16 = False):
    dir = Path(storage_path) / filename
    if Path(dir).exists():
        Path(dir).unlink()
    if utf16:
        df.to_csv(dir, encoding = "utf-16", index = False)
    else:
        df.to_csv(dir, index = False)
