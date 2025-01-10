from zipfile import ZipFile
from pathlib import Path
from s3Redshift import write_to_s3
import glob
import subprocess
import sys
import tempfile

def unZip(path_to_zip):
    path = Path(path_to_zip) / "Investment Research.zip"
    with ZipFile(path, "r") as zip_ref:
        zip_ref.extractall(path_to_zip)
    path.unlink()

def removeHTML(dir, country_df):
    locationHTML = Path(dir)
    for ext in ["*.html", "*.htm"]:
        for f in locationHTML.glob(ext):
            versionId = f.name.strip(ext)
            country_df.drop(country_df[country_df["versionid"] == versionId].index, inplace = True)
            f.unlink()
    return country_df

def toPdf(dir):
    libre_path = r"C:\Program Files\LibreOffice\program\swriter.exe" # window's path
    if sys.platform.startswith("linux"):
        libre_path = "/opt/libreoffice7.6/program/soffice" # only aws lambda (based on linux)
    flag = True
    for ext in ["*.docx", "*.xlsx"]:
        for f in Path(dir).glob(ext):
            try:
                temp_dir = tempfile.TemporaryDirectory()
                temp_dir_path = temp_dir.name
                cmdStr = [libre_path, "--headless", "--convert-to", "pdf", "--outdir", str(dir), str(f)]
                out = subprocess.call(cmdStr, env = {"HOME": temp_dir_path})
                if out == 0:
                    print("Converted to PDF successfully")
                else:
                    print(f"Error code {out}")
                    flag = False
                f.unlink()
            except Exception as e:
                print(f"An error occurred: {e}")
                flag = False
    return flag

# windows only
# def docxToPdf(download_path):
#     flag = True
#     word = win32com.client.Dispatch("Word.Application")
#     try:
#         for f in Path(download_path).glob("*.docx"):
#             pdf_path = f.with_suffix(".pdf")
#             doc = word.Documents.Open(str(f))
#             doc.SaveAs(str(pdf_path), FileFormat=17) 
#             doc.Close()
#             f.unlink() 
#     except Exception as e:
#         print(f"An error occurred: {e}")
#         flag = False
#     finally:
#         word.Quit()
#     return flag

# def xlsxToPdf(download_path):
#     flag = True
#     excel = win32com.client.gencache.EnsureDispatch("Excel.Application")
#     try:
#         for f in Path(download_path).glob("*.xlsx"):
#             pdf_path = Path(download_path) / (f.name.strip(".xlsx") + ".pdf")
#             wb = excel.Workbooks.Open(str(f))
#             wb.ExportAsFixedFormat(0, str(pdf_path))
#             wb.Close()
#             f.unlink()
#     except Exception as e:
#         print(f"An error occurred: {e}")
#         flag = False
#     finally:
#         excel.Quit()
#     return flag

def extractFolder(main_folder, sub_folder):
    main = Path(main_folder)
    sub = Path(sub_folder)
    for f in sub.iterdir():
        dst = main / f.name
        f.rename(dst)
    sub.rmdir()

def moveToDir(src, dst):
    dst = Path(dst)
    if not dst.exists():
        dst.mkdir(parents = True, exist_ok = True)
    for f in Path(src).glob("*.pdf"):
        if not Path(dst / f.name).exists():
            f.rename(dst / f.name)
        else:
            f.unlink()

def removeDownloadingZip(dir, filename):
    dirZip = Path(dir) / (filename + ".zip")
    if Path(dirZip).exists():
        dirZip.unlink()
    i = 1
    dirZip = Path(dir) / (filename + " (" + str(i) + ").zip")
    while Path(dirZip).exists():
        dirZip.unlink()
        i += 1
        dirZip = Path(dir) / (filename + " (" + str(i) + ").zip")
