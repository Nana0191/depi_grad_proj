
import requests
import shutil
import sys  # use this libaray to recieve the date
from pathlib import Path
from datetime import datetime
from tempfile import mkdtemp  #to make it temporary file
#use this as a temporary place when we are installing an old batch and replace the new one with it
#but if sm bad occured while gathering we donot want to delete the old batch and we didnot get the new one
#so we save the batch here before we replace it with new old one

#some config
OWNER = "mennaebied15"
REPO = "olist-batched-data"
BRANCH = "main"

OUTPUT_DIR = Path("/app/data/gathered_data")  #here where the data will be 
#this will be a volume in the docker image so the transform job can look at this volume and see the data gathered 
#so now this path will be linked with the volume in docker

EXPECTED_FILES = {
    "customers.csv",
    "orders.csv",
    "order_items.csv",
    "products.csv"
}

def download_csv_files(batch_date):
    try:
        datetime.strptime(batch_date, "%Y-%m-%d")
    except ValueError:   #have the data in specific format
        raise ValueError("batch_date must be in YYYY-MM-DD format")

    api_url = (
        f"https://api.github.com/repos/"
        f"{OWNER}/{REPO}/git/trees/{BRANCH}?recursive=1"
    )

    response = requests.get(api_url, timeout=30)
    response.raise_for_status()

    files = response.json()["tree"]  #get all the files that is in the batch

    batch_files = [  #what data should we get
        item for item in files
        if item["type"] == "blob"  #the data type must be in file
        and Path(item["path"]).parent == Path(batch_date)  #the file path is inside the batch folder itself
        and Path(item["path"]).name in EXPECTED_FILES  #the files we get should be from the files we expect to get nothing else
    ]

    found_files = {Path(item["path"]).name for item in batch_files}

    #just make sure that the files exists
    missing_files = EXPECTED_FILES - found_files

    #we should always get the 4 files together if someone is missing it will raise and error
    if missing_files:
        raise FileNotFoundError(
            f"Batch {batch_date} is incomplete. Missing: {missing_files}"
        )

    OUTPUT_DIR.parent.mkdir(parents=True, exist_ok=True)

    temp_dir = Path(
        mkdtemp(prefix="gathered_data_", dir=OUTPUT_DIR.parent)
    )  #we will make the temp folder as we said before so we
       #save the old batch inside it before replaceing it in the folder with the new one

    backup_dir = OUTPUT_DIR.with_name("gathered_data_backup")

    try:
        for item in batch_files: #here we are looking for the files itself and get their url and download them

            download_url = (
                f"https://raw.githubusercontent.com/"
                f"{OWNER}/{REPO}/{BRANCH}/{item['path']}"
            )

            result = requests.get(download_url, timeout=60)
            result.raise_for_status()

            #here we save the files in the gatherd folder with the batch data as folder name
            destination = temp_dir / batch_date / Path(item["path"]).name
            destination.parent.mkdir(parents=True, exist_ok=True)

            destination.write_bytes(result.content)

        #here we make sure the files we just downloaded are the ones we are expected
        downloaded_files = {
            file.name for file in (temp_dir / batch_date).iterdir()
        }

        if downloaded_files != EXPECTED_FILES:
            raise RuntimeError("Downloaded batch files do not match expected files")

        #here we do the overwrite if we have data that is old we replace it with the new one
        #here we just move the old data in the temp folder the backup folder and if the process of gathering the new data is
        #successful we then delete this temp folder
        if backup_dir.exists():
            shutil.rmtree(backup_dir)

        if OUTPUT_DIR.exists():
            OUTPUT_DIR.rename(backup_dir)

        #here we are moving the old batch to the temp folder
        try:
            temp_dir.rename(OUTPUT_DIR)
        except Exception:
            if backup_dir.exists():
                backup_dir.rename(OUTPUT_DIR)
            raise

        if backup_dir.exists():  #here there is a success in gathering the new batch so we delete the old one
            shutil.rmtree(backup_dir)

    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)


if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise SystemExit("Usage: python gathering.py YYYY-MM-DD")

    download_csv_files(sys.argv[1])   #here it recieve the date from airflow
