import urllib.request
from tqdm import tqdm
import minio


# Configuration for MinIO
MINIO_ENDPOINT = "minio:9000"  # Replace with your MinIO endpoint
MINIO_ACCESS_KEY = "minioadmin"  # Replace with your MinIO access key
MINIO_SECRET_KEY = "minioadmin"  # Replace with your MinIO secret key
MINIO_BUCKET_NAME = "raw"  # Replace with your MinIO bucket name
MINIO_DATA_SUS_FOLDER = "datasus/dbc"  # Folder within the bucket for DataSUS files

def download_dbc_file(uf, ano, mes, client):
    """Downloads a single .dbc file from the DataSUS FTP server and saves it to MinIO.

    Args:
        uf (str): The state code (e.g., 'CE').
        ano (str): The year (e.g., '24').
        mes (str): The month (e.g., '01').
        client (minio.Minio): The MinIO client object.
    """
    url = f'ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/RD{uf}{ano}{mes}.dbc'
    filename = f'RD{uf}{ano}{mes}.dbc'

    # Download the file using urllib
    urllib.request.urlretrieve(url, filename)

    # Upload to MinIO
    client.fput_object(
        MINIO_BUCKET_NAME,
        f"{MINIO_DATA_SUS_FOLDER}/{filename}",  # Use the designated folder
        filename,
        content_type='application/octet-stream'
    )

    # Delete the local file
    import os
    os.remove(filename)

def download_dbc_files_for_dates():
    uf = 'CE'
    dates = ['2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01']
    """Downloads .dbc files for specified dates and saves them to MinIO.

    Args:
        uf (str): The state code (e.g., 'CE').
        dates (list): A list of dates in 'YYYY-MM-DD' format.
    """
    # Initialize MinIO client
    client = minio.Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # Adjust based on your MinIO configuration
    )

    # Check if bucket exists, create if not
    if not client.bucket_exists(MINIO_BUCKET_NAME):
        client.make_bucket(MINIO_BUCKET_NAME)

    for date in tqdm(dates):
        ano, mes, _ = date.split('-')
        ano = ano[-2:]  # Extract the last two digits of the year
        download_dbc_file(uf, ano, mes, client)

download_dbc_files_for_dates()