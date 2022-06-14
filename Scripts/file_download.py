import pandas as pd
import zipfile
import os
from alive_progress import alive_bar
import time
from pathlib import Path
from paramiko import Transport, SFTPClient, RSAKey
from socket import gaierror, timeout
from server_info import host, port, user


cwd = Path(__file__).resolve()
parent_dir = Path(__file__).resolve().parent
origin_dir = Path(parent_dir).resolve().parent

jai_kern_download_dir = os.path.join(origin_dir, "Downloads/jai.kern/")
stp_kern_download_dir = os.path.join(origin_dir, "Downloads/stp.kern.user/")

jai_kern_save_dir = os.path.join(origin_dir, "Datasets/")
stp_kern_save_dir = os.path.join(origin_dir, "Datasets/")


def data_download(sftp_con, download_dir):
    if download_dir == jai_kern_download_dir:
        sftp_con.chdir("../jai.kern/")
        pb_title = "New Sensors Data"
    else:
        sftp_con.chdir("../stp.kern.user/")
        pb_title = "Old Sensors Data"

    sftp_file_list = set(sftp_con.listdir(path='.'))

    try:
        local_files = set(os.listdir(download_dir))
        new_files_list = list(sftp_file_list-local_files)
    except (ValueError,IndexError):
        print(f"No local files found for: {pb_title}. Downloading all files.")
        new_files_list = sftp_file_list    

    if download_dir == jai_kern_download_dir:
        new_files_list[:] = [file for file in new_files_list if ".zip" in file]
    else:
        new_files_list[:] = [file for file in new_files_list if ".csv" in file]

    new_file_count = len(new_files_list)

    if new_file_count > 0:
        with alive_bar(new_file_count, title=f'Files: {pb_title}') as bar:
            for i in range(new_file_count):
                file = new_files_list[i]
                sftp_con.get(file,os.path.join(download_dir, file))
                time.sleep(.005)
                bar()
    else:
        print(f"No new files to download for: {pb_title}.")

    return None


def create_sftp_con():

    key_file_path = (os.path.join(parent_dir, "sftp.kern"))
    key = RSAKey.from_private_key_file(key_file_path)

    con = Transport(host, port)
    con.connect(None,username=user, pkey=key)
    sftp_con = SFTPClient.from_transport(con)

    print("Connection successful !")
    return sftp_con


if __name__ == "__main__":
    print("\nTrying to establish connection to sftp ... ")
    
    try:
        sftp_con = create_sftp_con()
        for download_dir in [jai_kern_download_dir, stp_kern_download_dir]:
            data_download(sftp_con, download_dir)
        print("Download Complete. Closing Connection.\n\n")
        sftp_con.close()
    except socket.gaierror as e:
        print(f"Connection is not found. \nPossible internet connection/hostname/sftp issue!")
    except socket.timeout as t:
        print(f"Timeout Issue!")



