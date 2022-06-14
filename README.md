# kern_files_download_update
This repo contains code for connecting to AWS sftp servers, downloading some files from different directories, cleaning, merging and saving them in a local directory to use merged files in the Power BI. The whole process is can be done by running a batch file on windows.

# Project structure
```
Kern_File_Update
├── Datasets
│   ├── dataset_1.csv
│   └── dataset_2.csv
├── Downloads
│   ├── directory_1
|      ├── downloaded_file_1
|      └── downloaded_file_2
│   ├── directory_2
|      ├── downloaded_file_1
|      └── downloaded_file_2
├── Scripts
|    ├── venv_folder
|    ├── 1. update_file.bat
|    ├── 2. download_and_update_files.bat
|    ├── file_download.py
|    ├── file_update.py
|    ├── requirements.txt
|    ├── server_info.py
|    └── sftp_private_key_file
|
└── README.md
```
