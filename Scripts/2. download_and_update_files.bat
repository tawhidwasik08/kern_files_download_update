@echo off
set python_exe_loc=%cd%\kern_venv\Scripts\python.exe
set python_file_download_script_loc=%cd%\file_download.py
set python_file_update_script_loc=%cd%\file_update.py
%python_exe_loc% %python_file_download_script_loc%
%python_exe_loc% %python_file_update_script_loc%
pause