@echo off
set cwd=%~dp0
pushd %cwd%
echo %cd%


set nc_main=%cwd%main_pqpf_nc.py
set sc_main=%cwd%main_pqpf_sc.py


start "sqlauthproxy" cmd /k call sqlauthproxy.bat
timeout /t 15
call activate pqpf
python %nc_main%
python %sc_main%
call conda deactivate
timeout /t 15
taskkill /f /im cloud_sql_proxy.exe
taskkill /fi "WindowTitle eq sqlauthproxy*" /t /f
