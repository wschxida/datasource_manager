cd /d %~dp0
SET FileName=find_new_listpage.log
cd log
IF EXIST %FileName%  ( DEL /F /S /Q %FileName% )
cd ..
python find_new_listpage_run.py
pause