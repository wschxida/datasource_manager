cd /d %~dp0
SET FileName=listpage_url_pusher.log
cd log
IF EXIST %FileName%  ( DEL /F /S /Q %FileName% )
cd ..
python listpage_url_pusher_run.py
pause