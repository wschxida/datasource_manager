cd /d %~dp0
SET FileName=clear_reject_domain.log
cd log
IF EXIST %FileName%  ( DEL /F /S /Q %FileName% )
cd ..
python clear_reject_domain_run.py
pause