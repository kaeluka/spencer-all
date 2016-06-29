date
ls *.stdout
tail *.stdout
du -sh $(./getBenchmarkDrive.sh)/prototracefile*.log.gz
du -sh $(./getBenchmarkDrive.sh)/tracefiles/*
ls $(./getBenchmarkDrive.sh)/tracefiles | wc -l
