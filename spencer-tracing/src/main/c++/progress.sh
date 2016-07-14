date
ls *.stdout
tail *.stdout
du -sh $(./getBenchmarkDrive.sh)/prototracefile*.log.gz
du -sh $(./getBenchmarkDrive.sh)/com.github.kaeluka.spencer.tracefiles/*
ls $(./getBenchmarkDrive.sh)/com.github.kaeluka.spencer.tracefiles | wc -l
