@echo off
set CONSUL_BIN=D:\Downloads\consul_1.17.0_windows_amd64
set BACKUP_DIR=CD:\Downloads\consul_1.17.0_windows_amd64\backup
set SNAPSHOT_FILE=consul_snapshot_%date:~-4,4%%date:~-10,2%%date:~-7,2%_%time:~-11,2%%time:~-8,2%.snap

%CONSUL_BIN% snapshot save "%BACKUP_DIR%\%SNAPSHOT_FILE%"
