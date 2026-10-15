@echo off
set PYTHONPATH=%~dp0;%PYTHONPATH%
python -m pipeline_app.cli %*
