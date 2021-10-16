#!/bin/bash
cd /app
python -m pytest /app/tests/test_nasa_log.py >> /app/tests/test_results.log
echo "Done"