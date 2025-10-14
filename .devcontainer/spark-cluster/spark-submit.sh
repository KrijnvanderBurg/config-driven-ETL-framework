# Permission denied? run `chmod +x spark-submit.sh`

#!/bin/bash
# Use first argument as the file to submit, or default to sample_job.py if no argument provided
FILE_TO_SUBMIT=${1}

# Check if file has .py extension
# if [[ "$FILE_TO_SUBMIT" != *.py ]]; then
#   echo "Error: Only Python files (.py) are supported."
#   exit 1
# fi

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=devcontainer \
  --conf spark.driver.bindAddress=0.0.0.0 \
  "$FILE_TO_SUBMIT"