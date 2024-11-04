```shellscript
# access the container via the termninal 
docker exec -it apache_beam bash

# run the pipeline 
python /workspace/pipelines/pipeline4.py

# running in jupyter notebook 
# when booting up via 

docker compose up # do not dettach

# you will see at the end a link that will take you to jupyter in the browser 
http://127.0.0.1:8888/tree?token=f44449ab989bde9a40af77373f2f62f7e8c45e45645881

```
