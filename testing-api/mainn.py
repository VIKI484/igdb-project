
# läs in YAML i din kod
import yaml

with open("listnames.yml", "r") as f:
    config = yaml.safe_load(f)


urls = config.get("urls",[])

# Loopa över URL:arna  

for url in urls:
    my_data.api_fetch(url, client_id=, auth["access_token"], 
                      ["name", "rating", "release_dates"], 5)
    

    if my_data.data:
        logger.info(f"Data fetch successful from {url}")
        print(my_data)