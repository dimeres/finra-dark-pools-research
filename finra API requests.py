import requests
import pandas as pd
import json as js

# password is valid for one year
user = "paste user ID"
password = "password for API connection" #not API dev platform password


token_url = "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token?grant_type=client_credentials"

response = requests.post(token_url, 
                         auth=(user, password), 
                         headers={"Accept": "application/json"}, 
                         timeout=30) #FINRA asks for POST request to get token

print(response.status_code, response.reason)

#response.raise_for_status()
# The access_token returned by FIP is used as a BEARER token in the Authorization header (Bearer access_token) of API requests and not a Basic Auth token.
token = response.json()["access_token"]
print("Access token:", token)



#------------GETTING DATA

# variables to change datasets in URL
dataset =  "blockSummaryMock"
params = {
    "limit": 10, # Synchronous request max 5000 rec; Asynchronous - 100,000 records for any one API request.
    "monthStartDate[gte]": "2025-06-01",
    "monthStartDate[lte]": "2025-08-31",  
    "sort": "monthStartDate"              # ascending sort
}


api_url = f"https://api.finra.org/data/group/otcMarket/name/{dataset}"


headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

#for short testing USE GET request
data_response = requests.get(api_url, headers=headers, params={"limit": 5})
print(response.status_code, response.reason)

#review
data = data_response.json()
print(js.dumps(data, indent=4))
      

