import requests

# Salesforce OAuth2 credentials
client_id = "3MVG9VMBZCsTL9hmvV8TFw_RpZaH15_w1rx5c.K5qh76AIWcnupe6IklzqgUrbvYT72uU4nshYSh6DwauZ_Jp"
client_secret = "8C5F362DFC6FAC9B7A2D7F74BA66A36271953EF912E1F053EA257E6B9CB94B72"
username = "rakeshrocky@iscs.sandbox"
password = "12345678@LrsEyODEws6fiLwk9Ej65AfLfqG"
token_url = "https://login.salesforce.com/services/oauth2/token"  # Use https://login.salesforce.com for production

# Create a dictionary for the OAuth2 parameters
params = {
    "grant_type": "password",
    "client_id": client_id,
    "client_secret": client_secret,
    "username": username,
    "password": password
}

# Send POST request to obtain the access token
try:
    response = requests.post(token_url, data=params)
    response.raise_for_status()  # Check for HTTP errors
    access_token_info = response.json()  # Parse JSON response
    access_token = access_token_info.get("access_token")

    if access_token:
        print("Access Token:", access_token)
    else:
        print("Error:", access_token_info.get("error_description"))

except requests.exceptions.HTTPError as http_err:
    print(f"HTTP error occurred: {http_err}")
except Exception as err:
    print(f"Other error occurred: {err}")
