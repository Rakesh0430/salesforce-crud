
{
  "Name": "Acme Corporation Updated",
  "AnnualRevenue": 5000000,
  "Phone": "+1-555-123-4567"
}


curl -X POST \
  https://iscstech4-dev-ed.develop.my.salesforce.com/services/data/v58.0/sobjects/Account/ \
  -H 'Authorization: Bearer 00DWU00000DJWWu!AQEAQNYvMkgZUzAjz6xFLnnL1ULYzKrI28cIG97i5UZVPkx1RoLA1NbrfSyj2qtwrbquFd5PbO5bLxDlss.WXJ2yIDbk31JG' \
  -H "Content-Type: application/json" \
  -d '{
    "Name": "TechCorp Solutions",
    "AccountNumber": "TECH002",
    "Site": "East Coast",
    "Type": "Customer - Channel",
    "Industry": "Technology",
    "AnnualRevenue": 7500000,
    "Rating": "Warm",
    "Phone": "415-555-0123",
    "Fax": "415-555-0124",
    "Website": "www.techcorpsolutions.com",
    "TickerSymbol": "TECH",
    "Ownership": "Public",
    "NumberOfEmployees": 1500
  }'


  curl -X DELETE \
  https://iscstech4-dev-ed.develop.my.salesforce.com/services/data/v58.0/sobjects/Account/created_id \
  -H 'Authorization: Bearer 00DWU00000DJWWu!AQEAQNYvMkgZUzAjz6xFLnnL1ULYzKrI28cIG97i5UZVPkx1RoLA1NbrfSyj2qtwrbquFd5PbO5bLxDlss.WXJ2yIDbk31JG'





  import requests

# Salesforce OAuth2 credentials
client_id = "3MVG9PwZx9R6_UrcZJUxBG5sQRKJw1LBzOCjWsGpW1VhC3Ic7VyJBQ4MvWfXNzpJMFxGTEd8mACOoaYeHb0.f"
client_secret = "4037F4C7C2E0192C09A4EB64275C2E3FF2B707D1C2E631AB14ABE828669E2756"
username = "rock@iscs.sandbox"
password = "12345678@LrsZ5cmYVAMkNFr2r8DBZkzDGl6"
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





# code for acessing the salesforce instance url

import requests
client_id = "3MVG9PwZx9R6_UrcZJUxBG5sQRKJw1LBzOCjWsGpW1VhC3Ic7VyJBQ4MvWfXNzpJMFxGTEd8mACOoaYeHb0.f"
client_secret = "4037F4C7C2E0192C09A4EB64275C2E3FF2B707D1C2E631AB14ABE828669E2756"
username = "rock@iscs.sandbox"
password = "12345678@LrsZ5cmYVAMkNFr2r8DBZkzDGl6"
token_url = "https://login.salesforce.com/services/oauth2/token"  # For production, use login.salesforce.com; for sandbox, use test.salesforce.com

# Prepare the payload
payload = {
    "grant_type": "password",
    "client_id": client_id,
    "client_secret": client_secret,
    "username": username,
    "password": password
}

# Send the POST request
response = requests.post(token_url, data=payload)

# Parse the response
if response.status_code == 200:
    result = response.json()
    instance_url = result.get("instance_url")
    print("Instance URL:", instance_url)
else:
    print("Error:", response.text)


# salesforce rest api of a cutom object named ISCS__C given below for reference

https://iscstech4-dev-ed.develop.my.salesforce.com/services/data/v58.0/sobjects/ISCS__c/



Step 1: Log in to Salesforce
Open your web browser and navigate to your Salesforce org.
Enter your username and password to log in.

Step 2: Access Object Manager
Click the gear icon (⚙️) in the upper-right corner of Salesforce.
Select Setup from the dropdown menu.
On the Setup page, type Object Manager into the Quick Find box (on the left-hand side).
Click Object Manager in the search results.


Step 3: Create a New Custom Object
On the Object Manager page, click the Create dropdown in the upper-right corner.
Select Custom Object.
Fill in the details for your custom object:
Label: ISCS
Plural Label: ISCS Records
Object Name: ISCS (auto-filled based on the Label).
Record Name:
Choose Data Type:
If you want an auto-generated record name, choose Auto Number and use a format like ISCS-{0000}.
For manual input, select Text.
Allow Reports: Check this box.
Allow Activities: Check this box.
Allow Search: Check this box.
Deployment Status: Set to Deployed.
Click Save.


Step 4: Add Custom Fields
After creating the ISCS object, you'll be redirected to its details page.
If not, search for ISCS in Object Manager and select it.
In the sidebar, click Fields & Relationships.
Click New to add a new custom field.
Fields to Create
Follow these steps for each field:

Select the Field Type and click Next.
Fill in the field details:
Field Label: Name of the field (e.g., "Customer Name").
Field Name: Auto-filled based on the label.
Specify additional attributes (e.g., length, precision, required).
Set the Field Level Security:
Choose which profiles can see or edit the field.
Add the field to the page layout by selecting Add Field to All Page Layouts.
Click Save & New to create another field or Save to finish.


Example Fields
Here are some examples of fields to create:

Field Label	Field Type	Description
Customer Name	Text (255)	Full name of the customer
Email Address	Email	Customer's email address
Phone Number	Phone	Customer's phone number
Registration Date	Date	Date the customer was registered
Is Active	Checkbox	Indicates if the customer is active
Account Balance	Currency	Customer's account balance
Customer ID	Auto Number	Unique identifier for each customer (e.g., ISCS-{0001})


Step 5: Create a Tab for the ISCS Object
In Setup, type Tabs in the Quick Find box.
Click Tabs under User Interface.
Under Custom Object Tabs, click New.
Select ISCS from the Object dropdown.
Choose a Tab Style (e.g., a relevant icon for ISCS).
Click Next, then assign the tab to profiles and apps.
Click Save.


Step 6: Add Data to the ISCS Object
Navigate to the ISCS object tab (if created) or use the App Launcher to find the object.
Click New to create a new record.
Fill in the fields with sample data:
Customer Name: John Doe
Email Address: john.doe@example.com
Phone Number: +1 123 456 7890
Registration Date: 01/01/2025
Is Active: Checked
Account Balance: $1000
Click Save.


Step 7: Test and Verify
Go to the ISCS object tab and check if the sample records are visible.
Confirm that you can view and edit the fields.
Use the Global Search bar to confirm the object is searchable (if search is enabled).


