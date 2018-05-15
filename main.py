#==============================================================================
import json
import requests
import pandas as pd
import time
from pandas.io.json import json_normalize
from keboola import docker
#==============================================================================

## Initialise app and get parameters from configuration.
cfg = docker.Config('/data/')
token = cfg.get_parameters()['#token']

def getContacts(token):
    
    final_df = pd.DataFrame()
    offset = -1 
    contact_properties = ['hs_facebookid','hs_linkedinid','ip_city','ip_country',
                         'ip_country_code','newsletter_opt_in','firstname','linkedin_profile',
                         'lastname','email','mobilephone','phone','city',
                         'country','region','jobtitle','company','website','numemployees',
                         'industry','associatedcompanyid']
    
    while True:
        
        parameters = {'hapikey': token, 'property': contact_properties, 'vidOffset': offset, 'count': 100, 'formSubmissionMode':'all'}
        req = requests.get('https://api.hubapi.com/contacts/v1/lists/all/contacts/all', params = parameters)
        req_response = req.json()
            
        if req_response['has-more'] == True:
            final_df = final_df.append(json_normalize(req_response['contacts']))
        else:
            final_df = final_df.append(json_normalize(req_response['contacts']))
            return final_df
        
        offset = req_response['vid-offset']
        
def getCompanies(token):
    
    final_df = pd.DataFrame()
    offset = 0 
    company_properties = ['about_us','name','phone','facebook_company_page',
                         'city','country','website','numberofemployees',
                         'industry','annualrevenue','linkedin_company_page']
    
    while True:
        
        parameters = {'hapikey': token, 'properties': company_properties, 'offset': offset, 'limit': 250}
        req = requests.get('https://api.hubapi.com/companies/v2/companies/paged', params = parameters)
        req_response = req.json()
            
        if req_response['has-more'] == True:
            final_df = final_df.append(json_normalize(req_response['companies']))
        else:
            final_df = final_df.append(json_normalize(req_response['companies']))
            return final_df
        
        offset = req_response['offset']

## Datasets extraction
print('Extracting Companies from HubSpot CRM')        
Companies = getCompanies(token)
print('Extracting Contacts from HubSpot CRM')
Contacts = getContacts(token)

Contacts_sub_forms = pd.DataFrame()

## Create table with Contact's form submissions and drop column afterwards
for index, row in Contacts.iterrows():
    if len(row['form-submissions']) > 0 :
        temp_contacts_sub_forms = pd.DataFrame(row['form-submissions'])
        temp_contacts_sub_forms['CONTACT_ID'] = row['profile-token']    
        Contacts_sub_forms = Contacts_sub_forms.append(temp_contacts_sub_forms)

Contacts = Contacts.drop('form-submissions', 1)

## Write extracted data
Companies.to_csv('/data/out/tables/companies.csv', index = False)
Contacts.to_csv('/data/out/tables/contacts.csv', index = False)
Contacts_sub_forms.to_csv('/data/out/tables/contacts_form_submissions.csv', index = False)


