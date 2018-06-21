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
        
        parameters = {'hapikey': token, 'property': contact_properties, 'vidOffset': offset, 'count': 100, 'formSubmissionMode':'all', 'showListMemberships':'true'}
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
        
def getDeals(token):
    
    final_df = pd.DataFrame()
    offset = 0 
    deal_properties = ['authority', 'budget', 'campaign_source', 'hs_analytics_source', 'hs_campaign', 
                       'hs_lastmodifieddate', 'need', 'partner_name', 'timeframe', 'dealname', 'amount', 'closedate', 'pipeline',
                       'createdate', 'engagements_last_meeting_booked', 'dealtype', 'hs_createdate', 'description',
                       'start_date', 'closed_lost_reason',  'closed_won_reason', 'end_date', 'lead_owner', 'tech_owner', 
                       'service_amount', 'contract_type', 'hubspot_owner_id']
    
    while True:
        
        parameters = {'hapikey': token, 'properties': deal_properties, 'offset': offset, 'limit': 250, 
                      'propertiesWithHistory': 'dealstage','includeAssociations': 'true'}
        req = requests.get('https://api.hubapi.com/deals/v1/deal/paged', params = parameters)
        req_response = req.json()
            
        if req_response['hasMore'] == True:
            final_df = final_df.append(json_normalize(req_response['deals']))
        else:
            final_df = final_df.append(json_normalize(req_response['deals']))
            return final_df
        
        offset = req_response['offset']
        
def getActivities(token):
    
    final_df = pd.DataFrame()
    offset = 0
    
    while True:
        
        parameters = {'hapikey': token, 'offset': offset, 'limit': 250}
        req = requests.get('https://api.hubapi.com/engagements/v1/engagements/paged', params = parameters)
        req_response = req.json()
        
        if req_response['hasMore'] == True:
            final_df = final_df.append(json_normalize(req_response['results']))
            final_df.drop(['metadata.text', 'metadata.html'], 1)
        else:
            final_df = final_df.append(json_normalize(req_response['results']))
            final_df.drop(['metadata.text', 'metadata.html'], 1)
            return final_df
        
        offset = req_response['offset']

def getLists(token):
    
    final_df = pd.DataFrame()
    offset = 0
    
    while True:
        
        parameters = {'hapikey': token, 'offset': offset, 'limit': 250}
        req = requests.get('https://api.hubapi.com/contacts/v1/lists', params = parameters)
        req_response = req.json()
        
        if req_response['has-more'] == True:
            final_df = final_df.append(json_normalize(req_response['lists']))
        else:
            final_df = final_df.append(json_normalize(req_response['lists']))
            return final_df
        
        offset = req_response['offset']
        
def getOwners(token):
    
    final_df = pd.DataFrame()
    
    parameters = {'hapikey': token}
    req = requests.get(' https://api.hubapi.com/owners/v2/owners/', params = parameters)
    req_response = req.json()
        
    final_df = final_df.append(json_normalize(req_response))
    
    return final_df
                    
## Datasets extraction
print('Extracting Companies from HubSpot CRM')        
Companies = getCompanies(token)
print('Extracting Contacts from HubSpot CRM')
Contacts = getContacts(token)
print('Extracting Deals from HubSpot CRM')
Deals = getDeals(token)
print('Extracting Activities from HubSpot CRM')
Activities = getActivities('90a176d2-789f-4393-99aa-96b3f908934f')
print('Extracting Lists from HubSpot CRM')
Lists = getLists(token)
print('Extracting Owners from HubSpot CRM')
Owners = getOwners(token)

Contacts_sub_forms = pd.DataFrame()
Contacts_Lists = pd.DataFrame()

### Create table with Contact's form submissions and lists and drop column afterwards
for index, row in Contacts.iterrows():
    
    if len(row['form-submissions']) > 0 :
        temp_contacts_sub_forms = pd.DataFrame(row['form-submissions'])
        temp_contacts_sub_forms['CONTACT_ID'] = row['canonical-vid']    
        Contacts_sub_forms = Contacts_sub_forms.append(temp_contacts_sub_forms)
    if len(row['list-memberships']) > 0 :
        temp_contacts_lists = pd.DataFrame(row['list-memberships'])
        temp_contacts_lists['CONTACT_ID'] = row['canonical-vid']
        Contacts_Lists = Contacts_Lists.append(temp_contacts_lists)

Contacts = Contacts.drop(['form-submissions', 'list-memberships'], 1)

### Write extracted data
Companies.to_csv('/data/out/tables/companies.csv', index = False)
Contacts.to_csv('/data/out/tables/contacts.csv', index = False)
Deals.to_csv('/data/out/tables/deals.csv', index = False)
Activities.to_csv('/data/out/tables/activities.csv', index = False)
Lists.to_csv('/data/out/tables/lists.csv', index = False)
Owners.to_csv('/data/out/tables/owners.csv', index = False)
Contacts_sub_forms.to_csv('/data/out/tables/contacts_form_submissions.csv', index = False)
Contacts_Lists.to_csv('/data/out/tables/contacts_lists.csv', index = False)
