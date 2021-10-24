#!/usr/bin/env python
# coding: utf-8

# In[1]:



# In[2]:


import boto3


# In[3]:


s3 = boto3.resource('s3', aws_access_key_id='AKIA44BRX5AD2DFNSYSH', aws_secret_access_key='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')


# In[4]:


try:
    s3.create_bucket(Bucket='datacont-lucas',CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
except Exception as e:
    print("Already made a bucket.")


# In[5]:


bucket = s3.Bucket("datacont-lucas")


# In[6]:


bucket.Acl().put(ACL='public-read')


# In[8]:


body = open('/Users/Lucas 1/Documents/Cloud Computing/datafiles/exp1.csv','rb')


# In[9]:


o = s3.Object('datacont-lucas','test').put(Body=body)


# In[10]:


s3.Object('datacont-lucas','test').Acl().put(ACL='public-read')


# In[11]:


dyndb = boto3.resource('dynamodb',
 region_name='us-west-2',
 aws_access_key_id='AKIA44BRX5AD2DFNSYSH',
 aws_secret_access_key='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'
)


# In[12]:


try:
 table = dyndb.create_table(
 TableName='DataTable',
 KeySchema=[
 {
 'AttributeName': 'PartitionKey',
 'KeyType': 'HASH'
 },
 {
 'AttributeName': 'RowKey',
 'KeyType': 'RANGE'
 }
 ],
 AttributeDefinitions=[
 {
 'AttributeName': 'PartitionKey',
 'AttributeType': 'S'
 },
 {
 'AttributeName': 'RowKey',
 'AttributeType': 'S'
 },
 ],
 ProvisionedThroughput={
 'ReadCapacityUnits': 5,
 'WriteCapacityUnits': 5
 }
 )
except Exception as e:
 print("Already made a table.")
 #if there is an exception, the table may already exist. if so...
 table = dyndb.Table("DataTable")


# In[13]:


table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')


# In[14]:


print(table.item_count)


# In[15]:


import csv


# In[23]:


with open('/Users/Lucas 1/Documents/Cloud Computing/experiments.csv', 'r', encoding='utf-8-sig') as csvfile:
 csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
 for item in csvf:
     print(item)
     body = open('/Users/Lucas 1/Documents/Cloud Computing/datafiles/'+item[3], 'rb')
     s3.Object('datacont-lucas', item[3]).put(Body=body )
     md = s3.Object('datacont-lucas', item[3]).Acl().put(ACL='public-read')

     url = " https://s3-us-west-2.amazonaws.com/datacont-lucas/"+item[3]
     metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
         'description' : item[4], 'date' : item[2], 'url':url}
     try:
         table.put_item(Item=metadata_item)
     except:
         print("item may already be there or another failure")


# In[24]:


response = table.get_item(
 Key={
 'PartitionKey': 'experiment3',
 'RowKey': '3'
 }
)
item = response['Item']
print(item)


# In[25]:


response


# In[ ]:




