import os
import json
import base64
import datetime
import requests
import boto3
from dotenv import load_dotenv
from dagster import asset, AssetExecutionContext

load_dotenv()

def get_ebay_oauth_token() -> str:
    client_id = os.getenv("EBAY_CLIENT_ID")
    client_secret = os.getenv("EBAY_CLIENT_SECRET")
    
    auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")
    
    url = "https://api.ebay.com/identity/v1/oauth2/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {auth_header}"
    }
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope"
    }
    
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    return response.json()["access_token"]

@asset(group_name="ingestion")
def ebay_raw_to_s3(context: AssetExecutionContext) -> str:
    search_keyword = "rtx 4080"
    context.log.info(f"Fetching eBay data for: {search_keyword}")
    
    token = get_ebay_oauth_token()
    
    url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_US"
    }
    params = {
        "q": search_keyword,
        "limit": 50,
        "filter": "buyingOptions:{FIXED_PRICE}"
    }
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    raw_payload = response.json()
    
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    
    bucket = os.getenv("S3_BUCKET_NAME")
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
    clean_term = search_keyword.replace(" ", "_").lower()
    
    file_key = f"raw/ebay/{clean_term}/{timestamp}.json"
    
    s3_client.put_object(
        Bucket=bucket,
        Key=file_key,
        Body=json.dumps(raw_payload),
        ContentType="application/json"
    )
    
    context.log.info(f"Uploaded to s3://{bucket}/{file_key}")
    return file_key
