from fastapi import FastAPI, HTTPException, Depends , status
from fastapi.security import OAuth2PasswordBearer
import httpx
import asyncio
import pymongo
import os
import base64
import motor.motor_asyncio
from pydantic import BaseModel
from typing import List, Dict
import logging
from typing import Annotated
import base64
from hashlib import sha256
from typing import Annotated, AsyncGenerator, List, Optional
from uuid import uuid4
from urllib.parse import urlencode
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.security import OAuth2PasswordBearer
import httpx
from sqlalchemy import text
import uvicorn
from dotenv import load_dotenv
from fastapi import (
    FastAPI,
    Query,
    Request,
    Depends,
    HTTPException,
    status,
    APIRouter,
    FastAPI,
    Request,
    Depends,
    HTTPException,
    status,
)
# from db.mongo import client




app = FastAPI(
    title="CARTA Data Api",
    version="1.0.0",
    contact={
        "name": "Aniket",
        "email": "Aniket@joinhighcircle.com",
    },)




# Define scopes for requests
SCOPES_REQUESTS = [
    "read_investor_stakeholdercapitalizationtable",
    "read_investor_funds",
    "read_investor_firms",
    "read_investor_securities",
    "read_investor_capitalizationtables",
    "read_investor_investments",
    "read_issuer_info",
    "readwrite_issuer_securities",
    "read_portfolio_fundinvestmentdocuments",
]

# OAuth2PasswordBearer for token retrieval
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/get_access_token")



# -------------------- All credentials

# CLIENT_ID = "Jezr1DoOyv6l47BARDsjbG6fxedBuqynxtImFjmH"
# CLIENT_SECRET = "Ky5TGqBHta761wKcJUCOXrkwV46kI029Zy8loeTkFnadEz2mnWXyAFhZcv2PkNbak9IIk6Psm73Eryw5k6aierHX45FKpYhnjDQTNqfDASbNmuEcjSWbfX1qR84IInN1"
# BASE_AUTH_URL = "https://login.playground.carta.team"
# BASE_API_URL = "https://api.playground.carta.team/v1alpha1"


CLIENT_ID = os.getenv("CLIENT_ID")
print(CLIENT_ID)
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
print(CLIENT_SECRET)
BASE_AUTH_URL = os.getenv("BASE_AUTH_URL")
print(BASE_AUTH_URL)
BASE_API_URL = os.getenv("BASE_API_URL")
print(BASE_API_URL)



# MongoDB configuration
# MONGODB_URL = os.getenv("MONGODB_URL", "mongodb+srv://comedyclutches070:t4iOcGEXxW3j42vK@carta07.dctui.mongodb.net/")
# MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", "Carta_test")

# client = pymongo.MongoClient(MONGODB_URL)
# db = client[MONGODB_DB_NAME]

MONGODB_URL = os.getenv("MONGODB_URL")
print(MONGODB_URL)
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME")
print(MONGODB_DB_NAME)

client = pymongo.MongoClient(MONGODB_URL)
db = client[MONGODB_DB_NAME]




class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    expires_in: int



# aws_db_router = APIRouter(tags=["AWS RDS Access"])
mongo_db_router = APIRouter(tags=["Mongo Atlas Access"])
carta_data_router = APIRouter(tags=["All CARTA Data"])
carta_api_router = APIRouter(tags= ["CARTA Endpoints"])
access_token = APIRouter(tags= ["Get Access Token"])






# --------------------------- Firstly  To access Token  to get access to Apis

@access_token.post("/get_access_token/", response_model=TokenResponse) 
async def get_access_token():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise HTTPException(
            status_code=500,
            detail="Client ID and secret must be set in environment variables.",
        )

    # Create the base64 encoded client info
    client_info = f"{CLIENT_ID}:{CLIENT_SECRET}"
    encoded_client_info = base64.b64encode(client_info.encode()).decode()
    token_url = f"{BASE_AUTH_URL}/o/access_token/"
    headers = {
        "Authorization": f"Basic {encoded_client_info}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        "scope": " ".join(SCOPES_REQUESTS),
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(token_url, headers=headers, data=data)

            # Raise an exception for HTTP errors
            response.raise_for_status()
            return response.json()  # Automatically converts to TokenResponse model

        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=e.response.status_code, detail=e.response.text
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

#-----------------------------------------------------------------------



###-----------------------------------------   ALL CARTA DATA ----------------------------------------

## -----------  List  Firms

# ---------  Retrieves investment firms.

# @carta_router.get("/firms")


@carta_data_router.get("/investors/firms")
async def list_firms(access_token: Annotated[str, Depends(oauth2_scheme)]):
    url = f"{BASE_API_URL}/investors/firms"
    print(url)
    print(oauth2_scheme)
    print(access_token)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}",  # Use the actual token here
    }
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)
        if resp.is_success:
            return resp.json()
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, detail="Failed to fetch data"
    )




## -----------  List  Funds

# ------ Retrieves the investment funds of a given investment firm. ( also we can get Funds id  from firms )

# Function to handle API rate limits by introducing a delay
async def handle_rate_limit():
    logging.info("Rate limit hit. Waiting for 1 second before retrying...")
    await asyncio.sleep(1)  # Introduce a delay to prevent flooding the API with requests

# Function to fetch firm_ids from MongoDB
async def get_firm_ids_from_db() -> List[str]:
    # collection_firms = "firms"  #  database of firms list
    collection_firms = "list_firms"
    collection = db[collection_firms]
    
    ids = []
    for document in collection.find():
        if "firms" in document:
            for firm in document["firms"]:
                if "id" in firm:
                    ids.append(firm["id"])
    
    logging.info(f"Fetched firm_ids from MongoDB: {ids}")
    return ids

# Function to fetch funds for a given firm_id from the API
async def fetch_funds_for_firm(firm_id: str, token: str) -> Dict:
    url = f"{BASE_API_URL}/investors/firms/{firm_id}/funds"
    headers = {
        "Authorization": f"Bearer {token}",
    }

    logging.info(f"Fetching funds for firm_id: {firm_id}, URL: {url}")
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers)
            logging.info(f"Response status for firm_id {firm_id}: {response.status_code}")
            logging.info(f"Response content for firm_id {firm_id}: {response.text}")
            
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:  # Too Many Requests
                await handle_rate_limit()  # Introduce a delay before retrying
                return await fetch_funds_for_firm(firm_id, token)  # Retry request
            logging.error(f"HTTP error fetching funds for firm_id {firm_id}: {e.response.text}")
            raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
        except Exception as e:
            logging.error(f"General error fetching funds for firm_id {firm_id}: {str(e)}")
            raise HTTPException(status_code=500, detail="Error fetching funds data.")


# FastAPI endpoint to get all firm_ids and fetch funds for each firm

# @carta_router.get("/firms/funds", response_model=List[Dict])
@carta_data_router.get("/investors/firms/{firmId}/funds", response_model=List[Dict])
async def get_all_funds(token: str = Depends(oauth2_scheme)):
    try:
        # Fetch firm IDs from MongoDB
        firm_ids = await get_firm_ids_from_db()
        
        # Limit the number of firms to test if the issue is related to the volume of requests
        if not firm_ids:
            raise HTTPException(status_code=404, detail="No firm_ids found in the database.")
        
        tasks = []

        # For debug: use only a subset of firm_ids, for example, the first firm_id.
        # You can increase the number of firms after confirming this works.
        limited_firm_ids = firm_ids[:1]  # Test with a single firm_id to isolate the problem.

        # Create tasks for each firm_id to fetch its funds
        for firm_id in limited_firm_ids:
            tasks.append(fetch_funds_for_firm(firm_id, token))
        
        # Await all tasks to fetch funds
        funds = await asyncio.gather(*tasks)
        
        return funds
    except HTTPException as e:
        logging.error(f"HTTP exception: {str(e)}")
        raise e
    except Exception as e:
        logging.error(f"General exception: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))




#  ----------------------    List Investments 

#   ------------ Retrieve the company investments of a given investment fund.  ( and also to get Investments id from Funds )


# Function to get all firmId and fundId from MongoDB
async def get_firm_and_fund_ids() -> List[Dict[str, str]]:
    collection_firms = "firm_funds"   # database of stored funds 
    collection = db[collection_firms]

    firms_info = []
    for document in collection.find():
        if "firmId" in document and "id" in document:
            firms_info.append({"firmId": document["firmId"], "fundId": document["id"]})

    return firms_info

# Function to fetch investments for a given firm and fund
async def fetch_investments(firm_id: str, fund_id: str, token: str) -> Dict:
    url = f"{BASE_API_URL}/investors/firms/{firm_id}/funds/{fund_id}/investments"
    headers = {
        "Authorization": f"Bearer {token}",
    }
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

# FastAPI endpoint to get all Investments from firm_ids & fund_ids 

# @carta_router.get("/investments", response_model=List[Dict])
@carta_data_router.get("/investors/firms/{firmId}/funds/{fundId}/investments", response_model=List[Dict])
async def list_investments(token: str = Depends(oauth2_scheme)):
    try:
        firm_fund_ids = await get_firm_and_fund_ids()
        tasks = []

        # For each firm and fund ID, create tasks for fetching investments
        for entry in firm_fund_ids:
            firm_id = entry["firmId"]
            fund_id = entry["fundId"]
            tasks.append(fetch_investments(firm_id, fund_id, token))

        # Await all investment fetch tasks
        investments = await asyncio.gather(*tasks)
        return investments

    except HTTPException as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))






#   ------------  LIST  Capitalization Tables

#   ------------ Retrieves the capitalization tables for a given firm, fund, and investment company.  ( and also to get Capitalization_Id from Investemnts  )


# Function to get firm_id, fund_id and investment_id from MongoDB
async def get_captable_info() -> List[Dict[str, str]]:
    collection_investments = "186fb573-a22d-4c82-8ad3-3186f9095a41_85d952d3-5076-4e02-8872-ea175ec5f90d_investments" # Database of stored Investments 
    collection = db[collection_investments]

    captable_info = []
    for document in collection.find():
        if "investments" in document:
            for investment in document["investments"]:
                firm_id = investment.get("firmId", None)
                fund_id = investment.get("fundId", None)
                company_id = investment.get("companyId", None)
                captable_info.append({
                    "firmId": firm_id,
                    "fundId": fund_id,
                    "companyId": company_id
                })

    return captable_info

# Function to fetch capitalization tables for a given firm, fund, and company
async def fetch_capitalization_table(firm_id: str, fund_id: str, company_id: str, token: str) -> Dict:
    url = f"{BASE_API_URL}/investors/firms/{firm_id}/funds/{fund_id}/investments/{company_id}/capitalizationTables"
    headers = {
        "Authorization": f"Bearer {token}",
    }
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

# FastAPI endpoint to get all  Capitalization Tables from firm_ids , fund_ids & investment_id /comanyId

# @carta_router.get("/captableID", response_model=List[Dict])
@carta_data_router.get("/investors/firms/{firmId}/funds/{fundId}/investments/{companyId}/capitalizationTables", response_model=List[Dict])
async def list_capitalization_tables(token: str = Depends(oauth2_scheme)):
    try:
        investments_info = await get_captable_info()
        tasks = []

        # For each investment info, create tasks for fetching investments
        for entry in investments_info:
            firm_id = entry["firmId"]
            fund_id = entry["fundId"]
            company_id = entry["companyId"]
            tasks.append(fetch_capitalization_table(firm_id, fund_id, company_id, token))

        # Await all capitalization table fetch tasks
        capitalization_tables = await asyncio.gather(*tasks)
        return capitalization_tables

    except HTTPException as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





#   ------------  Get Capitalization Table       # Note we can't fetch the Stakeholder Captalization tables fro all at once it throws Error: " Too  many reqsts "

#   ------------ Retrieve a capitalization table for a given firm, fund, and investment , capitalizationTableID.  

import time

async def handle_rate_limit():
    time.sleep(1)  # Introduce a delay to prevent flooding the API with requests
    return


# Function to fetch captable data from MongoDB
async def get_capitalization_info() -> List[Dict[str, str]]:
    # collection_captables = db["stake_hlder_test"]   # use thios one it will return error too many requests
    collection_captables = db["get_capitalization_table"]  # Database of List Capitalization Table   

    captable_info = []
    for document in collection_captables.find():
        cap_table = document.get("capitalizationTable", {})
        
        captable_id = cap_table.get("id", None)  # Extract captable ID (id)
        firm_id = cap_table.get("firmId", None)  # Extract firm ID
        fund_id = cap_table.get("fundId", None)  # Extract fund ID
        investment_id = cap_table.get("companyId", None)  # Extract company ID as investment ID
        
        if captable_id and firm_id and fund_id and investment_id:
            captable_info.append({
                "captableId": captable_id,
                "firmId": firm_id,
                "fundId": fund_id,
                "investmentId": investment_id
            })

    return captable_info



# Function to fetch capitalization tables for a given firm, fund, and company

async def fetch_list_captables(firm_id: str, fund_id: str, investment_id: str, captable_id: str, token: str) -> Dict:
    url = f"{BASE_API_URL}/investors/firms/{firm_id}/funds/{fund_id}/investments/{investment_id}/capitalizationTables/{captable_id}"
    headers = {
        "Authorization": f"Bearer {token}",
    }
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:  # Too Many Requests
                await handle_rate_limit()  # Delay before retrying
                return await fetch_list_captables(firm_id, fund_id, investment_id, captable_id, token)
            raise HTTPException(status_code=e.response.status_code, detail=e.response.text)




# FastAPI endpoint to get all  Capitalization Tables from firm_ids , fund_ids & investment_id /comanyId

# @carta_router.get("/list_captables", response_model=List[Dict])
@carta_data_router.get("/investors/firms/{firmId}/funds/{fundId}/investments/{companyId}/capitalizationTables/{capitalizationTableId}", response_model=List[Dict])
async def get_list__of_captables(token: str = Depends(oauth2_scheme)):
    try:
        captable_info = await get_capitalization_info()
        tasks = []

        # For each investment info, create tasks for fetching stakeholder capitalization tables
        for entry in captable_info:
            firm_id = entry["firmId"]
            fund_id = entry["fundId"]
            investment_id = entry["investmentId"]
            captable_id = entry["captableId"]
            tasks.append(fetch_list_captables(firm_id, fund_id, investment_id, captable_id, token))

        # Await all capitalization table fetch tasks
        capitalization_tables = await asyncio.gather(*tasks)
        return capitalization_tables

    except HTTPException as e:
        raise  HTTPException(status_code=e.response.status_code, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




#   ------------  Get Stakeholder Capitalization Table

#   ------------ Retrieve an investor's stakeholder capitalization table for a given firm, fund, and investment , capitalizationTableID.  

# Function to fetch captable data from MongoDB
async def get_stake_holder_capitalization_info() -> List[Dict[str, str]]:
    collection_captables = db["stake_hlder_test"]      #if u make single attempt it gives the data but for multiple it will return error too many requests
    # collection_captables = db["get_capitalization_table"]  # Database of List Capitalization Table   

    captable_info = []
    for document in collection_captables.find():
        cap_table = document.get("capitalizationTable", {})
        
        captable_id = cap_table.get("id", None)  # Extract captable ID (id)
        firm_id = cap_table.get("firmId", None)  # Extract firm ID
        fund_id = cap_table.get("fundId", None)  # Extract fund ID
        investment_id = cap_table.get("companyId", None)  # Extract company ID as investment ID
        
        if captable_id and firm_id and fund_id and investment_id:
            captable_info.append({
                "captableId": captable_id,
                "firmId": firm_id,
                "fundId": fund_id,
                "investmentId": investment_id
            })

    return captable_info


# Function to fetch Stakeholder for a given firm, fund, investment and company
async def fetch_stakeholder_captables(firm_id: str, fund_id: str, investment_id: str, captable_id: str, token: str) -> Dict:
    url = f"{BASE_API_URL}/investors/firms/{firm_id}/funds/{fund_id}/investments/{investment_id}/stakeholderCapitalizationTables/{captable_id}"
    headers = {
        "Authorization": f"Bearer {token}",
    }
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:  # Too Many Requests
                await handle_rate_limit()  # Delay before retrying
                return await fetch_stakeholder_captables(firm_id, fund_id, investment_id, captable_id, token)
            raise HTTPException(status_code=e.response.status_code, detail=e.response.text)



# FastAPI endpoint to get all  Capitalization Tables from firm_ids , fund_ids & investment_id /comanyId

# @carta_router.get("/stakeholder_captables", response_model=List[Dict])
@carta_data_router.get("/investors/firms/{firmId}/funds/{fundId}/investments/{companyId}/stakeholderCapitalizationTables/{capitalizationTableId}", response_model=List[Dict])
async def get_stakeholder_capitalization_table(access_token: Annotated[str, Depends(oauth2_scheme)]):
    try:
        stake_holder_captable_info = await get_stake_holder_capitalization_info()
        tasks = []

        # For each investment info, create tasks for fetching stakeholder capitalization tables
        for entry in stake_holder_captable_info:
            firm_id = entry["firmId"]
            fund_id = entry["fundId"]
            investment_id = entry["investmentId"]
            captable_id = entry["captableId"]
            tasks.append(fetch_stakeholder_captables(firm_id, fund_id, investment_id, captable_id, access_token))

        # Await all capitalization table fetch tasks
            capitalization_tables = await asyncio.gather(*tasks)
        return capitalization_tables

    except HTTPException as e:
        raise  HTTPException(status_code=e.response.status_code, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


#----------------------------------------------------------------------------------------------------------------------------

#####  CARTA Investors Endpoints 
 
# List  Firms

@carta_api_router.get("/investors/firms.")
async def get_list_firms(access_token: Annotated[str, Depends(oauth2_scheme)], 
                         page_size: int = Query(10, ge=1, le=100),
                         page_token: str = None,):
    
    url = f"{BASE_API_URL}/investors/firms"
    params = {"pageSize": page_size, "pageToken": page_token}
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}",  # Use the actual token here
    }
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)
        if resp.is_success:
            return resp.json()
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, detail="Failed to fetch data"
    )


# List Funds
@carta_api_router.get("/investors/firms/{firm_id}/funds")
async def get_list_funds(access_token: Annotated[str, Depends(oauth2_scheme)], 
                         firm_id : str,
                         page_size: int = Query(10, ge=1, le=100),
                         page_token: str = None,):
    
    url = f"{BASE_API_URL}/investors/firms/{firm_id}/funds"
    params = {"pageSize": page_size, "pageToken": page_token}
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}",  # Use the actual token here
    }
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)
        if resp.is_success:
            return resp.json()
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, detail="Failed to fetch data"
    )

# List Investments

@carta_api_router.get("/investors/firms/{firm_id}/funds/{fund_id}/investments")
async def get_list_investments(
    firm_id: str,
    fund_id: str,
    access_token: Annotated[str, Depends(oauth2_scheme)],
    page_size: int = Query(10, ge=1, le=100),
    page_token: str = None,
):
    """
    Fetches data for a firm's funds from the API endpoint asynchronously.

    Args:
        firm_id (str): The ID of the firm.
        fund_id (str): The ID of the fund.
        page_size (int): The number of results to return per page (default is 10, min is 1, max is 100).
        page_token (str): The token to use for pagination (optional).
    """
    url = f"{BASE_API_URL}/investors/firms/{firm_id}/funds/{fund_id}/investments"
    params = {"pageSize": page_size, "pageToken": page_token}
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}",  # Use the actual token here
    }
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers, params=params)
        if resp.is_success:
            return resp.json()
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, detail="Failed to fetch data"
    )



# List Capitalization Tables

@carta_api_router.get(
    "/investors/firms/{firm_id}/funds/{fund_id}/investments/{company_id}/capitalizationTables"
)
async def get_list_capitalization_tables(
    firm_id: str,
    fund_id: str,
    company_id: str,
    access_token: Annotated[str, Depends(oauth2_scheme)],
    page_size: int = Query(10, ge=1, le=100),
    page_token: str = None,
):
    url = f"{BASE_API_URL}/investors/firms/{firm_id}/funds/{fund_id}/investments/{company_id}/capitalizationTables"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}",  # Use the actual token here
    }
    params = {"pageSize": page_size, "pageToken": page_token}
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers, params=params)
        if resp.is_success:
            return resp.json()
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, detail="Failed to fetch data"
    )


# Get Capitalization Table

@carta_api_router.get(
    "/investors/firms/{firm_id}/funds/{fund_id}/investments/{company_id}/capitalizationTables/{cap_table_id}")
async def get_capitalization_table(
    firm_id: str,
    fund_id: str,
    company_id: str,
    cap_table_id: str,
    access_token: Annotated[str, Depends(oauth2_scheme)],
    page_size: int = Query(10, ge=1, le=100),
    page_token: str = None,
):
    url = f"{BASE_API_URL}/investors/firms/{firm_id}/funds/{fund_id}/investments/{company_id}/capitalizationTables/{cap_table_id}"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}",  # Use the actual token here
    }
    params = {"pageSize": page_size, "pageToken": page_token}
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers, params=params)
        if resp.is_success:
            return resp.json()
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, detail="Failed to fetch data"
    )


# Get Stakeholder Capitalization Table

@carta_api_router.get(
    "/investors/firms/{firm_id}/funds/{fund_id}/investments/{company_id}/stakeholderCapitalizationTables/{cap_table_id}")
async def get_stakeholder_capitalization_table(
    firm_id: str,
    fund_id: str,
    company_id: str,
    cap_table_id: str,
    access_token: Annotated[str, Depends(oauth2_scheme)],
    page_size: int = Query(10, ge=1, le=100),
    page_token: str = None,
):
    url = f"{BASE_API_URL}/investors/firms/{firm_id}/funds/{fund_id}/investments/{company_id}/stakeholderCapitalizationTables/{cap_table_id}"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}",  # Use the actual token here
    }
    params = {"pageSize": page_size, "pageToken": page_token}
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers, params=params)
        if resp.is_success:
            return resp.json()
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, detail="Failed to fetch data"
    )


#----------------------------------------------------------------------------------------------------------------------


from db.mongo import client,get_async_mongodb
### --------------   Handling Mongo DB  collections -------------###
# MongoDB async client setup using motor
# mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGODB_URL)
# db_async = mongo_client[MONGODB_DB_NAME]







# async def manage_mongo_collection(collection_name: str, data: List[dict]):
#     """Drop the existing collection, recreate it, and insert the provided data."""
#     # Drop the collection if it exists
#     if collection_name in db.list_collection_names():
#         db.drop_collection(collection_name)
    
#     # Create a new collection
#     collection = db[collection_name]
    
#     # Insert data into the collection
#     if data:
#         collection.insert_many(data)
#     return {"message": f"Collection '{collection_name}' has been created and data inserted."}

# @carta_data_router.post("/mongo/investors/firms")
# async def dump_firms_to_mongo(access_token: Annotated[str, Depends(oauth2_scheme)], collection_name: str):
#     url = f"{BASE_API_URL}/investors/firms"
    
#     headers = {
#         "accept": "application/json",
#         "Authorization": f"Bearer {access_token}",
#     }

#     async with httpx.AsyncClient() as client:
#         resp = await client.get(url, headers=headers)
#         if resp.is_success:
#             firms_data = resp.json()
#             # Manage MongoDB collection
#             result = await manage_mongo_collection(collection_name, firms_data)
#             return result
    
#     raise HTTPException(
#         status_code=resp.status_code,
#         detail=f"Failed to fetch data: {resp.text}"
#     )



#####   mongo for storing List Firm
async def manage_mongo_collection(collection_name: str, data: List[dict]):
    """Drop the existing collection, recreate it, and insert the provided data."""
    # Drop the collection if it exists
    if collection_name in db.list_collection_names():
        db.drop_collection(collection_name)
    
    # Create a new collection
    collection = db[collection_name]
    
    # Insert data into the collection
    if data:
        collection.insert_one(data)
    return {"message": f"Collection '{collection_name}' has been created and data inserted."}

@mongo_db_router.post("/mongo/investors/firms")
async def dump_firms_to_mongo(access_token: str = Depends(oauth2_scheme)):
    url = f"{BASE_API_URL}/investors/firms"  # Adjust the base URL as needed
    collection_name = "list_firms"  # Define the collection name directly in the API

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}",
    }

    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)
        if resp.is_success:
            firms_data = resp.json()
            # Manage MongoDB collection using the fixed collection name
            result = await manage_mongo_collection(collection_name, firms_data)
            return result
    
    raise HTTPException(
        status_code=resp.status_code,
        detail=f"Failed to fetch data: {resp.text}"
    )



# ----- mongo for storing list funds
# Function to store funds in MongoDB with a specified collection name (drops the collection if it already exists)
async def store_funds_in_db(funds: List[Dict], collection_name: str="list_funds"):
    collection = db[collection_name]
    
    # Drop the collection if it exists
    if collection_name in db.list_collection_names():
        collection.drop()
        logging.info(f"Collection {collection_name} dropped.")
    
    # Ensure funds is a list of dictionaries (documents)
    if not isinstance(funds, list) or not all(isinstance(fund, dict) for fund in funds):
        raise ValueError("Funds data must be a list of dictionaries")
    
    # Insert funds data into the new collection
    if funds:  # Insert only if there are documents
        collection.insert_many(funds)
        logging.info(f"Funds data inserted into {collection_name} collection.")
    else:
        logging.warning(f"No funds data to insert into {collection_name}.")

# FastAPI endpoint to fetch and store funds in MongoDB
@mongo_db_router.post("/investors/firms/funds/store", response_model=str)
async def fetch_and_store_funds( token: str = Depends(oauth2_scheme),):
    try:
        # Fetch firm IDs from MongoDB
        firm_ids = await get_firm_ids_from_db()

        if not firm_ids:
            raise HTTPException(status_code=404, detail="No firm_ids found in the database.")
        
        tasks = []

        # For debugging: limit the number of firm_ids, e.g., first firm_id.
        limited_firm_ids = firm_ids[:1]  # Use more firm_ids as necessary.
        
        # Create tasks for each firm_id to fetch its funds
        for firm_id in limited_firm_ids:
            tasks.append(fetch_funds_for_firm(firm_id, token))
        
        # Await all tasks to fetch funds
        funds = await asyncio.gather(*tasks)

        # Debug: log the structure of 'funds' before processing
        logging.info(f"Funds data structure before flattening: {funds}")

        # Flatten the list of funds, handling cases where the response is nested
        all_funds = [fund for firm_funds in funds for fund in (firm_funds if isinstance(firm_funds, list) else [firm_funds])]
        
        # Store the funds in the specified MongoDB collection
        await store_funds_in_db(all_funds)  # Pass the collection_name here
        # await get_mongo_db_collection(all_funds, collection_name)

        return f"Funds data successfully stored in MongoDB collection 'list_funds'."
    except HTTPException as e:
        logging.error(f"HTTP exception: {str(e)}")
        raise e
    except Exception as e:
        logging.error(f"General exception: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))








app.include_router(carta_data_router)
app.include_router(carta_api_router)
app.include_router(access_token)
# app.include_router(aws_db_router)
app.include_router(mongo_db_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("carta_hc:app", host="0.0.0.0", port=8000, reload=True)

