from sqlalchemy import create_engine
from sqlalchemy import engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from decouple import config

"""
Access the payload for the given secret version if one exists. The version
can be a version number as a string (e.g. "5") or an alias (e.g. "latest").

The function is doing this work outside of the Python function itself. 
This will speed up our function by only accessing the secret once 
when the Cloud Function is instantiated, and not once for every single request.
"""

# Import the Secret Manager client library.
import google.cloud.logging # work around 'cause secretmanager not importing
from google.cloud import secretmanager

# Local Testing ONLY
# import os
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:\\Users\\a796994\\Documents\\dmitriy\\python\\AUDLCacheExtraction\\secret-manager.json"
# END Local testing

# Create the Secret Manager client.
client = secretmanager.SecretManagerServiceClient()

project_id = 562088991674
secret_id = "global-correlation-engine-8-DataMasterTest-Password"
version_id = "latest"
# Build the resource name of the secret version.
name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

# Access the secret version.
response = client.access_secret_version(request={"name": name})

# storing secret payload.
payload = response.payload.data.decode("UTF-8")

# Starting sql conection
connection_name = config("Connection")
db_name = config("Database")
db_user = config("User")
db_password = payload

# If your database is MySQL, uncomment the following two lines:
driver_name = 'mysql+pymysql'
query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})

# Local Testing ONLY
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:\\Users\\a796994\\Documents\\dmitriy\\python\\AUDLCacheExtraction\\cloud-sql.json"
# END Local testing
print("before")
engine = create_engine(
      engine.url.URL(
        drivername=driver_name,
        username=db_user,
        password=db_password,
        database=db_name,
        query=query_string,
      ),
      pool_size=5,
      max_overflow=2,
      pool_timeout=30,
      pool_recycle=1800
    )
Session = sessionmaker(bind=engine)
print("after")
Base = declarative_base()