# 1 - imports
from decouple import config
import pysftp
import fnmatch
import importlib
import datetime
import traceback
import os 
import logging
import paramiko
from shutil import copy2, SameFileError
from sqlalchemy import text

from google.cloud import storage

Repository = importlib.import_module('services.repository')

Session = importlib.import_module('entities.base')
Company = importlib.import_module('entities.Company')

def run_agent(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    logging.info("Running Agent")
    print("Running Agent")
    # print("on directory {0}", os.getcwd())
    # 2 - Get custom company
    request_json = request.get_json()
    request_args = request.args
    
    customCompany = None
    if request_json and 'company' in request_json:
        customCompany = request_json['company']
    elif request_args and 'company' in request_args:
        customCompany = request_args['company']

    logging.info(f"Custom Company: {0}", customCompany)
    print("Print - Custom company: {0}", customCompany)
    # 3 - extract a session
    
    session = Session.Session()
    

    # 4 - extract all companies
    try:
        logging.info("Beginning Processing Agent.")
        print("Beginning Processing Agent.")
        target = config("target")
        # companies = Repository.select(Company.Company, session, Company.Company.SNOWTarget, target)
        sql = text("select * from Companies WHERE SNOWTarget='prod'")
        result = session.execute(sql)
        companies = [row for row in result]
        logging.info(f"Retrieving companies: {0}", companies)
        print(f"Retrieving companies: {0}", companies)

        # 5 - Check if custom company was search
        if customCompany is not None:
            companies = []
            companies = Repository.select2condition(Company.Company, session, Company.Company.SNOWTarget, target, Company.Company.Name, customCompany)
            companies = companies if isinstance(companies, list) else [companies]
            logging.info(f"Retrieving custom companies: {0}", companies)
            print(f"Retrieving custom companies: {0}", companies)
        for company in companies:
            print("Retrieving files for {0}", company.Name)
            logging.info(f"Retrieving files for {0}", company.Name)
            print(f"Retrieving files for {0}", company.Name)

            RetrieveFilesForCompany(company)

            # modifying LastFetchDate attribute for a given company in the database      
            # record = session.query(Company.Company).filter(Company.Company.Name == company.Name).one()
            # record.LastFetchDate = datetime.datetime.now()
            # session.commit() 

        session.close()

    except Exception as e:
        logging.info(f"Exception not caught in Agent: {e}")
        print(f"Exception not caught in Agent: {e}")
        logging.error(traceback.format_exc())
        print(traceback.format_exc())
    
    # from os import walk

    # importPath = config("ImportPath")
    # _, _, filenames = next(walk(importPath))
    return f'response: OK'

def RetrieveFilesForCompany(company):
    try: 
        company_ident = ''
        if company.Name == 'Migros': company_ident = 'MIGROS'
        elif company.Name == 'ADT SECURITY': company_ident = 'ADT'
        else: company_ident = company.Ident
        print(company_ident)
        # # Location where we copy download file .csv of sftp server
        localPath = config("LocalPath")
        localFilePath = "{0}{1}/".format(localPath, company.Ident)
        print(localFilePath)
        bucketFilePath = "clientData/{0}/".format(company.Ident)
        # # .cvs file path with short name and use for cache
        importPath8 = config("ImportPath8")
        importPath16 = config("ImportPath16")

        # Create Directory Structure
        # path = os.path.join(localFilePath, "\\") 
        # os.mkdir(path) 

        sftpUserName = config("sftpUserName")
        sftpHost = config("sftpHost")
        sftpPort = int(config("sftpPort"))
        sftpKey = config("sftpKey")
        # filePath = "/home_of_snow_atosglobal{0}/{1}/snow_export/".format("" if company.SNOWTarget == "prod" else company.SNOWTarget, company.Ident)
        filePath = "/home_of_audl_{0}_prod/data_delivery/".format(company_ident.lower())
        print(filePath)
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        # sftpKey need to be on Secret manager
        
        with pysftp.Connection(host=sftpHost, port=sftpPort, username=sftpUserName, private_key=sftpKey, cnopts=cnopts) as sftpClient:
            files = sftpClient.listdir(f'{filePath}')
            print('connected')
            # Start loop for all files company inside sftp server
            for filename in files:
                print("File: {0}", filename)
                # if fnmatch.fnmatch(filename, "*.csv"):
                #     # File path for specific company file .csv
                sftpFilePath = filePath + filename
                # Copy download file to directory
                copy_file_destination = localFilePath + filename
                if not os.path.exists(localFilePath):
                    os.makedirs(localFilePath)
                # with open(copy_file_destination, "w", encoding="utf-8") as fs:
                # File download to folder in server ¨fs¨
                sftpClient.get(sftpFilePath, copy_file_destination)
                    # fs.Close()
                logging.info(f"Retrieved successfully file: {copy_file_destination}")
                print(f"Retrieved successfully file: {copy_file_destination}")
                    
                print(f"First Upload to bucket: {filename}")
                upload_to_bucket("gce-master-data", copy_file_destination, bucketFilePath + filename)
                
                # Convert File to UTF8 and relocate to proper Location
                with open(copy_file_destination, "r", encoding="utf-8") as reader:
                    reader.close()
                    # Open copy download file from local server
                    # Convert File to UTF8 and relocate to proper Location
                    # Rename file removing utf8, snow_atosglobal{0}_ and spaces
                    newFileName8 = filename.replace("_utf8","")
                    newFileName8 = newFileName8.replace(".", "_utf8.")
                    print(newFileName8)
                    # copy file with encoding utf8 and into directory
                    if not os.path.exists(importPath8):
                        os.makedirs(importPath8)
                    CopyContents(copy_file_destination, importPath8 + newFileName8)
                logging.info(f"Filed converted successfully: {importPath8 + newFileName8}")
                print(f"Filed converted successfully: {importPath8 + newFileName8}")
                
                print(f"Second upload to bucket: {newFileName8}")
                bucketFilePath2 = "clientData_utf8/{0}/".format(company.Ident)
                upload_to_bucket("gce-master-data", importPath8 + newFileName8, bucketFilePath2 + newFileName8)

                # Convert File to UTF16 and relocate to proper Location
                with open(importPath8 + newFileName8, "r", encoding="utf-16") as reader:
                    reader.close()
                    # Open copy download file from local server
                    # Convert File to UTF16 and relocate to proper Location
                    # Rename file removing utf8, snow_atosglobal{0}_ and spaces
                    newFileName16 = newFileName8.replace("_utf8", "_utf16")
                    print(newFileName16)
                    # copy file with encoding utf8 to utf 16 and into directory
                    if not os.path.exists(importPath16):
                        os.makedirs(importPath16)
                    CopyContents(importPath8 + newFileName8, importPath16 + newFileName16)
                logging.info(f"Filed converted successfully: {importPath16 + newFileName16}")
                print(f"Filed converted successfully: {importPath16 + newFileName16}")
                
                print(f"Third upload to bucket: {newFileName16}")
                bucketFilePath3 = "clientData_utf16/{0}/".format(company.Ident)
                upload_to_bucket("gce-master-data", importPath16 + newFileName16, bucketFilePath3 + newFileName16)
                    
                
    except Exception as e:
        logging.info(f"Exception not caught in Agent: {e}")
        logging.error(traceback.format_exc())

def CopyContents(src, dst):
    try:
        copy2(src,dst)
    # If source and destination are same 
    except SameFileError as e:
        logging.info(f"Source and destination represents the same file: {e}")
        logging.error(traceback.format_exc())
    # If there is any permission issue 
    except PermissionError: 
        logging.info(f"Permission denied: {e}")
        logging.error(traceback.format_exc())
    except IOError as e:
        logging.info(f"IOError: {e}")
        logging.error(traceback.format_exc())


def upload_to_bucket(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )
