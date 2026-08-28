import configparser
import boto3
from datetime import datetime
import pandas as pd
import boto3
import io
from datetime import datetime
import smtplib
import os
from email.message import EmailMessage
from dotenv import load_dotenv
# Step 1: Create a ConfigParser object
config = configparser.ConfigParser()
# Step 2: Read the config file
config.read('config.ini')
# Step 3: Access values using section and key
aws_key = config['aws']['access_key']
aws_secret = config['aws']['secret_key']
region = config.get('aws', 'region') # alternate way
db_host = config['database']['host']
db_port = config.getint('database', 'port') # automatic int conversion
'''print("AWS Key:", aws_key)
print("Database Host:", db_host)

print(config.sections()) 
print(config.options('aws')) 
print(config.has_option('aws', 'access_key')) '''


### boto 3 code
S3 = boto3.client(
"s3",
aws_access_key_id=aws_key,
aws_secret_access_key=aws_secret
)



# Configuration
BUCKET_NAME = 'amzn-pyth-shubham-patil-de'
DATE_STR = datetime.now().strftime('%Y%m%d')
DISPLAY_DATE = datetime.now().strftime('%Y-%m-%d')
date_str = datetime.now().strftime('%Y%m%d')

def get_master_data():
    """Reads product master from S3 once for efficiency."""
    key_path = f'NamasteKart/incoming_files/{date_str}/product_master.csv'
    
    # Option 2: If you prefer to hardcode it for testing
    # key_path = 'NamasteKart/incoming_files/20260629/product_master.csv'
    
    try:
        obj = S3.get_object(Bucket=BUCKET_NAME, Key=key_path)
        return pd.read_csv(io.BytesIO(obj['Body'].read()))
    except Exception as e:
        print(f"Error: Could not find product_master.csv at {key_path}")
        raise e

def validate_orders(df_orders, df_master):
    """Applies validation rules and returns rejected rows if any."""
    df_orders = df_orders.copy()
    reasons = []

    def check_row(row):
        errors = []
        if row.isnull().any(): errors.append("Empty field")
        if row['product_id'] not in df_master['product_id'].values:
            errors.append("Invalid Product ID")
        else:
            price = df_master.loc[df_master['product_id'] == row['product_id'], 'price'].values[0]
            if row['sales'] != (price * row['quantity']):
                errors.append("Incorrect total sales amount")
        if pd.to_datetime(row['order_date']) > datetime.now():
            errors.append("Future date")
        if row['city'] not in ['Mumbai', 'Bangalore']:
            errors.append("Invalid city")
        return "; ".join(errors) if errors else None

    df_orders['rejection_reason'] = df_orders.apply(check_row, axis=1)
    return df_orders[df_orders['rejection_reason'].notnull()]

def send_notification(total, success, failed):
    """Sends email summary to the business team."""
    load_dotenv(dotenv_path='.env')  # This loads the variables from the .env file

# Now your existing line will work perfectly!
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    SMTP_SERVER = "smtp.gmail.com"
    SMTP_PORT = 587
    
    msg = EmailMessage()
    msg['Subject'] = f"validation email {DISPLAY_DATE}"
    msg['From'] = "ssdp14896@gmail.com"
    msg['To'] = "patilss14896@gmail.com"
    
    if total == 0:
        body = f"No incoming files found for {DISPLAY_DATE}."
    else:
        body = (f"Total {total} incoming files processed.\n"
                f"{success} files passed validation.\n"
                f"{failed} files failed validation.")
    
    msg.set_content(body)
    # Configure your SMTP server here
    try:
        # Use SMTP_SSL for port 465, or keep SMTP for port 587
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.ehlo()
        server.starttls()
        server.ehlo()
        
        # Explicitly verify these are not None
        print(f"DEBUG: User={EMAIL_USER}") 
        
        server.login(EMAIL_USER, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")

def process_pipeline():
    master_df = get_master_data()
    print('Line 112 successful')
    prefix = f'NamasteKart/incoming_files/{date_str}/'
    files = S3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix).get('Contents', [])
    print('line number 115 :',files)
    if not files:
        send_notification(0, 0, 0)
        return

    success_count, fail_count = 0, 0

    for obj in files:
        file_key = obj['Key']
        file_name = file_key.split('/')[-1]
        
       # print(file_name)
        
        # Read and Validate
        if file_name.endswith('.csv') and file_name != 'product_master.csv':
            print(f"Processing order file: {file_name}")
            csv_data = S3.get_object(Bucket=BUCKET_NAME, Key=file_key)['Body'].read()
            df = pd.read_csv(io.BytesIO(csv_data))
            rejected = validate_orders(df, master_df)

        
        
            if rejected.empty:
                S3.copy_object(Bucket=BUCKET_NAME, CopySource={'Bucket': BUCKET_NAME, 'Key': file_key}, 
                           Key=f'NamasteKart/success_files/{DATE_STR}/{file_name}')
                success_count += 1
            else:
                # Save error log
                error_csv = rejected.to_csv(index=False)
                S3.put_object(Bucket=BUCKET_NAME, Key=f'NamasteKart/rejected_files/{DATE_STR}/error_{file_name}', Body=error_csv)
            # Move original
                S3.copy_object(Bucket=BUCKET_NAME, CopySource={'Bucket': BUCKET_NAME, 'Key': file_key}, 
                           Key=f'NamasteKart/rejected_files/{DATE_STR}/{file_name}')
                fail_count += 1
        
            S3.delete_object(Bucket=BUCKET_NAME, Key=file_key)
        
            send_notification(len(files), success_count, fail_count)
        else:
            print(f"Skipping file: {file_name}")

if __name__ == "__main__":
    process_pipeline()
