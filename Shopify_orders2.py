print("Script started")
import os
import glob
import pandas as pd
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
# --- Get latest CSV file ---
def get_latest_file(folder_path, extension="csv"):
    print(f"Looking in folder: {folder_path}")
    print(f"Full search path: {os.path.join(folder_path, f'*.{extension}')}")
    list_of_files = glob.glob(os.path.join(folder_path, f"*.{extension}"))
    if not list_of_files:
        print("No CSV files found in the folder.")
        return None
    latest_file = max(list_of_files, key=os.path.getctime)
    print(f"Latest CSV selected: {latest_file}")
    return latest_file
# --- Clean phone numbers ---
def clean_phone(phone):
    if pd.isna(phone):
        return ''
    phone = re.sub(r'\D', '', str(phone))  # Remove non-digit characters
    
    # Remove Bangladesh country code '880' if present
    if phone.startswith('880'):
        phone = phone[3:]
    # Remove Pakistan country code '92' if present
    elif phone.startswith('92'):
        phone = phone[2:]
    # Remove Saudi country code '966' if present
    if phone.startswith('966'):
        phone = phone[4:]
    
    return phone
# --- Format dates ---
def format_date(date_str):
    try:
        date_obj = pd.to_datetime(date_str)
        import platform
        if platform.system() == 'Windows':
            return date_obj.strftime("%B %d, %Y").replace(" 0", " ")
        else:
            return date_obj.strftime("%B %-d, %Y")
    except Exception:
        return date_str

# --- Process Shopify CSV ---
def process_shopify_csv(csv_path):
    df = pd.read_csv(csv_path)
    df = df[['Name', 'Created at','Shipping Name', 'Shipping Phone', 'Total', 'Lineitem name', 'Lineitem quantity']]
    df['Created at'] = df['Created at'].apply(format_date)
    df['Shipping Phone'] = df['Shipping Phone'].apply(clean_phone)
    # Fill missing info across order rows
    df[['Created at','Shipping Name', 'Shipping Phone', 'Total']] = df.groupby('Name')[['Created at','Shipping Name', 'Shipping Phone', 'Total']].transform('first')
    grouped = {}
    for _, row in df.iterrows():
        name = row['Name']
        if name not in grouped:
            grouped[name] = {
                'Order': name,
                'Date': row['Created at'],
                'Customer': row['Shipping Name'],
                'Phone': str(row['Shipping Phone']),
                'Total': row['Total'],
                'Products': []
            }
        grouped[name]['Products'].append((row['Lineitem name'], row['Lineitem quantity']))

    final_data = []
    for order in grouped.values():
        flat_row = [order['Order'], order['Date'], order['Customer'], order['Phone'], order['Total']]
        for pname, qty in order['Products']:
            flat_row += [pname, qty]
        final_data.append(flat_row)

    max_products = max(len(order['Products']) for order in grouped.values())
    headers = ['Order No', 'Date','Customer Name', 'Phone Number', 'Revenue']
    for i in range(1, max_products + 1):
        headers += [f'Product {i}', f'Qty {i}']

    return headers, final_data

# --- Append to Google Sheet ---
def append_to_existing_google_sheet(headers, data):
    sheet_id = "12t9TP56uhanpZILJ5uG2i8YgrIhDJCp66MAI4biZJGQ"
    tab_name = "Exported Orders"
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('hopeful-disk-462006-f0-31fae0b7c9f8.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id).worksheet(tab_name)
    existing_data = sheet.get_all_values()
    if not existing_data:
        print("Sheet is empty. Inserting headers...")
        sheet.append_row(headers)

    for row in data:
        sheet.append_row(row)

    print(f"Successfully appended {len(data)} orders to '{tab_name}'.")

# --- Main execution ---
if __name__ == "__main__":
    folder_path = r"W:\Data Entry Work"  # C hange to your folder path if needed
    latest_csv = get_latest_file(folder_path)
    if latest_csv:
        headers, data = process_shopify_csv(latest_csv)
        append_to_existing_google_sheet(headers, data)

