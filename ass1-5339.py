import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO
import os
import re

# Configuration
TARGET_URL = "https://data.nsw.gov.au/data/dataset/fuel-check"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
}
YEAR_FILTER = ['2024', '2025']
DATE_FIELDS = ['PriceUpdatedDate']
PRICE_FIELDS = ['Price']
OUTPUT_PATH = 'final_fuel_data.csv'

# Clean datetime fields: keep only the date part
def clean_datetime_fields(df, fields):
    for field in fields:
        if field in df.columns:
            df[field] = pd.to_datetime(df[field], errors='coerce').dt.date
    return df

# Remove exact duplicate rows from a DataFrame.
def remove_duplicates(df):
    before = df.shape[0]
    df = df.drop_duplicates().reset_index(drop=True)
    after = df.shape[0]
    print(f"Removed {before - after} duplicate rows")
    return df
    

# Check for missing values in the DataFrame
def check_missing_values(df):
    # Get total number of rows
    total_rows = len(df)
    
    # Calculate missing values for each column
    missing_values = df.isnull().sum()
    missing_percentage = (missing_values / total_rows * 100).round(2)
    
    # Create a summary DataFrame
    missing_info = pd.DataFrame({
        'Missing Values': missing_values,
        'Missing Percentage': missing_percentage
    })
    
    # Sort by missing percentage in descending order
    missing_info = missing_info.sort_values('Missing Percentage', ascending=False)
    
    # Only show columns with missing values
    missing_info = missing_info[missing_info['Missing Values'] > 0]
    
    if len(missing_info) > 0:
        print("\nMissing Values Summary:")
        print(missing_info)
        print(f"\nTotal number of rows: {total_rows}")
    else:
        print("\nNo missing values found in the dataset!")
    
    return missing_info

# Check for invalid values in the DataFrame
def check_invalid_values(df):
    total_rows = len(df)
    invalid_summary = {}
    
    # 只检查Price字段
    if 'Price' in df.columns:
        # Convert to numeric, coerce errors to NaN
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
        
        # Count invalid values (NaN or negative)
        invalid_mask = df['Price'].isna() | (df['Price'] < 0)
        invalid_count = invalid_mask.sum()
        
        if invalid_count > 0:
            invalid_percentage = (invalid_count / total_rows * 100).round(2)
            invalid_summary['Price'] = {
                'Invalid Values': invalid_count,
                'Invalid Percentage': invalid_percentage,
                'Min Value': df['Price'].min(),
                'Max Value': df['Price'].max()
            }
    
    if invalid_summary:
        print("\nPrice Field Invalid Values Summary:")
        for field, stats in invalid_summary.items():
            print(f"\n{field}:")
            print(f"  Invalid Values: {stats['Invalid Values']}")
            print(f"  Invalid Percentage: {stats['Invalid Percentage']}%")
            print(f"  Value Range: {stats['Min Value']} to {stats['Max Value']}")
        print(f"\nTotal number of rows: {total_rows}")
    else:
        print("\nNo invalid values found in Price field!")
    
    return invalid_summary

# Validate postcode format
def check_invalidate_postcodes(df):
    if 'Postcode' in df.columns:
        # Convert to string
        df['Postcode'] = df['Postcode'].astype(str)
        
        # Check for invalid postcodes (not 4 digits)
        invalid_mask = ~df['Postcode'].str.match(r'^\d{4}$')
        invalid_count = invalid_mask.sum()
        
        if invalid_count > 0:
            print(f"\nFound {invalid_count} invalid postcodes:")
            invalid_postcodes = df[invalid_mask]['Postcode'].unique()
            print("Invalid postcode values:", invalid_postcodes)
            
            # Convert valid postcodes to integers
            df.loc[~invalid_mask, 'Postcode'] = df.loc[~invalid_mask, 'Postcode'].astype(int)
        else:
            print("\nAll postcodes are valid 4-digit numbers")
            df['Postcode'] = df['Postcode'].astype(int)
    
    return df

# Download and load data
def download_fuel_data():
    all_data = []

    try:
        response = requests.get(TARGET_URL, headers=HEADERS)
        response.raise_for_status()
        print(f"Response status code: {response.status_code}")

        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.select('#dataset-resources > ul > li > div > ul > li:nth-child(2) > a')

        for link in links:
            file_url = link['href']
            file_name = file_url.split('/')[-1].lower()

            if any(year in file_name for year in YEAR_FILTER):
                try:
                    file_response = requests.get(file_url, headers=HEADERS)
                    file_response.raise_for_status()

                    if file_name.endswith('.csv'):
                        df = pd.read_csv(BytesIO(file_response.content))
                    elif file_name.endswith('.xlsx'):
                        df = pd.read_excel(BytesIO(file_response.content))
                    else:
                        print(f"Unsupported file type: {file_url}")
                        continue

                    all_data.append(df)
                    print(f"Downloaded successfully: {file_url}")
                except Exception as e:
                    print(f"Error reading file {file_url}: {e}")
    except Exception as e:
        print(f"Failed to access the target page: {e}")

    print(f"\nTotal number of dataframes collected: {len(all_data)}")
    return all_data

# Save all DataFrames to CSV file
def save_to_csv(dataframes, output_file):
    if not dataframes:
        print("No data to save")
        return
        
    # Combine all dataframes
    print("\nCombining all dataframes...")
    combined_df = pd.concat(dataframes, ignore_index=True)
    print(f"Combined DataFrame shape: {combined_df.shape}")
    
    # Remove any duplicates from the combined data
    print("\nRemoving duplicates from combined data...")
    combined_df = remove_duplicates(combined_df)
    
    # Sort by date if PriceUpdatedDate exists
    if 'PriceUpdatedDate' in combined_df.columns:
        print("\nSorting data by date...")
        combined_df = combined_df.sort_values('PriceUpdatedDate')
    
    # Data quality checks
    print("\nPerforming data quality checks...")
    check_missing_values(combined_df)
    check_invalid_values(combined_df)
    check_invalidate_postcodes(combined_df)
    
    # Save to CSV
    print(f"\nSaving data to {output_file}...")
    
    # Remove existing file if it exists
    if os.path.exists(output_file):
        os.remove(output_file)
    
    # Save with UTF-8 encoding to handle special characters
    combined_df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"Successfully saved {output_file}")

# Standardize suburb names to title case
def standardize_suburb_names(df):
    if 'Suburb' in df.columns:
        # Convert to title case, handling special cases
        df['Suburb'] = df['Suburb'].apply(lambda x: x.title() if pd.notna(x) else x)
        print("Standardized suburb names to title case")
    return df

# Replace 'NEW SOUTH WALES' with 'NSW' in Address column
def standardize_address_state(df):
    if 'Address' in df.columns:
        # Replace 'NEW SOUTH WALES' with 'NSW' (case insensitive)
        df['Address'] = df['Address'].str.replace('NEW SOUTH WALES', 'NSW', case=False)
        print("Standardized state name in addresses")
    return df

# Clean address by removing postcode and state
def clean_address(df):
    if 'Address' in df.columns and 'Postcode' in df.columns and 'Suburb' in df.columns:
        # First remove postcode from the end
        df['Address'] = df.apply(lambda row: re.sub(r'\s+' + str(row['Postcode']) + r'\s*$', '', row['Address'], flags=re.IGNORECASE), axis=1)
        
        # Clean up any extra spaces and commas
        df['Address'] = df['Address'].str.replace(r'\s*,\s*$', '', regex=True)  # Remove trailing comma
        df['Address'] = df['Address'].str.strip()
        
        # Now extract state (last 3 characters) and create State column
        df['State'] = df['Address'].str.extract(r'(\w{3})\s*$', expand=False)
        
        # Remove state from the end of address
        df['Address'] = df['Address'].str.replace(r'\s+\w{3}\s*$', '', regex=True)
        df['Address'] = df['Address'].str.strip()
        
        # Remove suburb from the end of address using Suburb column
        df['Address'] = df.apply(lambda row: re.sub(r'\s*,?\s*' + re.escape(str(row['Suburb'])) + r'\s*$', '', row['Address'], flags=re.IGNORECASE), axis=1)
        df['Address'] = df['Address'].str.strip()
        
        print("Cleaned addresses by removing postcode, state, and suburb")
    return df

# Main execution
if __name__ == "__main__":
    all_data = download_fuel_data()

    if all_data:
        print("\nData Integration and Cleaning...")
        for i in range(len(all_data)):
            # Data Integration and Cleaning
            all_data[i] = clean_datetime_fields(all_data[i], DATE_FIELDS)
            all_data[i] = remove_duplicates(all_data[i])
            all_data[i] = standardize_suburb_names(all_data[i])
            all_data[i] = standardize_address_state(all_data[i])
            all_data[i] = clean_address(all_data[i])

        # Save the cleaned data
        save_to_csv(all_data, OUTPUT_PATH)
    else:
        print("No data was collected.")