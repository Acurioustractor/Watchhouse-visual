import tabula
import pandas as pd
import requests
from io import BytesIO

def download_pdf(url):
    print(f"Downloading PDF from {url}")
    response = requests.get(url)
    return BytesIO(response.content)

def safe_split(value, index, default=''):
    parts = str(value).split()
    return parts[index] if len(parts) > index else default

def process_watchhouse_data(df):
    processed_data = []
    current_watchhouse = None
    for _, row in df.iterrows():
        try:
            if pd.notna(row['Unnamed: 0']) and row['Unnamed: 0'] != 'All Watch-houses':
                current_watchhouse = row['Unnamed: 0']
                age_group = row['Age']
            elif current_watchhouse and pd.notna(row['Age']):
                age_group = row['Age']
            else:
                continue

            processed_data.append({
                'Watch-house': current_watchhouse,
                'Age Group': age_group,
                'Total': row['Total in'],
                'Male': row['Unnamed: 1'],
                'Female': row['Gender'],
                'First Nations': safe_split(row['First Nations Status'], 0),
                'Non Indigenous': safe_split(row['First Nations Status'], 1),
                '0-2 Days': safe_split(row['In Custody (Days)'], 0),
                '3-7 Days': safe_split(row['In Custody (Days)'], 1),
                'Over 7 Days': safe_split(row['In Custody (Days)'], 2),
                'Longest Stay': row['Longest Days']
            })
        except Exception as e:
            print(f"Error processing row: {row}")
            print(f"Error message: {str(e)}")
    return pd.DataFrame(processed_data)

def extract_watchhouse_data(pdf_content):
    print("Extracting data from PDF")
    dfs = tabula.read_pdf(pdf_content, pages='all', multiple_tables=True)
    
    print(f"Number of tables extracted: {len(dfs)}")
    for i, df in enumerate(dfs):
        print(f"\nTable {i+1} shape: {df.shape}")
        print(f"Table {i+1} columns: {df.columns.tolist()}")
        print(f"First few rows of Table {i+1}:")
        print(df.head().to_string())
    
    combined_df = pd.concat(dfs, ignore_index=True)
    
    processed_df = process_watchhouse_data(combined_df)
    
    return processed_df

def main():
    pdf_url = "https://open-crime-data.s3.ap-southeast-2.amazonaws.com/Crime%20Statistics/Persons%20Currently%20In%20Watchhouse%20Custody.pdf"
    pdf_content = download_pdf(pdf_url)
    
    watchhouse_data = extract_watchhouse_data(pdf_content)
    
    csv_filename = 'watchhouse_data.csv'
    watchhouse_data.to_csv(csv_filename, index=False)
    print(f"Data saved to {csv_filename}")
    
    print("\nSummary of Watch-house Data:")
    print(watchhouse_data.groupby('Age Group').agg({
        'Total': 'sum',
        'Male': 'sum',
        'Female': 'sum',
        'First Nations': 'sum',
        'Non Indigenous': 'sum'
    }))
    
    print("\nFirst few rows of data:")
    print(watchhouse_data.head().to_string())

    print("\nFull Dataset:")
    print(watchhouse_data.to_string())

if __name__ == "__main__":
    main()