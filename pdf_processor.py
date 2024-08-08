import requests
import pandas as pd
import tabula
from io import BytesIO

def download_pdf(url):
    print(f"Downloading PDF from {url}")
    response = requests.get(url)
    response.raise_for_status()
    return BytesIO(response.content)

def extract_watchhouse_data(pdf_content):
    print("Extracting data from PDF")
    # Read all pages of the PDF
    tables = tabula.read_pdf(pdf_content, pages='all', multiple_tables=True)
    
    print(f"Number of tables extracted: {len(tables)}")
    
    # Combine all tables into one DataFrame
    df = pd.concat(tables, ignore_index=True)
    
    print("Combined DataFrame structure:")
    print(df.info())
    
    print("\nFirst few rows of the combined DataFrame:")
    print(df.head().to_string())
    
    # Clean up column names
    df.columns = df.columns.str.strip()
    
    # Filter rows for individual watchhouses and the Queensland total
    df = df[df['Unnamed: 0'].notna() & (df['Unnamed: 0'] != 'All Watchhouses')]
    
    # Rename columns for clarity
    df = df.rename(columns={
        'Unnamed: 0': 'Location',
        'Age': 'Age Group',
        'Total in': 'Total in Custody',
        'Unnamed: 1': 'Male',
        'Gender': 'Female',
        'Unnamed: 2': 'Other Gender',
        'First Nations Status': 'First Nations',
        'Unnamed: 3': 'Other Status',
        'In Custody (Days)': 'Custody Days',
        'Longest Days': 'Longest Stay'
    })
    
    # Split the 'First Nations' column into two
    if 'First Nations' in df.columns:
        df[['First Nations', 'Non Indigenous']] = df['First Nations'].str.split(expand=True)
    else:
        print("Warning: 'First Nations' column not found")
    
    # Split the 'Custody Days' column
    if 'Custody Days' in df.columns:
        custody_days = df['Custody Days'].str.split(expand=True)
        if custody_days.shape[1] == 3:
            df[['0-2 Days', '3-7 Days', 'Over 7 Days']] = custody_days
        else:
            print(f"Warning: 'Custody Days' column has {custody_days.shape[1]} parts instead of the expected 3")
            print(custody_days.head())
    else:
        print("Warning: 'Custody Days' column not found")
    
    # Convert numeric columns to integers
    numeric_columns = ['Total in Custody', 'Male', 'Female', 'Other Gender', 'First Nations', 'Non Indigenous', 
                       'Other Status', '0-2 Days', '3-7 Days', 'Over 7 Days', 'Longest Stay']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        else:
            print(f"Warning: Column '{col}' not found")
    
    return df

# URL of the PDF
pdf_url = "https://open-crime-data.s3.ap-southeast-2.amazonaws.com/Crime%20Statistics/Persons%20Currently%20In%20Watchhouse%20Custody.pdf"

try:
    # Download and process the PDF
    pdf_content = download_pdf(pdf_url)
    df = extract_watchhouse_data(pdf_content)

    # Display summary of the data
    print(f"\nTotal number of entries: {len(df)}")
    print("\nColumns in the dataset:")
    print(df.columns.tolist())

    print("\nFirst few rows of data:")
    print(df.head().to_string())

    if 'QUEENSLAND' in df['Location'].values:
        print("\nQueensland total:")
        print(df[df['Location'] == 'QUEENSLAND'].to_string(index=False))
    else:
        print("\nWarning: Queensland total not found in the data")

    print("\nSummary statistics for numeric columns:")
    print(df.describe())

    # Save to CSV
    csv_filename = 'watchhouse_data.csv'
    df.to_csv(csv_filename, index=False)
    print(f"\nCSV file '{csv_filename}' has been created.")

except Exception as e:
    print(f"An error occurred: {str(e)}")
    print("Please check the PDF structure and update the script accordingly.")