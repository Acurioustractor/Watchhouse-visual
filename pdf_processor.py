import requests
import tabula
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import schedule
import time
import os
import csv
import re

def download_pdf(url, save_path):
    print(f"Downloading PDF from {url}")
    response = requests.get(url)
    response.raise_for_status()
    with open(save_path, 'wb') as file:
        file.write(response.content)
    print(f"PDF saved to {save_path}")

def pdf_to_csv(pdf_path, csv_path):
    print(f"Converting PDF to CSV")
    df = tabula.read_pdf(pdf_path, pages='all')[0]
    print("Columns in the extracted data:")
    print(df.columns)
    print("\nFirst few rows of data:")
    print(df.head())
    print("\nFull DataFrame:")
    print(df)
    
    try:
        # Extract detailed data
        queensland_row = df[df['Unnamed: 0'] == 'QUEENSLAND']
        print("\nQueensland row:")
        print(queensland_row)
        
        if queensland_row.empty:
            raise ValueError("Queensland row not found in the data")
        
        # Split combined First Nations Status data
        first_nations_status = queensland_row['First Nations Status'].iloc[0].split()
        first_nations = int(first_nations_status[0])
        non_indigenous = int(first_nations_status[1])

        # Split combined In Custody (Days) data
        in_custody_days = queensland_row['In Custody (Days)'].iloc[0].split()
        
        # Use the correct year format
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        data = {
            'Date': current_date,
            'Total Adults': int(queensland_row['Total in'].iloc[0]),
            'Total Children': int(queensland_row['Total in'].iloc[1]) if len(queensland_row) > 1 else 0,
            'Total in Custody': int(queensland_row['Total in'].iloc[0]) + (int(queensland_row['Total in'].iloc[1]) if len(queensland_row) > 1 else 0),
            'Adults Male': int(queensland_row['Unnamed: 1'].iloc[0]),
            'Adults Female': int(queensland_row['Gender'].iloc[0]),
            'Children Male': int(queensland_row['Unnamed: 1'].iloc[1]) if len(queensland_row) > 1 else 0,
            'Children Female': int(queensland_row['Gender'].iloc[1]) if len(queensland_row) > 1 else 0,
            'Adults First Nations': first_nations,
            'Adults Non-Indigenous': non_indigenous,
            'Children First Nations': int(queensland_row['First Nations Status'].iloc[1].split()[0]) if len(queensland_row) > 1 else 0,
            'Children Non-Indigenous': int(queensland_row['First Nations Status'].iloc[1].split()[1]) if len(queensland_row) > 1 else 0,
            'Adults 0-2 Days': int(in_custody_days[0]),
            'Adults 3-7 Days': int(in_custody_days[1]),
            'Adults Over 7 Days': int(in_custody_days[2]),
            'Children 0-2 Days': int(queensland_row['In Custody (Days)'].iloc[1].split()[0]) if len(queensland_row) > 1 else 0,
            'Children 3-7 Days': int(queensland_row['In Custody (Days)'].iloc[1].split()[1]) if len(queensland_row) > 1 else 0,
            'Children Over 7 Days': int(queensland_row['In Custody (Days)'].iloc[1].split()[2]) if len(queensland_row) > 1 else 0,
            'Longest Adult Stay': int(queensland_row['Longest Days'].iloc[0]),
            'Longest Child Stay': int(queensland_row['Longest Days'].iloc[1]) if len(queensland_row) > 1 else 0
        }
        
        update_watchhouse_csv(data, csv_path)
        print(f"Detailed CSV saved to {csv_path}")
        print(pd.DataFrame([data]))
    except Exception as e:
        print(f"Error processing PDF data: {e}")
        print("Unable to extract required information. Please check the PDF structure.")

def update_watchhouse_csv(data, csv_file):
    fieldnames = [
        'Date', 'Total Adults', 'Total Children', 'Total in Custody',
        'Adults Male', 'Adults Female', 'Children Male', 'Children Female',
        'Adults First Nations', 'Adults Non-Indigenous',
        'Children First Nations', 'Children Non-Indigenous',
        'Adults 0-2 Days', 'Adults 3-7 Days', 'Adults Over 7 Days',
        'Children 0-2 Days', 'Children 3-7 Days', 'Children Over 7 Days',
        'Longest Adult Stay', 'Longest Child Stay'
    ]
    
    # Ensure the date is in the correct format
    data['Date'] = datetime.strptime(data['Date'], '%Y-%m-%d').strftime('%Y-%m-%d')
    
    # Check if file exists, if not, create it with headers
    if not os.path.exists(csv_file):
        with open(csv_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
    
    # Append new data
    with open(csv_file, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerow(data)

def create_visualization(csv_path, image_path):
    print(f"Creating visualization")
    df = pd.read_csv(csv_path)
    
    # Clean up the date column
    def clean_date(date_str):
        try:
            # Try to parse the date string
            return pd.to_datetime(date_str, format='%Y-%m-%d').strftime('%Y-%m-%d')
        except ValueError:
            # If parsing fails, try to extract a valid date
            match = re.search(r'(\d{4}-\d{2}-\d{2})', str(date_str))
            if match:
                return match.group(1)
            else:
                return None

    df['Date'] = df['Date'].apply(clean_date)
    df = df.dropna(subset=['Date'])  # Remove rows with invalid dates

    try:
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    except ValueError as e:
        print(f"Error parsing dates: {e}")
        print("Date column contents after cleaning:")
        print(df['Date'])
        return
    
    # Ensure numeric columns are properly typed
    numeric_columns = ['Total in Custody', 'Total Adults', 'Total Children']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.sort_values('Date')  # Sort by date

    plt.figure(figsize=(12, 6))
    plt.plot(df['Date'], df['Total in Custody'], label='Total')
    plt.plot(df['Date'], df['Total Adults'], label='Adults')
    plt.plot(df['Date'], df['Total Children'], label='Children')
    plt.title('Watchhouse Occupancy Over Time')
    plt.xlabel('Date')
    plt.ylabel('Number of Persons')
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(image_path)
    print(f"Visualization saved to {image_path}")

def daily_task():
    pdf_url = "https://open-crime-data.s3.ap-southeast-2.amazonaws.com/Crime%20Statistics/Persons%20Currently%20In%20Watchhouse%20Custody.pdf"
    pdf_path = "watchhouse_data.pdf"
    csv_path = "watchhouse_data.csv"
    visualization_path = "watchhouse_occupancy_trend.png"

    download_pdf(pdf_url, pdf_path)
    pdf_to_csv(pdf_path, csv_path)
    create_visualization(csv_path, visualization_path)
    print(f"Daily task completed for {datetime.now().strftime('%Y-%m-%d')}")

def main():
    print("PDF Processor is running!")
    daily_task()  # Run once immediately
    # Uncomment the following lines to schedule daily runs
    # schedule.every().day.at("00:00").do(daily_task)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)

if __name__ == "__main__":
    main()