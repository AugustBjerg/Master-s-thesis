import pandas as pd
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
input_path = os.path.join(parent_dir, 'metadata', 'Metrics registration.xlsx')
output_path = os.path.join(parent_dir, 'metadata', 'Metrics registration.csv')
sheet_name = 'Sheet1'

def convert_xlsx_to_csv(input_path, output_path):
    # Read the Excel file from the specified sheet
    df = pd.read_excel(input_path, sheet_name=sheet_name)
    # Convert to CSV
    df.to_csv(output_path, index=False)

# Execute
convert_xlsx_to_csv(input_path, output_path)