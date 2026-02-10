import pandas as pd
import os
from loguru import logger

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
input_path = os.path.join(parent_dir, 'metadata', 'Metrics registration.xlsx')
output_path = os.path.join(parent_dir, 'metadata', 'Metrics registration.csv')
sheet_name = 'Sheet1'

def correct_vessel_propeller_shaft_revolutions_unit(df):
    # set the value for "unit" of Vessel Propeller Shaft Revolutions to "revs"
    df.loc[df['quantity_name'] == 'Vessel Propeller Shaft Revolutions', 'unit'] = 'revs'

    # add a parenthesis (cumulative) to the quantity_name for vessel propeller shaft revolutions
    df.loc[df['quantity_name'] == 'Vessel Propeller Shaft Revolutions', 'quantity_name'] = 'Vessel Propeller Shaft Revolutions (cumulative)'

    return df

def remove_redundant_turbocharger_qid(df):
    # remove the redundant qid for main engine turbocharger rotational speed (there are two, one of which does not have any observations)
    initial_shape = df.shape
    df = df[df['qid_mapping'] != '2::0::25::0_1::2::0::3::0_1::0::6::0_8']
    logger.info(f'Removed redundant qid for main engine turbocharger rotational speed. Shape: {initial_shape} -> {df.shape}')
    return df

def convert_xlsx_to_csv(input_path, output_path):
    # Read the Excel file from the specified sheet
    df = pd.read_excel(input_path, sheet_name=sheet_name)

    # Correct the unit for Vessel Propeller Shaft Revolutions
    df = correct_vessel_propeller_shaft_revolutions_unit(df)
    df = remove_redundant_turbocharger_qid(df)

    # Convert to CSV
    df.to_csv(output_path, index=False)

# Execute
convert_xlsx_to_csv(input_path, output_path)