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

def add_calculated_fouling_penalty_index_rows(df):
    # add rows for the calculated fouling penalty index (delta and cumulative)
    delta_fpi_qid = "5::0::0::0_0::0::0::0::0_0::0::0::0_1"
    fpi_qid = "5::0::0::0_0::0::0::0::0_0::0::0::0_2"

    if delta_fpi_qid in df['qid_mapping'].values or fpi_qid in df['qid_mapping'].values:
        logger.warning('Calculated fouling penalty index qids already exist in the dataframe. Skipping adding calculated fouling penalty index rows.')
        return df

    new_rows = [
        {
            'qid_mapping': delta_fpi_qid,
            'quantity_name': 'fouling_pressure_change',
            'source_name': 'calculated',
            'unit': 'fouling_penalty_index_units',
        },
        {
            'qid_mapping': fpi_qid,
            'quantity_name': 'cumulative_fouling_penalty_index',
            'source_name': 'calculated',
            'unit': 'fouling_penalty_index',
        }
    ]

    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    logger.info(f'Added calculated fouling penalty index rows to the dataframe. Shape is now: {df.shape}')
    return df

def convert_xlsx_to_csv(input_path, output_path):
    # Read the Excel file from the specified sheet
    df = pd.read_excel(input_path, sheet_name=sheet_name)

    # Correct the unit for Vessel Propeller Shaft Revolutions
    df = correct_vessel_propeller_shaft_revolutions_unit(df)
    df = remove_redundant_turbocharger_qid(df)

    # rows for the fouling proxy
    df = add_calculated_fouling_penalty_index_rows(df)

    # Convert to CSV
    df.to_csv(output_path, index=False)

# Execute
convert_xlsx_to_csv(input_path, output_path)