import pandas as pd
import glob

# Function to merge CSV files

def merge_csv_files(output_file):
    # Use glob to get all the csv files in the folder
    csv_files = glob.glob('*.csv')

    # Create an empty list to hold the dataframes
    df_list = []

    # Loop through the csv files and append them to the list
    for file in csv_files:
        df = pd.read_csv(file)
        df_list.append(df)

    # Concatenate all dataframes in the list
    merged_df = pd.concat(df_list, ignore_index=True)

    # Write the merged dataframe to a new csv file
    merged_df.to_csv(output_file, index=False)

# Merge all CSV files in the current directory into 'merged_output.csv'
merge_csv_files('merged_output.csv')