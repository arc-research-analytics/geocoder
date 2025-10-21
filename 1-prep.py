# This script prepares the data for geocoding.
# Depending on what the input CSV looks like, this script will need to be modified.

# The input CSV should have the following columns:
# - Address
# - City
# - State
# - Zip
# If you are missing the City, State, or Zip, you can leave them blank, but you should still append at least the county name in a column called 'County'.
# Note that the creation of the 'full_address' column should include as many of these columns as possible, but you can just concatenate the Address and County columns if that's all you have.
# Thus, one possible value in 'full_address' would be '123 Main St Anytown CA 12345', while another possible value would be '123 Main St Fulton County GA'.

import pandas as pd

input_csv = 'test_data.csv'
output_csv = 'test_data_ready.csv'

df = pd.read_csv(input_csv)

# for any values in the zip code field that are more than 5 digits, take only the first 5 digits
df['zip'] = df['zip'].astype(str).str[:5]

# Concatenate all relevant columns into a single 'full_address' column
df['full_address'] = df['address'] + ' ' + df['city'] + ' ' + df['state'] + ' ' + df['zip'].astype(str)

# create a url column for each row
space_filler = '%20'
df['url'] = ['https://www.google.com/maps/search/' + i for i in df['full_address'].str.replace(' ', space_filler)]

# create unique id for each row
df['id'] = df.index

# move this new 'id' column to the first position
df = df[['id'] + [col for col in df.columns if col != 'id']]

# Export to CSV
df.to_csv(output_csv, index=False)