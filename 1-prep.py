# This script prepares the data for geocoding.
# Depending on what the input CSV looks like, this script will need to be modified.

import pandas as pd

input_csv = 'MM_Sales.csv'
output_csv = 'MM_Sales_ready.csv'

df = pd.read_csv(input_csv)

# for any values in 'Zip' that are more than 5 digits, take only the first 5 digits
df['Zip'] = df['Zip'].astype(str).str[:5]

# Concatenate all relevant columns into a single 'full_address' column
df['full_address'] = df['Address'] + ' ' + df['City'] + ' ' + df['State'] + ' ' + df['Zip'].astype(str)

# create a url column for each row
space_filler = '%20'
df['url'] = ['https://www.google.com/maps/search/' + i for i in df['full_address'].str.replace(' ', space_filler)]

# create unique id for each row
df['id'] = df.index

# move this new 'id' column to the first position
df = df[['id'] + [col for col in df.columns if col != 'id']]

# Export to CSV
df.to_csv(output_csv, index=False)