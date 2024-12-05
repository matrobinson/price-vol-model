import pandas as pd
import numpy as np
import datetime as dt
from enum import Enum

# Logging imports for maintenance and management
import logging

class BasisOfPrep(Enum):
	''' 
	Acts as the basis of prep selector for price volume analysis
	'''
	MONTHLY = 1
	QUARTERLY = 2
	YEARLY = 3


def _load_data(file_path: str) -> pd.DataFrame:
	'''
	Loads data from CSV
	'''
	return pd.read_csv('data.csv')


def _load_dim_date():
	'''
	Loads the date dimension table
	'''
	data = pd.read_csv('dim_date.csv')
	data['date'] = pd.to_datetime(data['date'], format="%d/%m/%Y")
	return data


def _clean_data(data: pd.DataFrame, date_field_names: [str], input_date_format: str) -> pd.DataFrame:
	'''
	Works through the columns in the dataset formats date fields,
	removes zero values and other cleansing actions

	Args:
	- data: Pandas Dataframe
	- date_col_names: Array

	Returns:
	- Clean data
	'''
	
	# If date field names are passed in then convert to date_time
	if date_field_names != []:	
		for col in date_field_names:
			# Convert the column to datetime format
			data[col] = pd.to_datetime(data[col], format=input_date_format)
			print(f"Converted {col} to datetime. Dtype: {data[col].dtypes}")

	# Add Y, Q, M to make referencing prior periods easier
	data['year'] = data['date'].dt.year
	data['quarter'] = data['date'].dt.quarter
	data['month'] = data['date'].dt.month

	# Filter to the neceesary data so we are only carrying out ETL on the data we need
	# TODO - Extract year inputs into either UI or txt file for user to set
	start_year = 2018
	end_year = 2023
	yr_range_under_review = range(start_year, end_year + 1)

	data = data[np.isin(data['year'], yr_range_under_review)]

	# Cleanse data
	cleaned_data = _cleanse_data(data=data)

	return cleaned_data


def _cleanse_data(data: pd.DataFrame) -> pd.DataFrame:
	'''
	Drops any NA values and removes any zero value rows
	'''
	df = data.dropna()
	row_count_before = df.shape[0]
	df = df.loc[df['value']!=0]
	rows_dropped = row_count_before - df.shape[0]
	print("Rows dropped ", rows_dropped)
	return df



def _calculate_total_by_customer_product_by_year(data: pd.DataFrame) -> pd.DataFrame:
	'''
	Calculates total revenue and volume by customer and product by year, quarter or month
	'''
	group_by_cols = ['metric', 'customer', 'product', 'year']
	df = data.groupby(group_by_cols)['value'].sum().reset_index()
	return df


def _tag_customer_status(data: pd.DataFrame) -> pd.DatetimeIndex:
	'''
	Tags the customer level status for revenue. 
	If the customer has no revenue in the prior year and revenue 
	in the current year then we deem this customer a new customer. 
	The inverse (no revenue in the current year and revenue in the prior year) 
	would be a churned (lost) customer.
	'''
	
	# Using revenue only
	revenue_df = data[data['metric'].str.lower() =='revenue']

	# Sort values so they are ordered by customer and year
	revenue_df = revenue_df.sort_values(by=['customer', 'product', 'year']).reset_index(drop=True)
	
	# CUSTOMER LEVEL
	# Get the total revenue per customer per year
	cust_rev_by_yr = revenue_df.groupby(['customer', 'year'])['value'].sum().reset_index()
	cust_rev_by_yr = cust_rev_by_yr.sort_values(by=['customer', 'year']).reset_index(drop=True)
	
	# Compare the current and previous rows for the same customer (row-1 customer and row 0 customer)
	cust_rev_by_yr['prior_yr_value'] = cust_rev_by_yr['value'].shift(1).where(cust_rev_by_yr['customer'] == cust_rev_by_yr['customer'].shift(1), None)

	print(cust_rev_by_yr.head(10))

	# Tag customer and product status'
	cust_rev_by_yr = _apply_tagging_conditions(data=cust_rev_by_yr, output_status_field_name='cust_status_annual')

	# Merge 'cust_status_annual' back to the original data
	df = data.merge(cust_rev_by_yr[['customer', 'year', 'cust_status_annual']], on=['customer', 'year'], how='left')
	print(df.head)

	return df


def _apply_tagging_conditions(data: pd.DataFrame, output_status_field_name: str) -> pd.DataFrame:
	'''
	Applies tagging to data set based on current and previous row value
	'''
	# Apply vectorized conditions to determine customer status
	conditions = [
		(data['value'] > 0) & (data['prior_yr_value'].isna()),  # New customer (first year)
		(data['value'] == 0) & (data['prior_yr_value'] > 0),  # Lost customer
		(data['value'] > data['prior_yr_value']),  # Upsell
		(data['value'] < data['prior_yr_value']),  # Downsell
	]

	choices = ['New', 'Lost', 'Upsell', 'Downsell']

	# Create a field name with the status tagging
	data[output_status_field_name] = np.select(conditions, choices, default='Recurring')
	
	return data



def main():
	print("Starting pipeline")
	# Todo - add user input for column names that are date fields. 
	# As a streamlit app we can add check box's next to the column names to do this
	date_field_names = ['date']
	data = _load_data(file_path='data.csv')
	clean_data = _clean_data(
		data=data, 
		date_field_names=date_field_names,
		input_date_format="%d/%m/%Y"
	)

	clean_data = _calculate_total_by_customer_product_by_year(
		data=clean_data
	)

	clean_data = _tag_customer_status(data=clean_data)




if __name__ == '__main__':
	main()

