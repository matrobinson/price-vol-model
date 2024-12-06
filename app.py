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



# ------------------------------------
# Data Loading
# ------------------------------------

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




# ------------------------------------
# ETL - Extract Transform Load
# ------------------------------------

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

	# Filter for specific range of data in period under review
	data = data[np.isin(data['year'], yr_range_under_review)]

	return data



# ------------------------------------
# Calculations
# ------------------------------------


def _create_arr_data(data: pd.DataFrame) -> pd.DataFrame:
	'''
	Creates an annual recurring revenue amount based off of the reported monthly recurring revenue 
	'''

	# Get only the revenue values
	recurring_rev = data[data['metric'] == 'Revenue']

	# Filter for only recurring revenue
	recurring_rev = recurring_rev[recurring_rev['revenue_type'].str.lower() == 'recurring']
	
	# Calculate the ARR from the MRR
	recurring_rev['value'] = recurring_rev['value'] * 12 # Months in year
	recurring_rev['metric'] = 'ARR' # Change metric to annual recurring revenue

	# Add the the ARR data to existing data
	data = data._append(recurring_rev)
	return data



def _calculate_total_by_customer_product_by_year(data: pd.DataFrame) -> pd.DataFrame:
	'''
	Calculates total revenue and volume by customer and product by year, quarter or month
	'''
	group_by_cols = ['metric', 'customer', 'product', 'year']
	df = data.groupby(group_by_cols)['value'].sum().reset_index()
	return df




def _apply_tagging_conditions(data: pd.DataFrame, output_status_field_name: str) -> pd.DataFrame:
	'''
	Applies tagging to data set based on current and previous row value
	'''
	
	conditions = []
	choices = []

	# If product level data structure then apply a credit note condition first too
	if 'product' in data.columns:
		conditions.append(data['product'].str.lower() == 'credit notes')
		choices.append('Credit Notes')

	# Apply vectorized conditions to determine customer status
	conditions.extend([
		((data['value'] >= 0) & (data['prior_yr_value'] >= 0) & (data['value'] == data['prior_yr_value'])),  # Recurring
		(data['value'] > data['prior_yr_value']),  # Upsell
		(data['value'] < data['prior_yr_value']),  # Downsell
		(data['value'] == 0) & (data['prior_yr_value'] > 0),  # Lost
		(data['value'] > 0) & (data['prior_yr_value'].isna()),  # New 
		(data['value'] < 0) | (data['prior_yr_value'] < 0), # Credit Notes
		
	])

	choices.extend(['Recurring','Upsell', 'Downsell', 'Lost', 'New', "Credit Notes"])

	# Create a field name with the status tagging
	data[output_status_field_name] = np.select(conditions, choices, default='N/A')
	
	return data


def _tag_final_cust_prod_status(data: pd.DataFrame) -> pd.DataFrame:
	'''
	Takes the customer status and the customer product status and 
	creates a final status for each record in the data
	'''

	cust_status_field_name = 'cust_status_annual'
	cust_prod_status_field_name = 'custprod_status_annual_temp'

	conditions = [
		(data[cust_prod_status_field_name].str.lower() == 'credit notes'), # Credit Notes

		(data[cust_status_field_name].str.lower() == 'new'), # new
		(data[cust_status_field_name].str.lower() == 'reactivated'), # reactivated
		(data[cust_status_field_name].str.lower() == 'deactivated'), # deactivated
		(data[cust_status_field_name].str.lower() == 'lost'), # lost

		# Cross Sell
		((data[cust_status_field_name].str.lower().isin(['recurring', 'upsell', 'downsell'])) &
         (data[cust_prod_status_field_name].str.lower() == 'new')),

		# Cross loss - existing product lost for existing customer
        ((data[cust_status_field_name].str.lower().isin(['recurring', 'upsell', 'downsell'])) &
         (data[cust_prod_status_field_name].str.lower() == 'lost')),

		# Customer prod level upsell, downsell or revenue is the same 'recurring'
		(data[cust_prod_status_field_name].str.lower() == 'upsell'),
		(data[cust_prod_status_field_name].str.lower() == 'downsell'),
		(data[cust_prod_status_field_name].str.lower() == 'recurring'),

		# Where customer status is tagged as a credit note but the product is new
		((data[cust_status_field_name].str.lower() == 'credit notes') & data[cust_prod_status_field_name].str.lower() == 'new'),
		((data[cust_status_field_name].str.lower() == 'credit notes') & data[cust_prod_status_field_name].str.lower() == 'lost'),
	]

	# Match the choices with the conditions
	choices = ['Credit Notes', 'New', 'Reactivated', 'Deactivated', 'Lost', 'xSell', 'xLoss', 'Upsell', 'Downsell', 'Recurring', 'xSell', 'xLoss']

	# Apply the choices in a vectorized operation
	data['custprod_status_annual'] = np.select(conditions, choices, default='N/A')

	return data


def _tag_customer_status(data: pd.DataFrame) -> pd.DatetimeIndex:
	'''
	Tags the customer level status for revenue. 
	If the customer has no revenue in the prior year and revenue 
	in the current year then we deem this customer a new customer. 
	The inverse (no revenue in the current year and revenue in the prior year) 
	would be a churned (lost) customer.
	'''
	
	# Using revenue only
	revenue_df = data[data['metric'].str.lower() == 'arr']

	# CUSTOMER LEVEL
	cust_rev_df = revenue_df
	
	# Sort values so they are ordered by customer and year
	cust_rev_df = cust_rev_df.sort_values(by=['customer', 'product', 'year']).reset_index(drop=True)

	# Get the total revenue per customer per year
	cust_rev_by_yr = cust_rev_df.groupby(['customer', 'year'])['value'].sum().reset_index()
	cust_rev_by_yr = cust_rev_by_yr.sort_values(by=['customer', 'year']).reset_index(drop=True)
	
	# Compare the current and previous rows for the same customer (row-1 customer and row 0 customer)
	cust_rev_by_yr['prior_yr_value'] = cust_rev_by_yr['value'].shift(1).where(cust_rev_by_yr['customer'] == cust_rev_by_yr['customer'].shift(1), None)

	# Tag customer and product status'
	cust_rev_by_yr = _apply_tagging_conditions(data=cust_rev_by_yr, output_status_field_name='cust_status_annual')

	# Merge 'cust_status_annual' back to the original data
	data = data.merge(cust_rev_by_yr[['customer', 'year', 'cust_status_annual']], on=['customer', 'year'], how='left')

	# PRODUCT LEVEL
	custprod_rev_df = revenue_df
	
	# Get the total revenue per customer per year
	custprod_rev_by_yr = custprod_rev_df.groupby(['customer', 'product', 'year'])['value'].sum().reset_index()
	custprod_rev_by_yr = custprod_rev_by_yr.sort_values(by=['customer', 'product', 'year']).reset_index(drop=True)
	
	# Compare the current and previous rows for the same customer (row-1 customer prod and row 0 customer prod)
	custprod_rev_by_yr['prior_yr_value'] = custprod_rev_by_yr['value'].shift(1).where(
		(custprod_rev_by_yr['product'] == custprod_rev_by_yr['product'].shift(1)) &
		(custprod_rev_by_yr['customer'] == custprod_rev_by_yr['customer'].shift(1)),
		None
	)

	custprod_rev_by_yr = _apply_tagging_conditions(data=custprod_rev_by_yr, output_status_field_name='custprod_status_annual_temp')

	# Merge 'cust_status_annual' back to the original data
	data = data.merge(custprod_rev_by_yr[['customer', 'product', 'year', 'custprod_status_annual_temp']], on=['customer', 'product', 'year'], how='left')

	# Final combined customer and product status
	data = _tag_final_cust_prod_status(data=data)
	
	data = data.drop(columns=['custprod_status_annual_temp'])

	print(data)

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

	clean_data = _create_arr_data(
		data=clean_data
	)

	clean_data = _calculate_total_by_customer_product_by_year(
		data=clean_data
	)

	clean_data = _tag_customer_status(data=clean_data)




if __name__ == '__main__':
	main()

