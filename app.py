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

	# Cleanse data
	return _cleanse_data(data=data)


def _cleanse_data(data: pd.DataFrame) -> pd.DataFrame:
	'''
	Drops any NA values and removes any zero value rows
	'''
	df = data.dropna()
	print("Removing zeros")
	row_count_before = df.shape[0]
	df = df.loc[df['value']!=0]
	rows_dropped = row_count_before - df.shape[0]
	print("Rows dropped ", rows_dropped)
	return df




def _join_dates_from_period_under_review_to_data(data: pd.DataFrame):
	dates = _load_dim_date()
	dfe = pd.merge(data, dates, how='left', on=['date'], indicator=True)
	return dfe



def calculate_total_by_customer_product_by_year(data: pd.DataFrame) -> pd.DataFrame:
	'''
	Calculates total revenue and volume by customer and product by year, quarter or month
	'''
	print(data.columns)
	group_by_cols = ['customer', 'product', 'year']
	df = data.groupby(group_by_cols)['value'].sum().reset_index()
	print(df.head)
	return 


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

	clean_data = _join_dates_from_period_under_review_to_data(
		data=clean_data
	)

	clean_data = calculate_total_by_customer_product_by_year(
		data=clean_data
	)


if __name__ == '__main__':
	main()

