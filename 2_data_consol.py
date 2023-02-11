#######################################################################################################
# Author: Michel Szklo
# 
# This script consolidate imported data into a panel of municipality by year and generate new variables
# of interest.
#
# 
#######################################################################################################


# 0. Set-up
# =================================================================

# Libraries
# ----------------------
import os
import sys
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
import lxml
import re

# Main directories
# ----------------------

raw_folder = 'raw_data/'
clean_folder = 'clean_data/'


# 1. Creating a blank balanced panel of municipalities (#5570) by year (#20)
# =================================================================

# importing a list of all brazilian municipalities
# ---------------------------------------------------- 

file = raw_folder + 'municipios.csv'
mun_list = pd.read_csv(file,
                 sep = ',',
                 encoding = 'utf-8',
                 low_memory = False) 


# classifying municipalities by geographical region 
regions = mun_list
regions['id_estado'] = regions['id_estado'].astype(str)
regions.insert(0,'id_region',pd.to_numeric(regions.id_estado.str[0:1], errors = 'coerce', downcast = 'integer'))

regions.loc[regions['id_region']==1,'Region'] = 'North'
regions.loc[regions['id_region']==2,'Region'] = 'Northeast'
regions.loc[regions['id_region']==3,'Region'] = 'Southeast'
regions.loc[regions['id_region']==4,'Region'] = 'South'
regions.loc[regions['id_region']==5,'Region'] = 'Central-West'

regions = regions.filter(['id_munic_6','Region','municipio','estado']).rename(columns = {'id_munic_6' : 'mun_code'})



# balanced panel
# ---------------------------------------------------- 

# number of years
n_years = len(list(range(2000,2020))) 

# number of municipalities
n_mun = len(mun_list['id_munic_6'])

# list of all municipalities x number of years
mun_codes = mun_list['id_munic_6'].astype(int).tolist() * n_years
mun_codes.sort()

# list of all years x number of municipalities
years = list(range(2000,2020)) * n_mun


df = pd.DataFrame.from_dict({'mun_code': mun_codes, 'year': years})


df = df.merge(regions, on = 'mun_code', how = 'left')


# 2. Merging all clean data to the balanced panel
# =================================================================

# number of variables in the empty balanced panel
nvar0 = len(df.columns) 


# Mortality data
# ---------------------------------------------------- 

file = clean_folder + 'df_sim.csv'
df_sim = pd.read_csv(file)


df = df.merge(df_sim, on = ['mun_code','year'], how = 'left')

nvar1 = len(df.columns) # number of variables after merging

# NOTE: If a municipality does not have any record of death for a specific year,
# we won't find this municipality in the micro data for that year, but this does not
# classify as missing.
# For this reason, the 'nan' values observed after merging the mortality data
# to the balanced panel are supposed to be 0.

df.iloc[:,nvar0:nvar1] = df.iloc[:,nvar0:nvar1].replace(np.nan, 0) # replacing nan with 0


# Birth records
# ---------------------------------------------------- 

file = clean_folder + 'df_births.csv'
df_births = pd.read_csv(file)


df = df.merge(df_births, on = ['mun_code','year'], how = 'left')
nvar2 = len(df.columns) # number of variables after merging

# NOTE: Similarly. in the scraped website, if a municipality does not have any record of death for
# a specific year, the table generated won't show that municipality.
# For this reason, the 'nan' values observed after merging the mortality data
# to the balanced panel are supposed to be 0

df.iloc[:,nvar1:nvar2] = df.iloc[:,nvar1:nvar2].replace(np.nan, 0) # replacing nan with 0


# Spending data
# ---------------------------------------------------- 

file = clean_folder + 'df_spend.csv'
df_spend = pd.read_csv(file)

df = df.merge(df_spend, on = ['mun_code','year'], how = 'left')


# Population data
# ---------------------------------------------------- 

file = clean_folder + 'df_pop.csv'
df_pop = pd.read_csv(file)

df = df.merge(df_pop, on = ['mun_code','year'], how = 'left')


# Merging inflation data
# ---------------------------------------------------- 

file = clean_folder + 'df_ipca.csv'
print(file)
df_ipca = pd.read_csv(file)

df = df.merge(df_ipca, on = 'year', how = 'left')




# 3. Calculating Infant Mortaltity Rates
# =================================================================

vars = list(df.columns)
im_vars = [s for s in vars if "im" in s]

for v in im_vars:
    v_rate = v + '_rate'
    df[v_rate] = round(df[v] / df['births'] * 1000,2)
    df.loc[df['births']==0,v_rate] = 0


# 4. Deflating spending data (2019 R$)
# =================================================================
	
df['pc_spend'] = round(df['pc_spend'] / df['index'],2)


# 5. Exporting
# =================================================================

print(df.info())
print('\n')
print(df.head())

output_file = 'df_final.csv'
df.to_csv(output_file, index=False)




