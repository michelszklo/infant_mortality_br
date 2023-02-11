#######################################################################################################
# Author: Michel Szklo
# 
# This script imports data related to Infant Mortality and Public Spending in Brazil from several sources
# for the period 2000-2019, and does simple data cleaning and organization.
#
# 
#######################################################################################################


# 0. Set-up
# =================================================================

# Libraries
# ----------------------
import os
import sys
from zipfile import ZipFile
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

# 1. Infant mortality micro data
# =================================================================

# I have access to Brazil's Ministry of Health Information System on Mortality (SIM)
# micro data in csv format. Each line represents an infant death and provides 
# several information about each death. Particularly, I'm interested in information 
# on the location (municipality), year, and cause of death.

# input
# -----------------------------------------------------------------

file = raw_folder + 'sim_microdata.zip'
with ZipFile(file, 'r') as zipObj:
   # Extract all the contents of zip file in different directory
   zipObj.extractall(raw_folder)


file = raw_folder + 'sim_microdata.csv'
df = pd.read_csv(file,
                 sep = ',',
                 encoding = 'latin1',
                 low_memory = False)

# selecting, renaming, and sorting
selected_variables = ['mun_res','ano','capcid_cau','cid_cau']
df = df.filter(selected_variables)
df = df.rename(columns={'mun_res':'mun_code',
                        'ano':'year',
                        'capcid_cau':'icd_chapter',
                        'cid_cau':'icd'})
df = df.sort_values(by = ['mun_code','year'])

print(df.head())

# Assessing the main causes of infant death in the baseline year of 2000
# -----------------------------------------------------------------
print(df["icd_chapter"][df["year"]==2000].value_counts())


# Aggregating data at the municipality-year level
# -----------------------------------------------------------------

# 1. Overall infant deaths

df['im'] = 1

# 2. Deaths by cause of death

# Perinatal
df['im_perinat'] = 0
df.loc[df['icd_chapter']=='Perinatal period conditions','im_perinat'] = 1

# Congenital
df['im_cong'] = 0
df.loc[df['icd_chapter']=='Congenital malformations','im_cong'] = 1

# Ill-defined
df['im_illdef'] = 0
df.loc[df['icd_chapter']=='Not well defined','im_illdef'] = 1

# Infectious
df['im_infec'] = 0
df.loc[df['icd_chapter']=='Infectious and parasitic diseases','im_infec'] = 1

# Respiratory
df['im_resp'] = 0
df.loc[df['icd_chapter']=='Respiratory system diseases','im_resp'] = 1

# Endocrine
df['im_endoc'] = 0
df.loc[df['icd_chapter']=='Endocrine/nutrit./metabolic diseases','im_endoc'] = 1

# External
df['im_exter'] = 0
df.loc[df['icd_chapter']=='External causes','im_exter'] = 1

# Nervous
df['im_nerv'] = 0
df.loc[df['icd_chapter']=='Nervous system diseases','im_nerv'] = 1

# Circulatory
df['im_circ'] = 0
df.loc[df['icd_chapter']=='Circulatory system diseases','im_circ'] = 1

# Blood
df['im_blood'] = 0
df.loc[df['icd_chapter']=='Blood diseases','im_blood'] = 1

# Digestive
df['im_digest'] = 0
df.loc[df['icd_chapter']=='Digestive system diseases','im_digest'] = 1

# Eye
df['im_eye'] = 0
df.loc[df['icd_chapter']=='Eye/ear/skin/musculosk./genitourinary system','im_eye'] = 1

# Neoplasms
df['im_neop'] = 0
df.loc[df['icd_chapter']=='Neoplasms','im_neop'] = 1



# Amenable to Primary Care (Alfradique, 2009) - https://www.scielo.br/j/csp/a/y5n975h7b3yW6ybnk6hJwft/abstract/?lang=pt

apc_icd = ["A361" ,"A362" ,"A369" ,"A370",
"A370" ,"A371" ,"A379" ,"A170" ,"B162",
"B169" ,"A371" ,"G000" ,"B268" ,"B269",
"A371" ,"B069" ,"B052" ,"B059" ,"A33" ,
"A35"  ,"A190" ,"A192" ,"A198" ,"A199",
"I00"  ,"I010" ,"I011" ,"I018" ,"I019",
"A513" ,"A519" ,"A522" ,"A527" ,"A530",
"A539" ,"A151" ,"A153" ,"A162" ,"A86" ,
"A000" ,"A001" ,"A009" ,"A010" ,"A011",
"A020" ,"A021" ,"A022" ,"A028" ,"A029",
"A030" ,"A039" ,"A040" ,"A041" ,"A042",
"A043" ,"A044" ,"A045" ,"A046" ,"A047",
"A048" ,"A049" ,"A050" ,"A052" ,"A059",
"A060" ,"A061" ,"A066" ,"A069" ,"A070",
"A072" ,"A073" ,"A078" ,"A079" ,"A080",
"A081" ,"A082" ,"A083" ,"A084" ,"A085",
"A09"  ,"D500" ,"D508" ,"D509" ,"E40" ,
"E41"  ,"E42"  ,"E43"  ,"E440" ,"E441",
"E45"  ,"E46"  ,"E512" ,"E519" ,"E52" ,
"E538" ,"E550" ,"E560" ,"E561" ,"E569",
"E616" ,"E619" ,"E630" ,"E631" ,"E638",
"E639" ,"E640" ,"E641" ,"E649" ,"J029",
"J039" ,"E638" ,"J060" ,"J068" ,"J069",
"J00"  ,"E638" ,"H660" ,"H662" ,"H663",
"H664" ,"H669" ,"J310" ,"J010" ,"J019",
"J158" ,"J159" ,"J069" ,"J181" ,"J14" ,
"J13"  ,"J153" ,"J154" ,"J450" ,"J451",
"J459" ,"J46"  ,"J200" ,"J201" ,"J203",
"J204" ,"J205" ,"J207" ,"J208" ,"J209",
"J210" ,"J218" ,"J219" ,"J40"  ,"J410",
"J411" ,"J42"  ,"J438" ,"J439" ,"J440",
"J441" ,"J448" ,"J449" ,"J47"  ,"I110",
"I119" ,"I10"  ,"I209" ,"I248" ,"I249",
"I500" ,"I501" ,"I509" ,"J81"  ,"I633",
"I638" ,"I639" ,"I64"  ,"I670" ,"I671",
"I672" ,"I674" ,"I677" ,"I678" ,"I679",
"I690" ,"I691" ,"I692" ,"I694" ,"I698",
"G459" ,"E100" ,"E101" ,"E550" ,"E140",
"E141" ,"E109" ,"E550" ,"E119" ,"E149",
"E142" ,"E145" ,"E156" ,"E147" ,"E148",
"G400" ,"G401" ,"G402" ,"G403" ,"G404",
"G405" ,"G406" ,"G407" ,"G408" ,"G409",
"G410" ,"G411" ,"G418" ,"G419" ,"N10" ,
"N111" ,"N118" ,"N119" ,"N12"  ,"N390",
"N394" ,"N398" ,"N399" ,"L010" ,"L011",
"L020" ,"L021" ,"L022" ,"L023" ,"L024",
"L028" ,"L029" ,"L030" ,"L031" ,"L032",
"L033" ,"L038" ,"L039" ,"L040" ,"L048",
"L049" ,"N390" ,"N394" ,"N398" ,"N399",
"N709" ,"N735" ,"N738" ,"N739" ,"N760",
"N764" ,"K251" ,"K254" ,"K255" ,"K256",
"K259" ,"K261" ,"K265" ,"K269" ,"K274",
"K921" ,"K922" ,"A500" ,"A501" ,"A502",
"A504" ,"A505" ,"A509" ,"A350"]

df['im_apc'] = 0
df.loc[df['icd'].isin(apc_icd),'im_apc'] = 1

# collapsing

cols = list(df.columns)
imvars = [s for s in cols if "im" in s]
dict = dict.fromkeys(imvars,'sum')

df_sim = df.groupby(['mun_code','year']).agg(dict).reset_index()
df_sim['im_napc']  = df_sim['im'] - df_sim['im_apc']

print(df_sim.head())


# Exporting
# -----------------------------------------------------------------

output_file = clean_folder + 'df_sim.csv'
df_sim.to_csv(output_file, index=False)



# Deleting unzipped file
# -----------------------------------------------------------------
file_delete = raw_folder + 'sim_microdata.csv'
os.remove(file_delete)


# 2. Birth data
# =================================================================

# To calculate infant mortaly rates I need to collect data on the number of
# births by municipality-year.

# For that, I scrape Brazil's DATASUS Information System on Birth Records (SINASC)
# website, where is possible to access data at the municipality level.

# http://tabnet.datasus.gov.br/cgi/tabcgi.exe?sinasc/cnv/nvbr.def


# note: the website might not be accessible outside of Brazil



# Scraping function 
# -----------------------------------------------------------------


def sinasc_scrape(year):
    text = '_____ scraping birth records by municipality for the year of ' + str(year) + ' _____ '
    print(text)
    
    
    # POST request
    # ---------------------------
    
    #  extracting year format for POST request data
    year_html = str(year)[2:4]
    
    # post request parameters
    data = 'Linha=Munic%EDpio&Coluna=--N%E3o-Ativa--&Incremento=Nascim_p%2Fresid.m%E3e&Arquivos=nvbr' + year_html + '.dbf&pesqmes1=Digite+o+texto+e+ache+f%E1cil&SMunic%EDpio=TODAS_AS_CATEGORIAS__&pesqmes2=Digite+o+texto+e+ache+f%E1cil&SCapital=TODAS_AS_CATEGORIAS__&pesqmes3=Digite+o+texto+e+ache+f%E1cil&SRegi%E3o_de_Sa%FAde_%28CIR%29=TODAS_AS_CATEGORIAS__&pesqmes4=Digite+o+texto+e+ache+f%E1cil&SMacrorregi%E3o_de_Sa%FAde=TODAS_AS_CATEGORIAS__&pesqmes5=Digite+o+texto+e+ache+f%E1cil&SMicrorregi%E3o_IBGE=TODAS_AS_CATEGORIAS__&pesqmes6=Digite+o+texto+e+ache+f%E1cil&SRegi%E3o_Metropolitana_-_RIDE=TODAS_AS_CATEGORIAS__&pesqmes7=Digite+o+texto+e+ache+f%E1cil&STerrit%F3rio_da_Cidadania=TODAS_AS_CATEGORIAS__&pesqmes8=Digite+o+texto+e+ache+f%E1cil&SMesorregi%E3o_PNDR=TODAS_AS_CATEGORIAS__&SAmaz%F4nia_Legal=TODAS_AS_CATEGORIAS__&SSemi%E1rido=TODAS_AS_CATEGORIAS__&SFaixa_de_Fronteira=TODAS_AS_CATEGORIAS__&SZona_de_Fronteira=TODAS_AS_CATEGORIAS__&SMunic%EDpio_de_extrema_pobreza=TODAS_AS_CATEGORIAS__&SLocal_ocorr%EAncia=TODAS_AS_CATEGORIAS__&pesqmes15=Digite+o+texto+e+ache+f%E1cil&SIdade_da_m%E3e=TODAS_AS_CATEGORIAS__&pesqmes16=Digite+o+texto+e+ache+f%E1cil&SInstru%E7%E3o_da_m%E3e=TODAS_AS_CATEGORIAS__&SEstado_civil_m%E3e=TODAS_AS_CATEGORIAS__&SDura%E7%E3o_gesta%E7%E3o=TODAS_AS_CATEGORIAS__&STipo_de_gravidez=TODAS_AS_CATEGORIAS__&pesqmes20=Digite+o+texto+e+ache+f%E1cil&SGrupos_de_Robson=TODAS_AS_CATEGORIAS__&SAdeq_quant_pr%E9-natal*=TODAS_AS_CATEGORIAS__&STipo_de_parto=TODAS_AS_CATEGORIAS__&SConsult_pr%E9-natal=TODAS_AS_CATEGORIAS__&SSexo=TODAS_AS_CATEGORIAS__&SCor%2Fra%E7a=TODAS_AS_CATEGORIAS__&SApgar_1%BA_minuto=TODAS_AS_CATEGORIAS__&SApgar_5%BA_minuto=TODAS_AS_CATEGORIAS__&SPeso_ao_nascer=TODAS_AS_CATEGORIAS__&SAnomalia_cong%EAnita=TODAS_AS_CATEGORIAS__&pesqmes30=Digite+o+texto+e+ache+f%E1cil&STipo_anomal_cong%EAn=TODAS_AS_CATEGORIAS__&formato=table&mostre=Mostra'
    
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        # 'Cookie': 'TS014879da=01e046ca4ce5b1742693db265b21785790ff5c91fbeb948aa2c11828a2b6e83c20d9e5115c4e060a7b65a672838b5544318100ae37',
        'Origin': 'http://tabnet.datasus.gov.br',
        'Pragma': 'no-cache',
        'Referer': 'http://tabnet.datasus.gov.br/cgi/tabcgi.exe?sinasc/cnv/nvbr.def',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        }
    
    # request
    response = requests.post(
        'http://tabnet.datasus.gov.br/cgi/tabcgi.exe?sinasc/cnv/nvbr.def',
        headers=headers,
        data=data,
        verify=False,
        timeout = 20
    )
    
    
    # Extracting HTML table elements
    # ---------------------------
    
    soup = BeautifulSoup(response.text, 'lxml')

    # table data
    tabdados = soup.select(".tabdados tbody tr td")
    tabdados = list(map(lambda node: node.get_text().strip(), tabdados))

    # table columns names
    col_tabdados = soup.select(".tabdados tr th")
    col_tabdados = list(map(lambda node: node.get_text().strip(), col_tabdados))


    # defining the number of rows and columns
    len_tabdados = len(tabdados)
    len_col_tabdados = len(col_tabdados)

    nrow = int(len_tabdados/len_col_tabdados)
    ncol = int(len_col_tabdados)

    # transforming into arrays and reshaping
    df = np.array(tabdados).reshape(nrow,ncol)

    # converting to pandas df
    df = pd.DataFrame(df,
                      columns = ['municipality','births'])

    
    # Data cleaning
    # ---------------------------
    
    # removes firts row containing the sum of all rows
    df = df.drop(index=0)

    # spliting municipalities' codes (our database id) from names 
    df.insert(0,'mun_code',pd.to_numeric(df.municipality.str[0:6], errors = 'coerce', downcast = 'integer'))
    df.drop(columns = 'municipality', axis = 1, inplace = True)

    # droping non numeric codes
    df = df.dropna()

    # converting births to numeric
    df['births'] = df['births'].str.replace('.','', regex = True)
    df['births'] = pd.to_numeric(df.births, errors = 'coerce')
    df["births"] = df["births"].replace(np.nan, 0)


    # adding year
    df.insert(0,'year', year)
    
    
    return df


# Loop to run function for 2000-2019 and append all
# -----------------------------------------------------------------
for year in range(2000,2020,1):
    
    df = sinasc_scrape(year=year)
    

    # Appending
    
    if year == 2000:
        df_births = df
    else:
        df_births = pd.concat([df_births, df])

df_births = df_births.astype(int)


# Exporting
# -----------------------------------------------------------------

output_file = clean_folder + 'df_births.csv'
df_births.to_csv(output_file, index=False)




# 3. Public Health Spending Data
# =================================================================

# I also collect data on public health spending per capita with the idea of assessing
# the relationship between spending and infant mortality in Brazil.

# For that, I scrape Brazil's DATASUS Information System on Public Health Budgets (SIOPS)
# website, where is possible to access data at the municipality level.

# http://siops-asp.datasus.gov.br/CGI/tabcgi.exe?SIOPS/serhist/municipio/mIndicadores.def


# note: the website might not be accessible outside of Brazil



# Scraping function for spending
# -----------------------------------------------------------------

def siops_scrape_spend(year):
    text = '_____ scraping public health spending per capita by municipality for the year of ' + str(year) + '_____ '
    print(text)


    # POST request
    # ---------------------------

    #  extracting year format for POST request data
    year_html = str(year)[2:4]

    # post request parameters
    data = 'Linha=Munic-BR&Coluna=Ano&Incremento=2.1_D.Total_Sa%FAde%2FHab&Arquivos=indmun'+ year_html + '.dbf&SUF=TODAS_AS_CATEGORIAS__&SCapitais=TODAS_AS_CATEGORIAS__&SMunic-BR=TODAS_AS_CATEGORIAS__&SRegi%E3o=TODAS_AS_CATEGORIAS__&SSele%E7%E3o_Capitais=TODAS_AS_CATEGORIAS__&SFaixa_Pop=TODAS_AS_CATEGORIAS__&formato=table&mostre=Mostra'

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'http://siops-asp.datasus.gov.br',
        'Pragma': 'no-cache',
        'Referer': 'http://siops-asp.datasus.gov.br/CGI/deftohtm.exe?SIOPS/serhist/municipio/mIndicadores.def',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    }
    # request
    response = requests.post(
        'http://siops-asp.datasus.gov.br/CGI/tabcgi.exe?SIOPS/serhist/municipio/mIndicadores.def',
        headers=headers,
        data=data,
        verify=False,
        timeout = 20
    )


    # Extracting HTML table elements
    # ---------------------------

    soup = BeautifulSoup(response.text, 'lxml')

    # table data
    tabdados = soup.select("body tr td")
    tabdados = list(map(lambda node: node.get_text().strip(), tabdados))
    del tabdados [0:4]
    del tabdados [-5:]


    # table columns names
    id_tabdados = soup.select("body tr th")
    id_tabdados = list(map(lambda node: node.get_text().strip(), id_tabdados))

    # defining the number of rows and columns
    id_cols = id_tabdados[0:3]
    id_rows = id_tabdados[3:]

    nrow = int(len(id_rows))
    ncol = int(len(id_cols))


    # # transforming into arrays and reshaping
    df = np.array(tabdados).reshape(nrow,ncol-1)


    # converting to pandas df
    df = pd.DataFrame(df,
                      columns = ['pc_spend','total'])

    # adding data id rows
    df.insert(0,'municipality',id_rows)


     # Data cleaning
    # ---------------------------

    # removes firts row containing the sum of all rows
    df.drop(index=0, inplace = True)

    # removes last column containing the sum of all cols
    df.drop(columns = 'total', axis = 1, inplace = True)

    # spliting municipalities' codes (our database id) from names 
    df.insert(0,'mun_code',pd.to_numeric(df.municipality.str[0:6], errors = 'coerce', downcast = 'integer'))
    df.drop(columns = 'municipality', axis = 1, inplace = True)

    # droping non numeric codes
    df = df.dropna()

    # converting spending to numeric

    df['pc_spend'] = df['pc_spend'].str.replace('.','', regex = True) # converting numbers from portuguese to english
    df['pc_spend'] = df['pc_spend'].str.replace(',','.', regex = True) # converting numbers from portuguese to english
    df['pc_spend'] = pd.to_numeric(df.pc_spend, errors = 'coerce')
    df["pc_spend"] = df["pc_spend"].replace(np.nan, 0)


    # adding year
    df.insert(0,'year', year)
    
    return df





# Scraping function for population
# -----------------------------------------------------------------

def siops_scrape_pop(year):
    text = '_____ scraping population by municipality for the year of ' + str(year) + '_____ '
    print(text)


    # POST request
    # ---------------------------

    #  extracting year format for POST request data
    year_html = str(year)[2:4]

    # post request parameters
    data = 'Linha=Munic-BR&Coluna=Ano&Incremento=Popula%E7%E3o&Arquivos=indmun'+ year_html + '.dbf&SUF=TODAS_AS_CATEGORIAS__&SCapitais=TODAS_AS_CATEGORIAS__&SMunic-BR=TODAS_AS_CATEGORIAS__&SRegi%E3o=TODAS_AS_CATEGORIAS__&SSele%E7%E3o_Capitais=TODAS_AS_CATEGORIAS__&SFaixa_Pop=TODAS_AS_CATEGORIAS__&formato=table&mostre=Mostra'

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
	    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
	    'Cache-Control': 'no-cache',
	    'Connection': 'keep-alive',
	    'Content-Type': 'application/x-www-form-urlencoded',
	    'Origin': 'http://siops-asp.datasus.gov.br',
	    'Pragma': 'no-cache',
	    'Referer': 'http://siops-asp.datasus.gov.br/CGI/tabcgi.exe?SIOPS/serhist/municipio/mIndicadores.def',
	    'Upgrade-Insecure-Requests': '1',
	    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    }
    # request
    response = requests.post(
        'http://siops-asp.datasus.gov.br/CGI/tabcgi.exe?SIOPS/serhist/municipio/mIndicadores.def',
        headers=headers,
        data=data,
        verify=False,
        timeout = 20
    )


    # Extracting HTML table elements
    # ---------------------------

    soup = BeautifulSoup(response.text, 'lxml')

    # table data
    tabdados = soup.select("body tr td")
    tabdados = list(map(lambda node: node.get_text().strip(), tabdados))
    del tabdados [0:4]
    del tabdados [-3:]


    # table columns names
    id_tabdados = soup.select("body tr th")
    id_tabdados = list(map(lambda node: node.get_text().strip(), id_tabdados))

    # defining the number of rows and columns
    id_cols = id_tabdados[0:3]
    id_rows = id_tabdados[3:]

    nrow = int(len(id_rows))
    ncol = int(len(id_cols))


    # transforming into arrays and reshaping
    df = np.array(tabdados).reshape(nrow,ncol-1)


    # converting to pandas df
    df = pd.DataFrame(df,
                      columns = ['pop','total'])

    # adding data id rows
    df.insert(0,'municipality',id_rows)


     # Data cleaning
    # ---------------------------

    # removes firts row containing the sum of all rows
    df.drop(index=0, inplace = True)

    # removes last column containing the sum of all cols
    df.drop(columns = 'total', axis = 1, inplace = True)

    # spliting municipalities' codes (our database id) from names 
    df.insert(0,'mun_code',pd.to_numeric(df.municipality.str[0:6], errors = 'coerce', downcast = 'integer'))
    df.drop(columns = 'municipality', axis = 1, inplace = True)

    # droping non numeric codes
    df = df.dropna()

    # converting spending to numeric

    df['pop'] = df['pop'].str.replace('.','', regex = True) # converting numbers from portuguese to english
    df['pop'] = df['pop'].str.replace(',','.', regex = True) # converting numbers from portuguese to english
    df['pop'] = df['pop'].astype(str).astype(int)
    df["pop"] = df["pop"].replace(np.nan, 0)


    # adding year
    df.insert(0,'year', year)
    
    return df



# Loop to run function for 2000-2019 and append all (Spending)
# -----------------------------------------------------------------
for year in range(2000,2020,1):
    
    df = siops_scrape_spend(year=year)
    
    # Appending
    # ---------------------------
    
    if year == 2000:
        df_spend = df
    else:
        df_spend = pd.concat([df_spend, df])
        
df_spend = df_spend.astype(int)

print(df_spend.head())        


# Loop to run function for 2000-2019 and append all (Population)
# -----------------------------------------------------------------
for year in range(2000,2020,1):
    
    df = siops_scrape_pop(year=year)
    
    # Appending
    # ---------------------------
    
    if year == 2000:
        df_pop = df
    else:
        df_pop = pd.concat([df_pop, df])
        
df_pop = df_pop.astype(int)   


# Exporting 
# -----------------------------------------------------------------

output_file = clean_folder + 'df_spend.csv'
df_spend.to_csv(output_file, index=False)


output_file = clean_folder + 'df_pop.csv'
df_pop.to_csv(output_file, index=False)




# 4. Inflation data
# =================================================================

# When working with spending data it's important to adjust nominal
# values to real values. I use IBGE's IPCA (Consumer Price Index) 
# monthly variation to build an index.

# http://www.ipeadata.gov.br/Default.aspx (go to )

file = raw_folder + 'ipca.csv'

df_ipca = pd.read_csv(file,
                    sep = ';')

# creating inflation index (2019) = 1 from monthly variation

df_ipca['index'] = 1
df_ipca['var_lead'] = df_ipca['IPCA'].shift(-1)/100 + 1

rows = len(df_ipca) - 2
loc_index = df_ipca.columns.get_loc("index")
loc_var_lead = df_ipca.columns.get_loc("var_lead")

for i in range(rows,-1,-1):
    df_ipca.iloc[i,loc_index] = df_ipca.iloc[i+1,loc_index] / df_ipca.iloc[i,loc_var_lead]

df_ipca.drop(columns = ['IPCA','var_lead'], axis = 1, inplace = True)



# Exporting

output_file = clean_folder + 'df_ipca.csv'
df_ipca.to_csv(output_file, index=False)


