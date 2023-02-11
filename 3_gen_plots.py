#######################################################################################################
# Author: Michel Szklo
# 
# This script create plots to analyse Infant Mortality recent trends in Brazil
# 
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
import re

import plotly
import plotly.io as pio
import plotly.express as px
import plotly.offline as py
import plotly.graph_objects as go


# Raw data directory
# ----------------------

graphs_folder = 'graphs/'



# 1. Loading final Data Frane
# =================================================================

df = pd.read_csv('df_final.csv')


# 2. Time Series Plot: Brazil and Regions
# =================================================================

# collapsing IMR (weighted mean)
# -------------------------------------------------------

wm = lambda x: np.average(x, weights=df.loc[x.index, "births"])


# Brazil
df_br = df.groupby('year').agg(imr_total = ('im_rate',wm)).reset_index()
df_br.insert(1,'Region','Brazil')
df_br = df_br.rename(columns = {'imr_total':'Infant Mortality Rate'})


# By Region
df_region = df.groupby(['year','Region']).agg(imr_total = ('im_rate',wm)).reset_index()
df_region = df_region.rename(columns = {'imr_total':'Infant Mortality Rate'})


df_plot = pd.concat([df_br, df_region])

# Plot
# -------------------------------------------------------

fig = px.line(df_plot,
				x="year",
				y="Infant Mortality Rate",
				title='Recent Trends in Infant Mortality Rates',
				color = 'Region',
				line_dash_sequence = ['solid','dash'],
				hover_data=["Region", "Infant Mortality Rate"])

fig.update_xaxes(dtick=1)


# fig.update_traces(hovertemplate=None)
# fig.update_layout(hovermode="x")


file = graphs_folder + '1_trends.html'
fig.write_html(file)




# # 3. Time Series Plot: By main causes of death
# # =================================================================

# # collapsing IMR (weighted mean)
# # -------------------------------------------------------

# Main causes of death
vars = ['im_perinat', 'im_cong','im_illdef','im_infec','im_resp']

# df_causes = pd.melt(df, id_vars=['mun_code','year'], value_vars = vars, var_name='cause',value_name = "imr")


df_causes = df.groupby('year').agg(perinat = ('im_perinat_rate',wm),
								   cong = ('im_cong_rate',wm),
								   illdef = ('im_illdef_rate',wm),
								   infec = ('im_infec_rate',wm),
								   resp = ('im_resp_rate',wm)).reset_index()

vars = ['perinat', 'cong','illdef','infec','resp']
df_causes = pd.melt(df_causes, id_vars=['year'], value_vars = vars, var_name='cause',value_name = "imr")

df_causes.loc[df_causes['cause']=='perinat','cause'] = 'Perinatal'
df_causes.loc[df_causes['cause']=='cong','cause'] = 'Congenital'
df_causes.loc[df_causes['cause']=='illdef','cause'] = 'Ill-defined'
df_causes.loc[df_causes['cause']=='infec','cause'] = 'Infectious'
df_causes.loc[df_causes['cause']=='resp','cause'] = 'Respiratory'

df_causes = df_causes.rename(columns = {'cause': 'Cause of Death','imr':'Infant Mortality Rate'})


# Plot
# -------------------------------------------------------

fig = px.line(df_causes, x="year", y="Infant Mortality Rate", title='Recent Trends in Infant Mortality Rates by Cause of Death in Brazil', color = 'Cause of Death')
fig.update_xaxes(dtick=1)




file = graphs_folder + '2_trends_cause.html'
fig.write_html(file)




# # 4. Scatter plot: Shifts in IMR (2000-2019) vs IMR in 2000
# # =================================================================

# Brazil is a very unequal country, not only in terms of income,
# but also in terms of access and quality of public serices,
# such as public healthcare. In this context, evaluating
# convergence in infant mortality rates is very relevent.


# Municipality level
# -------------------------------------------------------


# creating the variable shitfs in IMR

df_plot = df
df_plot['Shifts in Infant Mortality Rate - 2000-2019'] =  df_plot['im_rate'].shift(-19) - df_plot['im_rate']

df_plot = df_plot[df_plot['year'] == 2000]

df_plot = df_plot.filter(['mun_code','im_rate','Shifts in Infant Mortality Rate - 2000-2019','municipio','Region', 'pop'])
df_plot = df_plot.rename(columns = {'im_rate': 'Infant Mortality Rate in 2000','municipio':'Municipality','pop':'Population'})

df_plot = df_plot.dropna(axis=0)

fig = px.scatter(df_plot, x="Infant Mortality Rate in 2000", y="Shifts in Infant Mortality Rate - 2000-2019",
	title = 'Shifts in Infant Mortality Rate 2000-2019 and Infant Mortality Rate in 2000',
	color="Region", size = 'Population',hover_name="Municipality",size_max=150, range_x=[0,200], range_y=[-200,200])


file = graphs_folder + '3_scatter_shifts.html'
fig.write_html(file)


# State level
# -------------------------------------------------------

# aggretating
df_plot = df.dropna(axis=0)
df_plot = df_plot.groupby(['estado','year','Region']).agg(imr = ('im_rate',wm),
													 pop = ('pop','sum')).reset_index()



df_plot['Shifts in Infant Mortality Rate - 2000-2019'] =  df_plot['imr'].shift(-19) - df_plot['imr']



df_plot = df_plot[df_plot['year'] == 2000]

df_plot = df_plot.filter(['estado','imr','Shifts in Infant Mortality Rate - 2000-2019','municipio','Region', 'pop'])
df_plot = df_plot.rename(columns = {'imr': 'Infant Mortality Rate in 2000','estado':'State','pop':'Population'})

df_plot = df_plot.dropna(axis=0)

fig = px.scatter(df_plot, x="Infant Mortality Rate in 2000", y="Shifts in Infant Mortality Rate - 2000-2019",
	color="Region", size = 'Population',hover_name="State",size_max=50, range_x=[10,35], range_y=[-20,5])

fig.update_layout(
    title=go.layout.Title(
        text="Shifts in Infant Mortality Rate 2000-2019 and Infant Mortality Rate in 2000 <br><sup>Data plotted at the State level</sup>",
        xref="paper",
        x=0
    ))

file = graphs_folder + '4_scatter_shifts_st.html'
fig.write_html(file)

# # # 5. Scatter plot: Infant Mortality Rates vs Health Spending through time
# # # =================================================================

# municipality level
# -------------------------------------------------------

df_plot = df.filter(['mun_code','year','im_rate','municipio','Region','pop','pc_spend'])
df_plot = df_plot.rename(columns = {'im_rate': 'Infant Mortality Rate','municipio':'Municipality','pop':'Population','pc_spend':'Public Health Spending per Capita (2019R$)'})

df_plot = df_plot.dropna(axis=0)

fig = px.scatter(df_plot, x="Public Health Spending per Capita (2019R$)", y="Infant Mortality Rate",
	animation_frame="year", animation_group="Municipality", color = 'Region',hover_name = 'Municipality',
	size = 'Population',size_max = 100,range_x=[0,3000])

fig.update_layout(
    title=go.layout.Title(
        text="The changing relationship between health spending and Infant Mortality. <br><sup>Data plotted at the Municipality level</sup>",
        xref="paper",
        x=0
    ))


file = graphs_folder + '5_scatter_spending.html'
fig.write_html(file)



# State level
# -------------------------------------------------------

# aggretating
df_plot = df.dropna(axis=0)
df_plot = df_plot.groupby(['estado','year','Region']).agg(imr = ('im_rate',wm),
													 pop = ('pop','sum'),
													 pc_spend = ('pc_spend',wm)).reset_index()




df_plot = df_plot.rename(columns = {'imr': 'Infant Mortality Rate','estado':'State','pop':'Population','pc_spend':'Public Health Spending per Capita (2019R$)'})


df_plot.to_csv('teste.csv')

df_plot = df_plot.dropna(axis=0)

fig = px.scatter(df_plot, x="Public Health Spending per Capita (2019R$)", y="Infant Mortality Rate",
	animation_frame="year", animation_group="State", color = 'Region',hover_name = 'State',
	size = 'Population',size_max = 50,range_x=[0,1500], range_y=[0,50])

fig.update_layout(
    title=go.layout.Title(
        text="The changing relationship between health spending and Infant Mortality. <br> <sup>Data plotted at the State level</sup>",
        xref="paper",
        x=0
    ))


file = graphs_folder + '6_scatter_spending_st.html'
fig.write_html(file)