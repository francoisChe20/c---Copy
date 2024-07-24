#################################################################################################################
### 1°/ IMPORT LIBRAIRIES
#################################################################################################################

import re
import pandas as pd
import numpy as np
import requests
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from datetime import datetime, timedelta
import time
import os
import glob
import warnings

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

warnings.filterwarnings("ignore")

#################################################################################################################
### 2) SCRAP POSITIONING DATA:
###     - Traders in Financial Report
###     - The Disaggregated Report    
#################################################################################################################

def get_cot_data():

    # Get the Traders in Financial Futures Report
    df_tff = get_tff_data()
    time.sleep(2)
    df_tff = filter_contracts(df_tff)

    time.sleep(1)

    # Get the Disaggregated Report (for Commodities)
    # df_disaggregated = get_disaggregated_data()
    # time.sleep(2)
    # df_disaggreagted = filter_contracts(df_disaggregated)

    # return [df_tff, df_disaggregated]
    return df_tff


def filter_contracts(df):

    print('Keeping the most important futures contracts in progress ...')

    contracts_target = pd.read_excel('target_contracts.xlsx')
    
    contracts_to_keep = list(contracts_target['MARKET'])
    
    contracts_name_to_replace = {
        
        'UST 2Y NOTE':'2-YEAR NOTES',
        'UST 5Y NOTE':'5-YEAR NOTES',
        'UST 10Y NOTE':'10-YEAR NOTES',
        'E-MINI RUSSEL 1000 VALUE INDEX':'EMINI RUSSEL VALUE INDEX',
        'E-MINI S&amp;P CONSU STAPLES INDEX':'S&P CONSUMER STAPLES INDEX',
        'E-MINI S&amp;P ENERGY INDEX':'S&P ENERGY INDEX',
        'E-MINI S&amp;P FINANCIAL INDEX':'S&P FINANCIAL INDEX',
        'E-MINI S&amp;P HEALTH CARE INDEX':'S&P HEALTH CARE INDEX',
        'E-MINI S&amp;P UTILITIES INDEX': 'S&P UTILITIES INDEX',
        'E-MINI S&amp;P 400 STOCK INDEX': 'S&P 400 - MID CAP',
        'E-MINI S&amp;P ANNUAL DIVIDEND INDEX' : 'S&P 500 ANNUAL DIVIDEND INDEX',
        'MICRO E-MINI NASDAQ-100':'NASDAQ-100 (MNQ)',
        'NZ DOLLAR':'NEW ZEALAND DOLLAR',
        'S&P 500 Consolidated':'S&P 500 CONSOLIDATED',
        'SO AFRICAN RAND':'SOUTH AFRICAN RAND',
        'ULTRA UST 10Y':'ULTRA 10-YEAR BONDS',
        'DOW JONES U.S. REAL ESTATE IDX':'DOW JONES U.S. REAL ESTATE',
        'MICRO E-MINI S&P 500 INDEX':'MICRO S&P 500',
        'UST BOND':'30-YEAR BONDS',
        'DJIA x $5':'DOW JONES',
        'RUSSELL E-MINI':'RUSSELL 2000',
        'NASDAQ-100 Consolidated':'NASDAQ-100 Consolidated',
        'NASDAQ MINI':'NASDAQ-100 (NQ)',
        'E-MINI S&amp;P 500':'S&P 500 (ES)',
        'ETHER CASH SETTLED':'ETHEREUM',
        'MICRO ETHER':'MICRO ETHER',
        'ULTRA UST BOND':'ULTRA 30-YEAR BONDS',
        'CHEESE (CASH-SETTLED)': 'CHEESE (CASH)',
        'BUTTER (CASH SETTLED)' : 'BUTTER',
        'COFFEE C' : 'COFFEE',
        'COPPER- #1':'COPPER',
        'COTTON NO. 2': 'COTTON',
        'WTI-PHYSICAL': 'CRUDE OIL',
        'BRENT LAST DAY' : 'BRENT CRUDE OIL',
        'MILK, Class III': 'DC MILK, Class III',
        'COCOA':'COCOA',
        'NY HARBOR ULSD':'HEATING OIL',
        ' LUMBER': 'LUMBER',
        'NAT GAS NYME':'NATURAL GAS',
        'FRZN CONCENTRATED ORANGE JUICE':'ORANGE JUICE',
        'SOYBEANS':'SOYBEAN',
        'STEEL-HRC':'STEEL',
        'SUGAR NO. 11':'SUGAR',
        'WHEAT-SRW':'WHEAT',
        'VIX FUTURES':'VIX',
        'MSCI EAFE ':'MSCI EAFE'
    }
    
    # replace the good names for historic fils
    df['Contract'] = df['Contract'].replace(contracts_name_to_replace)

    # we keep only the contracts on which we are focusing
    df = df[df['Contract'].isin(contracts_to_keep)]

    contract_type = []
    contract_rank = []
    for i in df['Contract']:
        ct = str(contracts_target[contracts_target['MARKET']==i]['CATEGORY NAME'].values[0])
        rank = str(contracts_target[contracts_target['MARKET']==i]['CATEGORY RANK'].values[0])
        contract_type.append(ct)
        contract_rank.append(rank)

    df['Asset Class'] = contract_type
    df['Asset Class ID'] = contract_rank

    df = df.sort_values(by='Asset Class ID').reset_index(drop=True)

    missing_contracts = set(contracts_to_keep) - set(df['Contract'])

    return df


def get_tff_data():

    print('Chrome Launching ...')
    chrome = webdriver.Chrome()

    future_name = []
    contracts_id = []
    open_interest = []
    long_pos_dealer = []
    long_pos_asset_manager = []
    long_pos_leveraged_funds = []
    long_pos_other = []
    long_pos_nonreportables = []
    short_pos_dealer = []
    short_pos_asset_manager = []
    short_pos_leveraged_funds = []
    short_pos_other = []
    short_pos_nonreportables = []

    df_tff = pd.DataFrame({})

    chrome.get('https://www.cftc.gov/dea/futures/financial_lf.htm')
    time.sleep(1)
    response = chrome.page_source
    temp = response.split('<pre')[1]
    list = temp.split('Dealer')[1:]

    for i in list:

        sections = i.split('-----------------------------------------------------------------------------------------------------------------------------------------------------------\n')[1]
        future_name.append(sections.split(' -')[0])
        contracts_id.append(sections.split('CFTC Code #')[1].split()[0])
        open_interest.append(int(sections.split('Open Interest is')[1].split('\n')[0].replace(' ', '').replace(',','')))

        positions = sections.split('Positions')[1].split('\n')[1]
        split_values = positions.split()
        final = [value for value in split_values if value.strip() != '']
        
        # dealers
        total_long = int(final[0].replace(',',''))
        long_pos_dealer.append(total_long)
        total_short = int(final[1].replace(',',''))
        short_pos_dealer.append(total_short)

        # asset managers
        total_long = int(final[3].replace(',',''))
        long_pos_asset_manager.append(total_long)
        total_short = int(final[4].replace(',',''))
        short_pos_asset_manager.append (total_short)

        # leveraged funds
        total_long = int(final[6].replace(',',''))
        long_pos_leveraged_funds.append(total_long)
        total_short = int(final[7].replace(',',''))
        short_pos_leveraged_funds.append (total_short)

        # other reportables
        total_long = int(final[9].replace(',',''))
        long_pos_other.append(total_long)
        total_short = int(final[10].replace(',',''))
        short_pos_other.append (total_short)

        # nonreportables
        total_long = int(final[12].replace(',',''))
        long_pos_nonreportables.append(total_long)
        total_short = int(final[13].replace(',',''))
        short_pos_nonreportables.append (total_short)


    df_tff['Contract'] = future_name
    df_tff['ID'] = contracts_id
    df_tff['Total Open Interest'] = open_interest
    df_tff['Dealers - Long Pos'] = long_pos_dealer
    df_tff['Dealers - Short Pos'] = short_pos_dealer
    df_tff['Asset Managers - Long Pos'] = long_pos_asset_manager
    df_tff['Asset Managers - Short Pos'] = short_pos_asset_manager
    df_tff['Leveraged Funds - Long Pos'] = long_pos_leveraged_funds
    df_tff['Leveraged Funds - Short Pos'] = short_pos_leveraged_funds
    df_tff['Other Reportables - Long Pos'] = long_pos_other
    df_tff['Other Reportables - Short Pos'] = short_pos_other
    df_tff['NonReportables - Long Pos'] = long_pos_nonreportables
    df_tff['NonReportables - Short Pos'] = short_pos_nonreportables

    print('Data successfully extracted')
    chrome.quit()
    print('Chrome closed')

    return df_tff









    
