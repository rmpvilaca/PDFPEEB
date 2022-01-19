from numba import njit
import pandas as pd
from pandas.io.api import read_csv

class Sdc:
    def load(self, file_path):
        self.df=sdc_load(file_path)

    def filter(self):
        return sdc_filter(self.df)
    
    def mean(self):
        return sdc_mean(self.df)
    
    def sum(self):
        return sdc_sum(self.df)
    
    def unique_rows(self):
        return sdc_unique_rows(self.df)

    def groupby(self):
        return sdc_groupby(self.df)

    def multiple_groupby(self):
        return sdc_multiple_groupby(self.df)

    def join(self):
        return sdc_join(self.df)

@njit 
def sdc_load(file_path):
	return pd.read_csv(file_path+".csv")

@njit 
def sdc_filter(df):
    return df[(df.tip_amount >= 1) & (df.tip_amount <= 5)]

@njit 
def  sdc_mean(df_sdc):
    return df_sdc.passenger_count.mean()

@njit 
def sdc_sum(df):
        return df.fare_amount+df.extra+df.mta_tax+df.tip_amount+df.tolls_amount+df.improvement_surcharge

@njit 
def sdc_unique_rows(df):
    return df.VendorID.value_counts()

@njit
def sdc_groupby(df_sdc):
    return df_sdc.groupby("passenger_count").tip_amount.mean()

@njit 
def sdc_multiple_groupby(df):
    return df.groupby(["passenger_count", "payment_type"]).tip_amount.mean()

@njit 
def sdc_join(df):
    payments =  pd.DataFrame({'payments': ['Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown', 'Voided trip'],'payment_type':[1,2,3,4,5,6]})
    return df.merge(payments, left_on='payment_type', right_on='payment_type')
    #return "teste"





    
