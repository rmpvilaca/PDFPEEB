import polars as pl
import pandas as pd
import numpy as np
import pyarrow as pa

class Polars:
    def load(self, file_path):
        self.df = pl.read_parquet(file_path+".parquet")
        
    def filter(self):
        self.df = self.df[(self.df.tip_amount >= 1) & (self.df.tip_amount <= 5)]

    def mean(self):
        return self.df.passenger_count.mean()

    def sum(self):
        return self.df.fare_amount+self.df.extra+self.df.mta_tax+self.df.tip_amount+self.df.tolls_amount+self.df.improvement_surcharge
    
    def unique_rows(self):
        return self.df.VendorID.value_counts()

    def groupby(self):
        return self.df.groupby("passenger_count")["tip_amount"].mean

    def multiple_groupby(self):
        return self.df.groupby(["passenger_count", "payment_type"])["tip_amount"].mean

    def join(self):
        # Use Pandas to force UInt8. Polars doesn't support type change
        pd_payments =  pd.DataFrame({'payments': ['Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown', 'Voided trip'],'payment_type':[1,2,3,4,5,6]})  
        pd_payments['payment_type'] = pd_payments['payment_type'].astype('uint8')
        payments= pl.from_arrow(pa.Table.from_pandas(pd_payments))
        return self.df.join(payments, left_on='payment_type', right_on='payment_type')