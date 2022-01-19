from numpy import right_shift
import dask.dataframe as dd
import pandas as pd
from dask.distributed import LocalCluster, Client

class Dask:
    def load(self, file_path,n_workers):
        cluster = LocalCluster(n_workers=n_workers)
        self.client= Client(cluster)
        self.df = dd.read_parquet(file_path+".parquet")
        self.df.persist()
    
    def filter(self):
        self.df = self.df[(self.df.tip_amount >= 1) & (self.df.tip_amount <= 5)]

    def mean(self):
        return self.df.passenger_count.mean().compute()

    def sum(self):
        return self.df.fare_amount+self.df.extra+self.df.mta_tax+self.df.tip_amount+self.df.tolls_amount+self.df.improvement_surcharge
    
    def unique_rows(self):
        return self.df.VendorID.value_counts().compute()
    
    def groupby(self):
        return self.df.groupby("passenger_count").tip_amount.mean().compute()

    def multiple_groupby(self):
        return self.df.groupby(["passenger_count", "payment_type"]).tip_amount.mean().compute()

    def join(self):
        payments =  dd.from_pandas(pd.DataFrame({'payment_name': ['Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown', 'Voided trip'],'payment_type':[1,2,3,4,5,6]}),npartitions=1)
        return self.df.merge(payments, left_on='payment_type',right_on='payment_type',right_index=True).compute()
        #return "teste"
    
    

    
