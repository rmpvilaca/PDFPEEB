import vaex


class Vaex:   
    def load(self, file_path):
        self.df = vaex.open(file_path+".parquet")

    def filter(self):
        self.df = self.df[self.df.tip_amount >= 1 and self.df.tip_amount <= 5]

    def mean(self):
        return self.df.passenger_count.mean()

    def unique_rows(self):
        return self.df.groupby(by="VendorID",agg={'count_VendorID': vaex.agg.count()})

    def groupby(self):
        return self.df.groupby(by="passenger_count",agg={'mean_tip_amount': vaex.agg.mean('tip_amount')})

    def multiple_groupby(self):
        return self.df.groupby(by=["passenger_count",],agg={'mean_tip_amount': vaex.agg.mean('tip_amount')})

    def join(self):
        payments =  vaex.from_arrays(payments=['Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown', 'Voided trip'],payment_type=[1,2,3,4,5,6])
        return self.df.join(payments, left_on='payment_type', right_on='payment_type')

    