from datatable import dt, f, by, join, rowsum


class Datatable:
    def load(self, file_path):
        self.df = dt.fread(file_path+".csv")
    
    def filter(self):
        self.df=self.df[(f.tip_amount>=1) & (f.tip_amount<=5), :]
        
    def mean(self):
        return self.df[:, dt.mean(dt.f.passenger_count)]
    
    def sum(self):
        return self.df[:, rowsum(f.fare_amount,f.extra,f.mta_tax,f.tip_amount,f.tolls_amount,f.improvement_surcharge)]
        
    def unique_rows(self):
        return self.df[:, dt.count(), by("VendorID")]

    def groupby(self):
        return self.df[:, dt.mean(f.tip_amount), by("passenger_count")]    

    def multiple_groupby(self):
        return self.df[:, dt.mean(f.tip_amount), by("passenger_count","payment_type")]    

    def join(self):
        payments =  dt.Frame({'payments': ['Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown', 'Voided trip'],'payment_type':[1,2,3,4,5,6]})
        payments.key="payment_type"
        return self.df[:,:, join(payments)]
        #return "teste"    

    
 