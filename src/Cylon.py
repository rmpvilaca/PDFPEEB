from pycylon import read_csv #, DataFrame, CylonEnv
# from pycylon.net import MPIConfig

# config: MPIConfig = MPIConfig()
# env: CylonEnv = CylonEnv(config=config, distributed=False)

class Cylon:
    def load(self, file_path):
        self.df = read_csv(file_path+".csv")

    def filter(self):
        pass
    
    def groupby(self):
        return self.df.groupby(by=4).agg({"tip_amount":"mean"})

    def multiple_groupby(self):
        return self.df.groupby(by=[4,10]).agg({"tip_amount":"mean"})

    # def join(self):
    #     df2=DataFrame([[1,2,3,4,5,6],['Credit Card', 'Cash','No Charge','Dispute','Unknown','Voided trip']],columns=['payment_type','payment_name'])
    #     df2.set_index([1], inplace=True)
    #     return self.df.merge(df2, left_on=[10],right_on=[1])