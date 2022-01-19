import pandas as pd

dfs=[]
start_year=2019
end_year=2021


def save(year,month):
	df=pd.concat(dfs, ignore_index=True)
	filename='yellow_tripdata_'+str(start_year)+'_01-'+str(year)+'_'+str(month)
	df.to_parquet(filename+'.parquet',row_group_size=1000000,engine="pyarrow")
	df.to_csv(filename+'.csv',chunksize=1000000)

for year in range(start_year, end_year):
	print(year)
	for month in range(1, 13):
		print(month)
		dfs.append(pd.read_csv("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_"+format(year, '04d')+"-"+format(month, '02d')+".csv",
   	 		dtype={
         		"payment_type": "UInt8",
    		  	"VendorID": "UInt8",
    			"passenger_count": "UInt8",
    			"RatecodeID": "UInt8",
    		},
		))
		if (year==start_year) and (month==6):
			save(year,month)
		elif (year==start_year) and (month==12):
			save(year,month)

save(end_year-1,12)


