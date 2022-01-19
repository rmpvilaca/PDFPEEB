import matplotlib.pyplot as plt
import numpy as np

operations=["mean","sum","unique_rows","groupby","multiple_groupby","join"]
frameworks=['Pandas', 'Bodo', 'Vaex', 'Koalas', 'Polars', 'Datatable', 'Modin', 'DuckDB','Cylon']#Sdc

datasets=["yellow_tripdata_2019_01-2019_6","yellow_tripdata_2019_01-2019_12","yellow_tripdata_2019_01-2020_12"]
names={"yellow_tripdata_2019_01-2019_6":"4GB","yellow_tripdata_2019_01-2019_12":"8GB","yellow_tripdata_2019_01-2020_12":"12GB"}

# width of the bars
barWidth = 0.9
load = {}
max = 5000000000

# Dict {dataset -> { operation -> [times]}}
operations_warmup_time = {}
operations_mean_time = {}
operations_var_time = {}
filtered_operations_warmup_time = {}
filtered_operations_mean_time = {}
filtered_operations_var_time = {}

energy = {}

# TODO: Adapt to missing data and replace with n.a.
for dataset in datasets:
    print(dataset)
    load[dataset] =[]
    energy[dataset] = []
    operations_warmup_time[dataset] = {}
    operations_mean_time[dataset] = {}
    operations_var_time[dataset] = {}
    filtered_operations_warmup_time[dataset] = {}
    filtered_operations_mean_time[dataset] = {}
    filtered_operations_var_time[dataset] = {}
    f = open("results_"+dataset+".csv", "r")
    f_cols={}
    for line in f.readlines():
        cols = line.split(",")
        framework=cols[0] if int(cols[1])==1 else cols[0]+"-"+cols[1]
        f_cols[framework]=cols
    f.close()
    for operation in operations:
            operations_warmup_time[dataset][operation] = []
            operations_mean_time[dataset][operation] = []
            operations_var_time[dataset][operation] = []
            filtered_operations_warmup_time[dataset][operation] = []
            filtered_operations_mean_time[dataset][operation] = []
            filtered_operations_var_time[dataset][operation] = []

    for framework in frameworks:
        if framework in f_cols:
            cols=f_cols[framework]
            load[dataset].append(float(cols[2]))
            index=3
            for operation in operations:
                if cols[index]:
                    operations_warmup_time[dataset][operation].append(float(cols[index]))
                    operations_mean_time[dataset][operation].append(float(cols[index+1]))
                    operations_var_time[dataset][operation].append(float(cols[index+2]))
                    filtered_operations_warmup_time[dataset][operation].append(float(cols[index+3]))
                    filtered_operations_mean_time[dataset][operation].append(float(cols[index+4]))
                    filtered_operations_var_time[dataset][operation].append(float(cols[index+5]))
                else:
                    # This operation for this framework is missing
                    operations_warmup_time[dataset][operation].append(np.nan)
                    operations_mean_time[dataset][operation].append(np.nan)
                    operations_var_time[dataset][operation].append(np.nan)
                    filtered_operations_warmup_time[dataset][operation].append(np.nan)
                    filtered_operations_mean_time[dataset][operation].append(np.nan)
                    filtered_operations_var_time[dataset][operation].append(np.nan)
                index+=6
            energy[dataset].append(float(cols[index])/float(cols[index+1]))
        else:
            # Full row for this framework is missing
            load[dataset].append(np.nan)
            for operation in operations:
                operations_warmup_time[dataset][operation].append(np.nan)
                operations_mean_time[dataset][operation].append(np.nan)
                operations_var_time[dataset][operation].append(np.nan)
                filtered_operations_warmup_time[dataset][operation].append(np.nan)
                filtered_operations_mean_time[dataset][operation].append(np.nan)
                filtered_operations_var_time[dataset][operation].append(np.nan)
            energy[dataset].append(np.nan)

# The x position of bars
warmup_pos={}
filtered_warmup_pos={}
time_pos={}
filtered_time_pos={}

warmup_pos=np.arange(start=barWidth,stop=len(frameworks)*4,step=4)

filtered_warmup_pos=[x + barWidth for x in warmup_pos]

time_pos=[x + barWidth*2 for x in warmup_pos]

filtered_time_pos=[x + barWidth*3 for x in warmup_pos]

print("POS:",warmup_pos,filtered_warmup_pos,time_pos,filtered_time_pos)

# making subplots
fig, ax = plt.subplots(1,len(datasets),figsize=(16,9),dpi=150,sharex=True)
fig.suptitle('Load Time', fontsize=16)

ax[0].set_xticks(warmup_pos)
ax[0].set_xticklabels(frameworks)


# Create load bars
for index,dataset in enumerate(datasets):
    # set the title to subplots
    ax[index].set_title(names[dataset])
    ax[index].bar(warmup_pos, load[dataset], width = barWidth, edgecolor = 'black', color="blue", capsize=7, label='load')
    for i, value in enumerate(load[dataset]):
        ax[index].text(warmup_pos[i]-barWidth, value, "{:10.1f}".format(value) if value < max else "n.a.",fontsize=5)
    
    ax[index].tick_params(axis='both', which='major', labelsize=10, rotation=60)
    plt.sca(ax[index])
    plt.ylabel('Time(s)')

# set spacing
fig.tight_layout()

plt.savefig('load.png')
plt.savefig('load.pdf')


for operation in operations:
    #Clear 
    plt.clf() 
    fig, ax = plt.subplots(1,len(datasets),figsize=(16,9),dpi=150,sharex=True)
    fig.suptitle(operation+' Time', fontsize=16)
    ax[0].set_xticks(time_pos)
    ax[0].set_xticklabels(frameworks)

    for index,dataset in enumerate(datasets):
         # set the title to subplots
        ax[index].set_title(names[dataset])
        # Create warmup bar
        ax[index].bar(warmup_pos, operations_warmup_time[dataset][operation], width = barWidth, color='lightskyblue',edgecolor = 'black', capsize=7, label='warmup_'+operation)         # Create filtered warmup bars
        for i, value in enumerate(operations_warmup_time[dataset][operation]):
            ax[index].text(warmup_pos[i]-barWidth, value, "{:10.1f}".format(value) if value < max else "n.a.",fontsize=5)

        # Create warmup filtered operation bar
        ax[index].bar(filtered_warmup_pos, filtered_operations_warmup_time[dataset][operation], width = barWidth, color='lightgreen', edgecolor = 'black', capsize=7, label='filtered_warmup_'+operation)
        for i, value in enumerate(filtered_operations_warmup_time[dataset][operation]):
            ax[index].text(filtered_warmup_pos[i]-barWidth, value, "{:10.1f}".format(value) if value < max else "n.a.",fontsize=5)

        # Create operation bar
        ax[index].bar(time_pos, operations_mean_time[dataset][operation], width = barWidth, color='deepskyblue',edgecolor = 'black', yerr=operations_var_time[dataset][operation], capsize=7, label=operation)
        for i, value in enumerate(operations_mean_time[dataset][operation]):
            ax[index].text(time_pos[i]-barWidth, value, "{:10.1f}".format(value) if value < max else "n.a.",fontsize=5)

        # Create filtered operation bar
        ax[index].bar(filtered_time_pos, filtered_operations_mean_time[dataset][operation], width = barWidth, color='green', edgecolor = 'black', yerr=filtered_operations_var_time[dataset][operation], capsize=7, label='filtered_'+operation)
        for i, value in enumerate(filtered_operations_mean_time[dataset][operation]):
            ax[index].text(filtered_time_pos[i]-barWidth, value, "{:10.1f}".format(value) if value < max else "n.a.",fontsize=5)

        # general layout
        ax[index].tick_params(axis='both', which='major', labelsize=10, rotation=60)

        plt.sca(ax[index])
        plt.ylabel('Time(s)')
        plt.legend()
    
    # set spacing
    fig.tight_layout()  

    plt.savefig(operation+'.png')
    plt.savefig(operation+'.pdf')

#Clear 
plt.clf() 

fig, ax = plt.subplots(1,len(datasets),figsize=(16,9),dpi=150, sharex=True)
fig.suptitle('Energy consuption', fontsize=16)
ax[0].set_xticks(warmup_pos)
ax[0].set_xticklabels(frameworks)

# Create energy load bars
for index,dataset in enumerate(datasets):
    ax[index].set_title(names[dataset])
    ax[index].bar(warmup_pos, energy[dataset], width = barWidth,color="green", edgecolor = 'black', capsize=7, label='energy')
    
    for i, value in enumerate(energy[dataset]):
        ax[index].text(warmup_pos[i]-barWidth, value, "{:10.1f}".format(value) if value < max else "n.a.",fontsize=5)

    # general layout
    ax[index].tick_params(axis='both', which='major', labelsize=10, rotation=60)
    plt.sca(ax[index])
    plt.ylabel('Operation/s/W')
 
# set spacing
#fig.tight_layout()  

plt.savefig('energy.png')
plt.savefig('energy.pdf')
