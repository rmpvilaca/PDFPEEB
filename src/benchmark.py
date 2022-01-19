
import time, importlib, argparse
import numpy as np

operations=["mean","sum","unique_rows","groupby","multiple_groupby","join"]
runs=10

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Benchmark Python DataFrames.')
    parser.add_argument('--framework', required=True, help='Class of the dataframe to benchmark')
    parser.add_argument('--dataset', required=True, help='dataset')
    parser.add_argument('--workers', type=int, required=False, help='number of workers', default=1)
   
    args = parser.parse_args()
    print(args)
    module = importlib.import_module(args.framework)
    klass =getattr(module, args.framework)
    sut = klass()
    operations_time={}
    filtered_operations_time={}
    methods={}
    for operation in operations:
        operations_time[operation]=[]
        filtered_operations_time[operation]=[]
        try:
            methods[operation]=getattr(klass,operation)
        except AttributeError as error:
            # Ignore. Not added to map and latter CSV with empty values
            pass

    print(sut.__class__)
    t0 = time.time()
    if (args.framework == "Dask"):
        sut.load(args.dataset,args.workers)
    else:
        sut.load(args.dataset)
    load_time = time.time() - t0
    print("Load time: {:.8f}".format(load_time))

    t_start = time.time()
    
    for i  in range(runs):
        for operation in operations:
            if operation in methods:
                t0 = time.time()
                methods[operation](sut)
                operations_time[operation].append(time.time()-t0)
    
    sut.filter()

    for i  in range(runs):
        for operation in operations:
            if operation in methods:
                t0 = time.time()
                methods[operation](sut)
                filtered_operations_time[operation].append(time.time()-t0)  

    ops_time=time.time()-t_start
              
    f = open("results_"+args.dataset+".csv", "a")
    f.write(args.framework)
    f.write(",")

    f.write(str(args.workers))
    f.write(",")

    f.write(str(load_time))

    for operation in operations:
        if operation in methods:
            f.write(",")
            f.write(str(operations_time[operation][0]))
            f.write(",")
            f.write(str(np.mean(operations_time[operation][1:])))
            f.write(",")
            f.write(str(np.std(operations_time[operation][1:])))
            f.write(",")
            f.write(str(filtered_operations_time[operation][0]))
            f.write(",")
            f.write(str(np.mean(filtered_operations_time[operation][1:])))
            f.write(",")
            f.write(str(np.std(filtered_operations_time[operation][1:])))   
        else:
            for i in range(6):
                f.write(",")
    f.write(",")
    f.write(str(runs*2.0*len(methods)/ops_time))
    f.write("\n")
    f.close()

