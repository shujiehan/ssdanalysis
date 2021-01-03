# Simulator

We extend the C++ discrete-event simulator [SimEDC](http://adslab.cse.cuhk.edu.hk/software/simedc/) to support the reliability evaluation on our [dataset](https://github.com/alibaba-edu/dcbrain/tree/master/diskdata/ssd_open_data).

## Prerequisite

gcc version 5.4.0

## Usage

### Parse the topology of the dataset
Assume that the dataset is stored in `../data/`.
* `python parse_topology.py ../data/`
* You will obtain the topology metadata (`meta.csv`) under `../data/`, in which we have 128 clusters with  at least 16 racks for each cluster.

### Run experiments

- All configurations files are under the directory (`conf/`).
- We assume the results are stored in `results/`, so please first create `results/` under this directory (`simulator/`).
- Compile: `make`
- Run experiments by `./simedc conf/[config file]`
- Note that we set the number of processes as 1 by default. You can change the value of `processes` in configuration files for multithreading (see below for parameters details).

### Configuration files for testing different redundancy schemes

- You can use the configuration files in the second column to generate independent failures by mathematical failure model (i.e., exponential distribution)
- You can use the configuration files in the third column to simulate correlated failures driven by our dataset.

| Redundancy schemes |    Independent failures    |    Correlated failures     |
| :----------------: | :------------------------: | :------------------------: |
|       Rep(2)       |  `rep2_eager_model.conf`   |  `rep2_eager_trace.conf`   |
|       Rep(3)       |  `rep3_eager_model.conf`   |  `rep3_eager_trace.conf`   |
|      RS(6,3)       |  `rs63_eager_model.conf`   |  `rs63_eager_trace.conf`   |
|      RS(10,4)      |  `rs104_eager_model.conf`  |  `rs104_eager_trace.conf`  |
|      RS(12,4)      |  `rs124_eager_model.conf`  |  `rs124_eager_trace.conf`  |
|    LRC(12,2,2)     | `lrc1222_eager_model.conf` | `lrc1222_eager_trace.conf` |

### Configuration files for testing different repair schemes

* Eager repair: The above configuration files enable eager repair by default.

* Lazy repair: We consider RS(10,4) and RS(12,4) with different thresholds of lazy repair. The maximum threshold is 4 chunks.

  | Lazy repair threshold |          RS(10,4)           |          RS(12,4)           |
  | :-------------------: | :-------------------------: | :-------------------------: |
  |       2 chunks        | `rs104_lazy_trace_th2.conf` | `rs124_lazy_trace_th2.conf` |
  |       3 chunks        | `rs104_lazy_trace_th3.conf` | `rs124_lazy_trace_th3.conf` |
  |       4 chunks        | `rs104_lazy_trace_th4.conf` | `rs124_lazy_trace_th4.conf` |

### Parameters in configuration files

- `processes`: number of processes (i.e., threads) for running
- `iterations`: total iterations for running on each cluster
- `mission`: mission time, 10 years by default
- `chunk_size`: we set the chunk size as 256 MiB by default
- `code_type`: `Rep` for replication, `RS` for Reed-Solomon coding, and `LRC` for Local Reconstruction Coding
- `code_n`: total chunks in a coding group
- `code_k`: number of data chunks
- `code_l`: number of local coding groups only for LRC
- `failure_trace`: whether using failure trace in our dataset (1 for enabling and 0 otherwise)
- `lazy_repair`: whether enabling lazy repair (1 for enabling and 0 otherwise)
- `lazy_th`: threshold of chunks for lazy repair (the minimum value is 2)
- `seed`: random seed
- `res_fname`: path and file name of results
- `start_idx`: start index of clusters in `../data/meta.csv` (0 is the minimum value)
- `end_idx`: end index of clusters in `../data/meta.csv` (127 is the maximum value since we have 128 clusters)

### Results

- The results are stored in `results/` in `.csv` format.
  - We report the probability of data loss (`PDL`), relative error of PDL (`RE`), and normalized data loss (`NOMDL`).
  - If RE > 20% for a cluster, you can run more iterations (how to set the number of extra iterations, you may refer to [SimEDC paper](http://www.cse.cuhk.edu.hk/~pclee/www/pubs/srds17simedc.pdf).)

## Contact

Please email to Shujie Han (sjhan@cse.cuhk.edu.hk) if you have any questions.
