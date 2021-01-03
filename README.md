# Source Code and Dataset

The repository includes the source code of analysis for correlated failures of SSDs and trace-driven simulator of storage reliability under different redundancy schemes, as well as the dataset of SSD failures in Alibaba.

## Publication

Shujie Han, Patrick P. C. Lee, Fan Xu, Yi Liu, Cheng He, and Jiongzhou Liu.  
**"An In-Depth Study of Correlated Failures in Production SSD-Based Data Centers."**  
Proceedings of the 19th USENIX Conference on File and Storage Technologies (FAST 2021), February 2021.  
(AR: 28/130 = 21.5%)

## Directories

- `data/` ([dataset of SSD failures in Alibaba](https://github.com/alibaba-edu/dcbrain/tree/master/diskdata/ssd_open_data)) spans two years from Jan. 1, 2018 to Dec. 31, 2019, including 
  - `ssd_failure_tag.tar.gz`: the SMART logs and trouble tickes for all SSD failures among the 11 drive models,
  - `location_info_of_ssd.tar.gz`: locations, and applications, for the 11 drive models, and
  - `smart_log_20191231.tar.gz`: the last day of SMART logs for all SSDs of the 11 drive models.
- `analysis/` includes the scripts of the correlation analysis for SSD failures.
- `simulator/` includes the source code and configuration files for the simulator which extends [SimEDC](http://adslab.cse.cuhk.edu.hk/software/simedc/) to support the reliability evaluation on our dataset.

## Contact

Please email to Shujie Han (sjhan@cse.cuhk.edu.hk) if you have any questions.  
