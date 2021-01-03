# Analysis Scripts

We use the scripts to analyze the correlated failures of SSDs including intra-node and intra-rack failures in the dataset.

## Prerequisite

- Python3: Please install [numpy](https://numpy.org/) and [pandas](https://pandas.pydata.org/).

## Usage

- Assume that the dataset is stored under `../data/`

  `python measure.py ../data/`

- The results will be stored under `resutls/`

## Functions

- `get_intra_failures`: output the percentages of failures for different intra-node and intra-rack failure group sizes in Finding 1.
  - Results: `finding_1_node.csv` and `finding_1_rack.csv`
- `compute_conditional_prob`: output the conditional probabilities of having an additional SSD failure for different failure sizes per intra-node or intra-rack failure group in Finding 2.
  - Results: `cond_prob_node.csv` and `cond_prob_rack.csv`
- `get_intra_failures_diff_thresholds`: output the percentages of intra-node (intra-rack) failures
  broken down by different thresholds of the intra-node (intra-rack) failure time intervals in Finding 3.
  - Results: `finding_3.csv`
- `spatial_correlations`: output the relative percentages of failures for different sets of intra-node or intra-rack failure group sizes across impacting factors (i.e., drive models, age, lithography, capacity, and applications) in Figures 4, 7, 9, 11, and 15.
  - Results: `spatial_node*.csv` and `spatial_rack*.csv`
- `temporal_correlations`: output the relative percentages of failures for different thresholds of intra-node or intra-rack failure time intervals across impacting factors (i.e., drive models, age, lithography, capacity, and applications) in Figures 6, 8, 10, 12, and 17.
  - Results: `temporal_node*.csv` and `temporal_rack*.csv`	
- `get_avg_num_of_ssds`: output the average numbers of SSDs per node or rack for the drive models or applications in Figures 5 and 16.
  - Results: `avg_num_model_node.csv`, `avg_num_model_rack.csv`, `avg_num_app_node.csv`, and `avg_num_app_rack.csv`	
- `check_machine_room`: check machine rooms for A2 and B3 as these two drive models have a high relative percentage of intra-rack failures in Finding 4.
  - Results: print out on screen
- `check_rated_life_used`: check the rated life used for different age groups of intra-node and intra-rack failures in Finding 7.
  - Results: `intra_node_age_rated_life_used.csv` and `intra_rack_age_rated_life_used.csv`
- `check_writes_percentage`: output the percentages of writes for the applications in Figure 14 (a).
  - Results: `app_writes_percentage.csv`
- `check_age_for_app`: check the age of intra-node and intra-rack failures with the failure time interval threshold of 1 minute for the applications in Finding 12.
  - Results: `check_node_app_age.csv` and `check_rack_app_age.csv`
- `srcc`: output SRCC values between each SMART attribute and the intra-node or intra-rack failures in Finding 13.
  - Results: `srcc_node.csv` and `srcc_rack.csv`	
- `check_homogeneous_component`: output the percentages of nodes (racks) attached with SSDs from the same drive model among nodes (racks) attached with at least two SSDs in Section 2.1 
  - Results: print out on screen

## Contact

Please email to Shujie Han (sjhan@cse.cuhk.edu.hk) if you have any questions.