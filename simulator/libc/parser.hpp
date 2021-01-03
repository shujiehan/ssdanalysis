#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <random>
#include <map>
#include <vector>
#include "trace.hpp"
using namespace std;

struct Configure {
  int num_processes;
  int num_iterations;
  double mission_time;
  int num_racks;
  int nodes_per_rack;
  int disks_per_node;
  long capacity_per_disk;
  int chunk_size;
  long num_stripes;
  string code_type;
  int code_n;
  int code_k;
  weibull_distribution<double> disk_fail_dists;
  bool use_network;
  double *network_setting;
  bool use_failure_trace;
  string trace_fname;
  bool lazy_repair;
  int lazy_repair_threshold;
  default_random_engine generator;
  int code_l;
  int seed;
  vector<FailedDisk> *trace_list;
  string res_fname;
  int start_idx;
  int end_idx;
};

struct Meta {
  int num_racks;
  int nodes_per_rack;
  int disks_per_node;
  int total_disks;
  int num_failures;
  int num_iterations;
};

class Parser {
  private:
    string conf_fname_;
  public:
    Parser(string conf_fname);
    void GetConfiguration(Configure *configure);
    void GetMeta(vector<Meta> *meta);
    void GetMeta(vector<Meta> *meta, string fname);
};
