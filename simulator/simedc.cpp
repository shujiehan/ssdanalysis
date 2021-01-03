#include <pthread.h>
#include <cmath>
#include <cstring>
#include <iomanip>
#include "libc/simulation.hpp"

struct Parameters {
  Configure configure;
  vector<unsigned long> results;
};

void *do_it(void *args) {
  Parameters *params = (Parameters *)args;
  Simulation simulation(&(params->configure));
  unsigned int data_loss;
  unsigned long tot_num_failed_stripes, tot_num_lost_chunks;
  simulation.Run(&data_loss, &tot_num_failed_stripes, 
      &tot_num_lost_chunks);
  params->results.push_back((unsigned long)data_loss);
  params->results.push_back(tot_num_failed_stripes);
  params->results.push_back(tot_num_lost_chunks);
  return 0;
}

void summarize_input(Configure configure) {
  printf("*************Configuration*************\n");
  printf("total_iterations = %d\nnum_processes = %d\nmission_time(hours) = %.0f\n",
      configure.num_iterations, configure.num_processes, configure.mission_time);
  printf("num_racks = %d\nnodes_per_rack = %d\ndisks_per_node = %d\n",
      configure.num_racks, configure.nodes_per_rack, configure.disks_per_node);
  printf("capacity_per_disk = %ld\nchunk_size = %d\nnum_stripes = %ld\n", 
      configure.capacity_per_disk, configure.chunk_size, configure.num_stripes);
  printf("code_type = %s\ncode_n = %d\ncode_k = %d\n", 
      configure.code_type.c_str(), configure.code_n, configure.code_k);
  printf("use_failure_trace = %d\n", configure.use_failure_trace);
  printf("use_lazy_repair = %d\nlazy_repair_threshold = %d\n", 
      configure.lazy_repair, configure.lazy_repair_threshold);
  printf("network setting = [%.0f, %.0f]\n", 
      configure.network_setting[0], configure.network_setting[1]);
  printf("**************************************\n\n");
}

double calc_relative_error(int total_iterations, unsigned long tot_data_loss, 
    double mean) {
  vector<int> samples(total_iterations - tot_data_loss, 0);
  vector<int> data_loss(tot_data_loss, 1);
  samples.insert(samples.end(), data_loss.begin(), data_loss.end());
  double sum = 0;
  double stdev = 0;
  for (vector<int>::iterator it = samples.begin(); it < samples.end(); it++) {
    sum += fabs((*it - mean) * (*it - mean));
  }
  if (total_iterations > 1) {
    stdev = sqrt((1.0 / (total_iterations * 1.0 - 1.0)) * sum);
  }
  if (tot_data_loss == 0) {
    return 0;
  } else {
    return (1.960 * (stdev / sqrt(total_iterations))) / mean; // 95% confidence
    //return (1.645 * (stdev / sqrt(total_iterations))) / mean; // 90% confidence
  }
}

void summarize_output(int total_iterations, unsigned long total_chunks,
    unsigned long tot_data_loss, unsigned long tot_num_lost_chunks) {
  double avg_data_loss = 1.0 * tot_data_loss / total_iterations;
  double relative_error = calc_relative_error(total_iterations, tot_data_loss, avg_data_loss);
  double avg_num_lost_chunks = 1.0 * tot_num_lost_chunks / total_iterations;
  double permanent_NOMDL = avg_num_lost_chunks / total_chunks;
  printf("PDL\t\tRE\t\tNOMDL\n");
  printf("%.6f\t%.6f\t%e\n", avg_data_loss, relative_error, permanent_NOMDL);
}

void summarize_output(string res_fname, int idx, int num_racks, int nodes_per_rack, 
    int disks_per_node, int total_disks, int num_failures, int total_iterations, 
    unsigned long total_chunks, unsigned long tot_data_loss, unsigned long tot_num_lost_chunks) {
  double avg_data_loss = 1.0 * tot_data_loss / total_iterations;
  double relative_error = calc_relative_error(total_iterations, tot_data_loss, avg_data_loss);
  double avg_num_lost_chunks = 1.0 * tot_num_lost_chunks / total_iterations;
  double permanent_NOMDL = avg_num_lost_chunks / total_chunks;
  printf("PDL\t\tRE\t\tNOMDL\n");
  printf("%.6f\t%.6f\t%e\n", avg_data_loss, relative_error, permanent_NOMDL);
  ofstream outfile(res_fname, ofstream::app);
  if (!outfile.fail()) {
    if (idx == 0) {
      outfile << "#disks/node,#nodes/rack,#racks,#total disks,#failures,";
      outfile << "PDL,RE,NOMDL\n";
    }
    outfile << disks_per_node << "," << nodes_per_rack << "," << num_racks << ",";
    outfile << total_disks << "," << num_failures << ",";
    outfile << fixed << setprecision(6) << avg_data_loss << "," << relative_error << ",";
    outfile << scientific << permanent_NOMDL << endl;
    outfile.close();
  }
}
int main(int argc, char **argv) {
  if (argc < 2) {
    cout << "Input configure file!" << endl;
    return 0;
  }
  Parser parser(argv[1]);
  Configure configure;
  parser.GetConfiguration(&configure);

  // network setting
  double network_setting[2] = {125, 125}; // 125MB/s
  double *p_network = network_setting;
  configure.network_setting = p_network;

  pthread_t *thread;
  thread = new pthread_t[configure.num_processes];
  Parameters *params;
  params = new Parameters[configure.num_processes];

  // read meta file
  int idx = 0;
  vector<Meta> meta;
  if (argc < 3) {
    parser.GetMeta(&meta);
  } else {
    string meta_fname = argv[2];
    parser.GetMeta(&meta, meta_fname);
  }
  for (vector<Meta>::iterator it_meta = meta.begin(); it_meta < meta.end(); it_meta++) {
    if (idx < configure.start_idx) {
      idx ++;
      continue;
    }
    if (idx > configure.end_idx) {
      break;
    }
    configure.num_racks = it_meta->num_racks;
    configure.nodes_per_rack = it_meta->nodes_per_rack;
    configure.disks_per_node = it_meta->disks_per_node;
    int num_disks = configure.num_racks * configure.nodes_per_rack * configure.disks_per_node;
    configure.num_stripes = configure.capacity_per_disk * num_disks / configure.code_n / configure.chunk_size / 2;
    if (it_meta->num_failures == 0) {
      continue;
    }
    if (it_meta->num_racks < configure.code_n) {
      continue;
    }
    if (argc == 3) {
      configure.num_iterations = it_meta->num_iterations / configure.num_processes + 1;
    }
    char tracefname[80] = "../data/clusters/d";
    strcat(tracefname, to_string(configure.disks_per_node).c_str());
    strcat(tracefname, string("n").c_str());
    strcat(tracefname, to_string(configure.nodes_per_rack).c_str());
    strcat(tracefname, string(".csv").c_str());
    cout << tracefname << endl;
    configure.trace_fname = string(tracefname);

    // read trace
    vector<FailedDisk> trace_list;
    if (configure.use_failure_trace) {
      Trace trace(configure.trace_fname, configure.mission_time);
      trace.ReadTrace(&trace_list);
    }
    configure.trace_list = &trace_list;

    summarize_input(configure);

    // note that rand() is not a thread-safe function
    // when using multiple-threads, the results are not reproduced.
    srand(configure.seed); 


    //vector<future<vector<unsigned long>> > fut;
    for (int i = 0; i < configure.num_processes; i++) {
      // change rseed for each thread when multi-threading
      default_random_engine generator(configure.seed + i * 1000); 
      configure.generator = generator;
      params[i].configure = configure;
      vector<unsigned long> results;
      params[i].results = results;
      pthread_create(&thread[i], NULL, do_it, &params[i]);
      //fut.push_back(async(do_it, configure));
    }
    unsigned long tot_data_loss = 0, tot_num_failed_stripes = 0, tot_num_lost_chunks = 0;
    for (int i = 0; i < configure.num_processes; i++) {
      //vector<unsigned long> results = fut[i].get();
      pthread_join(thread[i], NULL);
      tot_data_loss += params[i].results[0];
      tot_num_failed_stripes += params[i].results[1];
      tot_num_lost_chunks += params[i].results[2];
    }
    int total_iterations = configure.num_processes * configure.num_iterations;
    unsigned long total_chunks = configure.num_stripes * configure.code_n;
    summarize_output(configure.res_fname, idx, configure.num_racks,
        configure.nodes_per_rack, configure.disks_per_node, 
        it_meta->total_disks, it_meta->num_failures, total_iterations, 
        total_chunks, tot_data_loss, tot_num_lost_chunks);
    idx ++;
  }
  delete [] thread;
  delete [] params;

  return 0;
}
