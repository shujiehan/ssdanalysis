#include "parser.hpp"

Parser::Parser(string conf_fname):conf_fname_(conf_fname){}

void Parser::GetConfiguration(Configure *configure) {
  ifstream infile(conf_fname_, ifstream::in);
  if (!infile.fail()) {
    string line, word;
    map<string, string> config_map;
    while(!infile.eof()) {
      getline(infile, line);
      stringstream s(line);
      string key;
      if (getline(s, key, '=')) {
        string value;
        if (getline(s, value)) {
          config_map[key] = value;
        }
      }
    }
    configure->num_processes = stoi(config_map[string("processes")]);
    int num_iterations = stoi(config_map[string("iterations")]);
    if (num_iterations % configure->num_processes != 0) {
      cout << "num_iterations should be divisible by num_process!" << endl;
    }
    int sub_num_iterations = num_iterations / configure->num_processes;
    configure->num_iterations = sub_num_iterations;
    configure->mission_time = stod(config_map[string("mission")]);
    if (config_map.find(string("num_racks")) != config_map.end()) {
      configure->num_racks = stoi(config_map[string("num_racks")]);
      configure->nodes_per_rack = stoi(config_map[string("nodes_per_rack")]);
      configure->disks_per_node = stoi(config_map[string("disks_per_node")]);
      int num_disks = configure->num_racks * configure->nodes_per_rack * configure->disks_per_node;
      configure->num_stripes = configure->capacity_per_disk * num_disks / configure->code_n / configure->chunk_size / 2;
    }
    configure->chunk_size = stoi(config_map[string("chunk_size")]);
    configure->code_type = config_map[string("code_type")];
    configure->code_n = stoi(config_map[string("code_n")]);
    configure->code_k = stoi(config_map[string("code_k")]);
    configure->use_failure_trace = stoi(config_map[string("failure_trace")]);
    if (config_map.find(string("trace_fname")) != config_map.end()) {
      configure->trace_fname = config_map[string("trace_fname")];
    } else {
      configure->trace_fname = "";
    }
    configure->lazy_repair = stoi(config_map[string("lazy_repair")]);
    configure->lazy_repair_threshold = stoi(config_map[string("lazy_th")]);

    configure->code_l = stoi(config_map[string("code_l")]);
    configure->seed = stoi(config_map[string("seed")]);
    configure->res_fname = config_map[string("res_fname")];
    configure->start_idx = stoi(config_map[string("start_idx")]);
    configure->end_idx = stoi(config_map[string("end_idx")]);

    configure->use_network = true;
    configure->capacity_per_disk = 512 * 1024;
    weibull_distribution<double> disk_fail_dists(1.0, 8760/0.0116);
    configure->disk_fail_dists = disk_fail_dists;

  } else {
    cout << "Fail to open configuration file!" << endl;
  }
}

void Parser::GetMeta(vector<Meta> *meta) {
  ifstream infile("../data/meta.csv", ifstream::in);
  if (!infile.fail()) {
    string line, word;
    vector<string> row;
    while(!infile.eof()) {
      getline(infile, line);
      stringstream s(line);
      row.clear();
      while(getline(s, word, ',')) {
        row.push_back(word);
      }
      if (strcmp(row[0].c_str(), string("#disks/node").c_str()) == 0) {
        assert(strcmp(row[1].c_str(), string("#nodes/rack").c_str()) == 0);
        assert(strcmp(row[2].c_str(), string("#racks").c_str()) == 0);
        assert(strcmp(row[3].c_str(), string("#total disks").c_str()) == 0);
        assert(strcmp(row[4].c_str(), string("#failures").c_str()) == 0);
        continue;
      }
      if (row.size() == 0) {
        break;
      }
      Meta one_meta;
      one_meta.disks_per_node = stoi(row[0]);
      one_meta.nodes_per_rack = stoi(row[1]);
      one_meta.num_racks = stoi(row[2]);
      one_meta.total_disks = stoi(row[3]);
      one_meta.num_failures = stoi(row[4]);
      row.clear();
      meta->push_back(one_meta);
    }
    infile.close();
  }
}

void Parser::GetMeta(vector<Meta> *meta, string fname) {
  ifstream infile(fname, ifstream::in);
  if (!infile.fail()) {
    string line, word;
    vector<string> row;
    while(!infile.eof()) {
      getline(infile, line);
      stringstream s(line);
      row.clear();
      while(getline(s, word, ',')) {
        row.push_back(word);
      }
      if (strcmp(row[0].c_str(), string("#disks/node").c_str()) == 0) {
        assert(strcmp(row[1].c_str(), string("#nodes/rack").c_str()) == 0);
        assert(strcmp(row[2].c_str(), string("#racks").c_str()) == 0);
        assert(strcmp(row[3].c_str(), string("#total disks").c_str()) == 0);
        assert(strcmp(row[4].c_str(), string("#failures").c_str()) == 0);
        assert(strcmp(row[5].c_str(), string("iterations").c_str()) == 0);
        continue;
      }
      if (row.size() == 0) {
        break;
      }
      Meta one_meta;
      one_meta.disks_per_node = stoi(row[0]);
      one_meta.nodes_per_rack = stoi(row[1]);
      one_meta.num_racks = stoi(row[2]);
      one_meta.total_disks = stoi(row[3]);
      one_meta.num_failures = stoi(row[4]);
      one_meta.num_iterations = stoi(row[5]);
      row.clear();
      meta->push_back(one_meta);
    }
    infile.close();
  }
}
