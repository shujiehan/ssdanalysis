#include "trace.hpp"

Trace::Trace(string fname, double mission_time)
  :fname_(fname), mission_time_(mission_time), period_(17520.0) {
}

void Trace::ReadTrace(vector<FailedDisk> *trace_list) {
  ifstream infile(fname_, ifstream::in);
  if (!infile.fail()) {
    
    vector<string> row;
    string line, word;
    while(!infile.eof()) {
      getline(infile, line);
      stringstream s(line);
      row.clear();
      while(getline(s, word, ',')) {
        row.push_back(word);
      }
      if (strcmp(row[0].c_str(), string("disk_total_id").c_str()) == 0) {
        assert(strcmp(row[1].c_str(), string("fail_time").c_str()) == 0);
        continue;
      }
      if (row.size() == 0){
        break;
      }
      int disk_id;
      double this_fail_time;
      if (row.size() == 3) {
        disk_id = stoi(row[0]);
        this_fail_time = stod(row[1]);
      } else {
        if (row.size() == 2) {
          disk_id = stoi(row[0]);
          this_fail_time = stod(row[1]);
        }
      }
      row.clear();
      FailedDisk failed_disk = {disk_id, this_fail_time};
      trace_list->push_back(failed_disk);
    }
      infile.close();
    if (mission_time_ > period_) {
      Replay(trace_list);
    }
  } else {
    cout << "Fail to open trace file!" << endl;
  }
}

void Trace::Replay(vector<FailedDisk> *trace_list) {
  int n = mission_time_ / period_;
  vector<FailedDisk> extend_trace_list;
  for(vector<FailedDisk>::iterator it = trace_list->begin(); 
      it < trace_list->end(); it++) {
    for (int i = 1; i < n; i++) {
      double extend_fail_time = it->fail_time + period_ * i;
      FailedDisk failed_disk = {it->disk_id, extend_fail_time};
      extend_trace_list.push_back(failed_disk);
    }
  }
  // concat extend_trace_list to trace_list_
  trace_list->insert(trace_list->end(), extend_trace_list.begin(), 
      extend_trace_list.end());
}
