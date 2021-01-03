#include <assert.h>
#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <sstream>
#include <map>
#include <vector>
using namespace std;

struct FailedDisk {
  int disk_id;
  double fail_time;
};

class Trace {
  private:
    string fname_;
    double mission_time_;
    double period_;

  public:
    Trace(string fname, double mission_time);
    void ReadTrace(vector<FailedDisk> *trace_list);
    void Replay(vector<FailedDisk> *trace_list);
};
