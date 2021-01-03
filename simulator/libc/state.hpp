#include <cstring>
#include <iostream>
#include <string>
#include <bitset>
#include <vector>
#include "disk.hpp"
using namespace std;

#define MAX 400000

class State{
  private:
    static const string kCurrStateOK;
    static const string kCurrStateDegraded;
    int num_disks_;
    bitset<MAX> bm_avail_disks_, bm_failed_disks_;
    int num_failed_disks_;
    string sys_state_;

  public:
    State();
    State(int num_disks);
    void UpdateSysState();
    bool UpdateState(string event_type, vector<int> disk_id_set);
    void FailDisk(int disk_id);
    void RepairDisk(int disk_id);
    vector<int> GetFailedDisks();
    void Copy(State state);
    int GetNumDisks();
    int GetNumFailedDisks();
    unsigned long GetBmFailedDisks();
    unsigned long GetBmAvailDisks();
    string GetSysState();
};
