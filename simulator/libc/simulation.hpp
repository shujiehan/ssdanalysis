#include <iostream>
#include <queue>
#include <vector>
#include <random>
#include <string>
#include <map>
#include "placement.hpp"
#include "network.hpp"
#include "state.hpp"
#include "parser.hpp"
using namespace std;

struct Event {
  double event_time;
  string event_type;
  int element_id;
  double repair_bwth;
};

struct CompareEventTime {
  bool operator() (Event const& e1, Event const& e2) {
    return e1.event_time > e2.event_time;
  }
};

class Simulation {
  private:
    int num_iterations_;
    double mission_time_;
    int num_racks_, nodes_per_rack_, disks_per_node_, capacity_per_disk_;
    int num_disks_;
    int chunk_size_, code_n_, code_k_, code_l_, num_stripes_;
    string code_type_;
    priority_queue<Event, vector<Event>, CompareEventTime> events_queue_, wait_repair_queue_;
    Placement placement_;
    weibull_distribution<double> disk_fail_dists_, disk_repair_dists_;
    bool use_network_, use_failure_trace_;
    Network network_;
    double network_setting_[2];
    string trace_fname_;
    vector<FailedDisk> *trace_list_;

    bool lazy_repair_;
    int lazy_repair_threshold_;
    
    State state_;
    vector<Disk> disks_;
    // a dict of dict, {disk_idx: {disk_id: [stripe_id...], disk_id: [stripe_id...]}}
    map<int, map<int, vector<int> > > disk_stripes_in_repair_;
    // {stripe_id: [disk_id...]}, keep the stripes that will be lazily repaired and their bad chunks stored on which disks.
    map<int, vector<int> > stripe_disks_to_repair_;

    default_random_engine generator_;

    int num_stripes_repaired_, num_stripes_repaired_single_chunk_;
    int num_stripes_delayed_;

  public:
    Simulation(Configure *configure);
    Simulation(int num_iterations, double mission_time, int num_racks, 
        int nodes_per_rack, int disks_per_node, long capacity_per_disk, 
        int chunk_size, long num_stripes, string code_type, int code_n, int code_k,
        weibull_distribution<double> disk_fail_dists, 
        bool use_network, double network_setting[], bool use_failure_trace, 
        vector<FailedDisk> *trace_list, string trace_fname,
        bool lazy_repair, int lazy_repair_threshold, default_random_engine generator, 
        int code_l=0);
    void Reset();
    void SetDiskFail(int disk_idx, double curr_time);
    void SetDiskRepair(int disk_idx, double curr_time);
    void SetDiskLazyRepair(int disk_idx, double curr_time);
    void SetDiskRepairFollowed(int disk_idx, double curr_time);
    double ComputeRepairTrafficForStripe(int num_failed_chunk, 
        int num_alive_chunk_same_rack, vector<int> alive_chunk_same_rack, 
        int fail_idx);
    int CheckStripeDisksToRepair(int stripe_id);
    int CheckStripeDisksToRepair(int stripe_id, int disk_id);
    bool GetNextEvent(double curr_time, double *curr_event_time,
        string *curr_event_type, vector<int> *device_idx_set);
    unsigned int RunIteration(int *num_failed_stripes, int *num_lost_chunks);
    void Run(unsigned int *data_loss, unsigned long *tot_num_failed_stripes, 
        unsigned long *tot_num_lost_chunks);

};
