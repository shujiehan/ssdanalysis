#include "simulation.hpp"

Simulation::Simulation(Configure *c)
  :num_iterations_(c->num_iterations), mission_time_(c->mission_time),
   num_racks_(c->num_racks), nodes_per_rack_(c->nodes_per_rack),
   disks_per_node_(c->disks_per_node),
   capacity_per_disk_(c->capacity_per_disk), chunk_size_(c->chunk_size),
   num_stripes_(c->num_stripes), code_type_(c->code_type), code_n_(c->code_n),
   code_k_(c->code_k), code_l_(c->code_l),
   disk_fail_dists_(c->disk_fail_dists), use_network_(c->use_network),
   use_failure_trace_(c->use_failure_trace),
   trace_fname_(c->trace_fname),
   lazy_repair_(c->lazy_repair), trace_list_(c->trace_list),
   lazy_repair_threshold_(c->lazy_repair_threshold), generator_(c->generator),
   num_disks_(num_racks_ * nodes_per_rack_ * disks_per_node_){
  network_setting_[0] = c->network_setting[0]; 
  network_setting_[1] = c->network_setting[1];
}

Simulation::Simulation(int num_iterations, double mission_time, int num_racks, int nodes_per_rack,
                       int disks_per_node, long capacity_per_disk, int chunk_size, 
                       long num_stripes, string code_type, int code_n, 
                       int code_k, weibull_distribution<double> disk_fail_dists, 
                       bool use_network, double network_setting[], 
                       bool use_failure_trace,
                       vector<FailedDisk> *trace_list,
                       string trace_fname, bool lazy_repair,
                       int lazy_repair_threshold, default_random_engine generator, 
                       int code_l)
  :num_iterations_(num_iterations), mission_time_(mission_time), num_racks_(num_racks), 
   nodes_per_rack_(nodes_per_rack), disks_per_node_(disks_per_node),
   capacity_per_disk_(capacity_per_disk), chunk_size_(chunk_size),
   num_stripes_(num_stripes), code_type_(code_type), code_n_(code_n),
   code_k_(code_k), code_l_(code_l),
   disk_fail_dists_(disk_fail_dists),
   use_network_(use_network), use_failure_trace_(use_failure_trace), 
   trace_list_(trace_list), trace_fname_(trace_fname),
   lazy_repair_(lazy_repair), lazy_repair_threshold_(lazy_repair_threshold), 
   num_disks_(num_racks_ * nodes_per_rack_ * disks_per_node_),
   generator_(generator) {
  network_setting_[0] = network_setting[0]; 
  network_setting_[1] = network_setting[1];
}

void Simulation::Reset() {
  state_ = State(num_disks_);
  if (use_failure_trace_) {
    for (int disk_id = 0; disk_id < num_disks_; disk_id ++) {
      Disk d = Disk();
      d.InitClock(0.0);
      d.InitState();
      disks_.push_back(d);
    }
  } else {
    for (int disk_id = 0; disk_id < num_disks_; disk_id++) {
      Disk d = Disk(disk_fail_dists_);
      d.InitClock(0.0);
      d.InitState();
      disks_.push_back(d);
    }
  }
  events_queue_ = priority_queue<Event, vector<Event>, CompareEventTime>();
  wait_repair_queue_ = priority_queue<Event, vector<Event>, CompareEventTime>();
  disk_stripes_in_repair_ = map<int, map<int, vector<int> > > ();
  stripe_disks_to_repair_ = map<int, vector<int> > ();

  if (use_failure_trace_) {
    vector<FailedDisk>::iterator it;
    for (vector<FailedDisk>::iterator it = trace_list_->begin(); it < trace_list_->end(); it++) {
      if (it->fail_time <= mission_time_) {
        Event e = {it->fail_time, Disk::kEventDiskFail, it->disk_id, 0};
        events_queue_.push(e);
      }
    }
  } else {
    for (int disk_id = 0; disk_id < num_disks_; disk_id++) {
      double disk_fail_time = disk_fail_dists_(generator_);
      if (disk_fail_time <= mission_time_) {
        Event e = {disk_fail_time, Disk::kEventDiskFail, disk_id, 0};
        events_queue_.push(e);
      }
    }
  }
  placement_ = Placement(num_racks_, nodes_per_rack_, disks_per_node_, 
                         capacity_per_disk_, num_stripes_, chunk_size_, 
                         code_type_, code_n_, code_k_, code_l_, generator_);
  network_ = Network(num_racks_, nodes_per_rack_, network_setting_);
  num_stripes_repaired_ = 0;
  num_stripes_repaired_single_chunk_ = 0;
  num_stripes_delayed_ = 0;
}

void Simulation::SetDiskFail(int disk_idx, double curr_time) {
  double disk_fail_time = disk_fail_dists_(generator_) + curr_time;
  if (disk_fail_time <= mission_time_) {
    Event e = {disk_fail_time, Disk::kEventDiskFail, disk_idx, 0};
    events_queue_.push(e);
  }
}

void Simulation::SetDiskRepair(int disk_idx, double curr_time) {
  int rack_id = (int) (disk_idx / (nodes_per_rack_ * disks_per_node_));
  if (network_.GetAvailCrossRackRepairBwth() == 0) {
    Event e = {curr_time, "", disk_idx, 0};
    wait_repair_queue_.push(e);
  } else { // available cross rack repair bwth > 0
    double cross_rack_download = 0;
    vector<int> stripes_to_repair = placement_.GetStripesToRepair(disk_idx);
    int num_stripes_repaired_actual = 0;

    vector<int>::iterator iter_stripe;
    for (iter_stripe = stripes_to_repair.begin(); 
        iter_stripe < stripes_to_repair.end(); iter_stripe++) {
      int num_failed_chunk = 0;
      int num_alive_chunk_same_rack = 0;
      int idx = 0;
      int fail_idx = 0;
      int global_failed_disks_num = 0;
      int stripe_failed_disks_num[2] = {0};
      vector<int> alive_chunk_same_rack;
      
      vector<int> disks_attached = placement_.GetStripeLocation(*iter_stripe);
      vector<int>::iterator iter_disk;
      for (iter_disk = disks_attached.begin(); 
          iter_disk < disks_attached.end(); iter_disk++) {
        // RS, DRC, replication
        if (strcmp(code_type_.c_str(), Placement::kCodeTypeLRC.c_str()) != 0) {
          if (strcmp(disks_[*iter_disk].GetCurrState().c_str(), Disk::kStateCrashed.c_str()) == 0) 
            num_failed_chunk ++;
          else {
            if ((int)(*iter_disk / (nodes_per_rack_ * disks_per_node_)) == rack_id) {
              num_alive_chunk_same_rack ++;
            }
          }
        } else { // LRC
          if (strcmp(disks_[*iter_disk].GetCurrState().c_str(), Disk::kStateCrashed.c_str()) == 0) {
            num_failed_chunk ++;
            if (disk_idx == *iter_disk) fail_idx = idx;
            // check position of failed disk
            if (idx == Placement::kLrcGlobalParity[0] || idx == Placement::kLrcGlobalParity[1]) {
              // global parity
              global_failed_disks_num ++;
            } else {
              if (idx != Placement::kLrcLocalParity[0] && idx != Placement::kLrcLocalParity[1]) {
                // data group
                for (int gid = 0; gid < code_l_; gid++) {
                  const int *p = find(begin(Placement::kLrcDataGroup[gid]), end(Placement::kLrcDataGroup[gid]), idx);
                  if (p != end(Placement::kLrcDataGroup[gid])) {
                    stripe_failed_disks_num[gid] ++;
                    break;
                  }
                }
              }
            }
          } else {
            // *iter_disk is alive
            // check local parity
            for (int gid = 0; gid < code_l_; gid++) {
              if (idx == Placement::kLrcLocalParity[gid] && stripe_failed_disks_num[gid] > 0){
                stripe_failed_disks_num[gid] --;
                break;
              }
            }
            if ((int)(*iter_disk / (nodes_per_rack_ * disks_per_node_)) == rack_id) {
              num_alive_chunk_same_rack ++;
              alive_chunk_same_rack.push_back(idx);
            }
          }
          idx ++;
        }
      }
      if (num_failed_chunk == 1)  // single chunk repair
        num_stripes_repaired_single_chunk_ ++;
      else {
        // Check correlated failures
        if (strcmp(code_type_.c_str(), Placement::kCodeTypeLRC.c_str()) == 0) {
          int sum = global_failed_disks_num;
          for (int gid = 0; gid < code_l_; gid++) {
            sum += stripe_failed_disks_num[gid];
          }
          if (sum > code_n_ - code_k_ - code_l_) {
            cout << "data loss" << endl;
            return;
          }
        } else {
          if (num_failed_chunk > code_n_ - code_k_) {
            cout << "data loss" << endl;
            return;
          }
        }
      }

      num_stripes_repaired_actual ++;
      cross_rack_download += ComputeRepairTrafficForStripe(num_failed_chunk, 
          num_alive_chunk_same_rack, alive_chunk_same_rack, fail_idx);
    } // end of looping stripe
    num_stripes_repaired_ += num_stripes_repaired_actual;
    // Not consider cross rack upload for eager repair with multiple bad chunks since other disks may be still failed.
    double repair_bwth = network_.GetAvailCrossRackRepairBwth();
    network_.UpdateAvailCrossRackRepairBwth(0.0);
    double repair_time = cross_rack_download * chunk_size_ / repair_bwth / 3600.0; // hours
    Event e = {repair_time + curr_time, Disk::kEventDiskRepair, disk_idx, repair_bwth};
    events_queue_.push(e);
  }
}

int Simulation::CheckStripeDisksToRepair(int stripe_id) {
  map<int, vector<int> >::iterator it_key;
  it_key = stripe_disks_to_repair_.find(stripe_id);
  if (it_key != stripe_disks_to_repair_.end()) 
    return 1;
  return 0;
}

//overloaded function
int Simulation::CheckStripeDisksToRepair(int stripe_id, int disk_id) {
  map<int, vector<int> >::iterator it_key;
  it_key = stripe_disks_to_repair_.find(stripe_id);
  if (it_key != stripe_disks_to_repair_.end()) {
    vector<int>::iterator it_value = find(it_key->second.begin(), it_key->second.end(), disk_id);
    if (it_value != it_key->second.end()) {
      return 2; // Both key and value are found.
    } else {
      return 1; // Only key is found;
    }
  }
  return 0; // None is found
}

void Simulation::SetDiskLazyRepair(int disk_idx, double curr_time){
  int rack_id = (int) (disk_idx / (nodes_per_rack_ * disks_per_node_));
  if (network_.GetAvailCrossRackRepairBwth() == 0) {
    Event e = {curr_time, "", disk_idx, 0};
    wait_repair_queue_.push(e);
  } else { // available cross rack repair bwth > 0
    double cross_rack_download = 0;
    vector<int> stripes_to_repair = placement_.GetStripesToRepair(disk_idx);
    int num_stripes_repaired_actual = 0;
    // record disks and their attached stripes that will be repaired following disk_idx
    map<int, vector<int> > map_disk_stripes_in_repair; 

    vector<int>::iterator iter_stripe;
    for (iter_stripe = stripes_to_repair.begin(); 
        iter_stripe < stripes_to_repair.end(); iter_stripe++) {
      int num_failed_chunk = 0;
      int num_alive_chunk_same_rack = 0;
      int idx = 0;
      int fail_idx = 0;
      int global_failed_disks_num = 0;
      int stripe_failed_disks_num[2] = {0};
      vector<int> alive_chunk_same_rack;
      vector<int> disks_to_repair; // find the disks that need to repaired in one stripe

      vector<int> disks_attached = placement_.GetStripeLocation(*iter_stripe);
      vector<int>::iterator iter_disk;
      for (iter_disk = disks_attached.begin(); 
          iter_disk < disks_attached.end(); iter_disk++) {
        // RS, DRC, replication
        if (strcmp(code_type_.c_str(), Placement::kCodeTypeLRC.c_str()) != 0) {
          if (strcmp(disks_[*iter_disk].GetCurrState().c_str(), Disk::kStateCrashed.c_str()) == 0 ||
              CheckStripeDisksToRepair(*iter_stripe, *iter_disk) == 2) {
            num_failed_chunk ++;
            disks_to_repair.push_back(*iter_disk);
          } else {
            if ((int)(*iter_disk / (nodes_per_rack_ * disks_per_node_)) == rack_id) {
              num_alive_chunk_same_rack ++;
            }
          }
        } else { // LRC
          if (strcmp(disks_[*iter_disk].GetCurrState().c_str(), Disk::kStateCrashed.c_str()) == 0 ||
              CheckStripeDisksToRepair(*iter_stripe, *iter_disk) == 2) { // *iter_disk is failed
            num_failed_chunk ++;
            disks_to_repair.push_back(*iter_disk);
            if (disk_idx == *iter_disk) fail_idx = idx;
            // check position of failed disk
            if (idx == Placement::kLrcGlobalParity[0] || idx == Placement::kLrcGlobalParity[1]) {
              // global parity
              global_failed_disks_num ++;
            } else {
              if (idx != Placement::kLrcLocalParity[0] && idx != Placement::kLrcLocalParity[1]) {
                // data group
                for (int gid = 0; gid < code_l_; gid++) {
                  const int *p = find(begin(Placement::kLrcDataGroup[gid]), end(Placement::kLrcDataGroup[gid]), idx);
                  if (p != end(Placement::kLrcDataGroup[gid])) {
                    stripe_failed_disks_num[gid] ++;
                    break;
                  }
                }
              }
            }
          } else {
            // *iter_disk is alive
            // check local parity
            for (int gid = 0; gid < code_l_; gid++) {
              if (idx == Placement::kLrcLocalParity[gid] && stripe_failed_disks_num[gid] > 0){
                stripe_failed_disks_num[gid] --;
                break;
              }
            }
            if ((int)(*iter_disk / (nodes_per_rack_ * disks_per_node_)) == rack_id) {
              num_alive_chunk_same_rack ++;
              alive_chunk_same_rack.push_back(idx);
            }
          }
          idx ++;
        }
      } // end of looping disks on this stripe
      // Check tolerable limit
      if (strcmp(code_type_.c_str(), Placement::kCodeTypeLRC.c_str()) == 0) {
        int sum = global_failed_disks_num;
        for (int gid = 0; gid < code_l_; gid++) {
          sum += stripe_failed_disks_num[gid];
        }
        if (sum > code_n_ - code_k_ - code_l_) {
          cout << "data loss" << endl;
          if (CheckStripeDisksToRepair(*iter_stripe) == 1) {
            //cout << "Current disk_idx = " << disk_idx << ", current stripe = " << *iter_stripe << endl;
            for (iter_disk = disks_to_repair.begin(); iter_disk < disks_to_repair.end(); iter_disk++) {
              if (find(stripe_disks_to_repair_[*iter_stripe].begin(), 
                    stripe_disks_to_repair_[*iter_stripe].end(), *iter_disk) == 
                  stripe_disks_to_repair_[*iter_stripe].end()) {
                // this disk is not in stripe_disks_to_repair_[*iter_stripe]
                stripe_disks_to_repair_[*iter_stripe].push_back(*iter_disk);
              }
            }
          } else {
            stripe_disks_to_repair_[*iter_stripe] = disks_to_repair;
          }
          return;
        }
      } else {
        if (num_failed_chunk > code_n_ - code_k_) {
          cout << "data loss" << endl;
          if (CheckStripeDisksToRepair(*iter_stripe) == 1) {
            //cout << "Current disk_idx = " << disk_idx << ", current stripe = " << *iter_stripe << endl;
            for (iter_disk = disks_to_repair.begin(); iter_disk < disks_to_repair.end(); iter_disk++) {
              if (find(stripe_disks_to_repair_[*iter_stripe].begin(), 
                    stripe_disks_to_repair_[*iter_stripe].end(), *iter_disk) == 
                  stripe_disks_to_repair_[*iter_stripe].end()) {
                // this disk is not in stripe_disks_to_repair_[*iter_stripe]
                stripe_disks_to_repair_[*iter_stripe].push_back(*iter_disk);
              }
            }
          } else {
            stripe_disks_to_repair_[*iter_stripe] = disks_to_repair;
          }
          return;
        }
      }
      if (num_failed_chunk < lazy_repair_threshold_) {
        //cout << "num_failed_chunk = " << num_failed_chunk << ", lazy_repair_threshold = " << lazy_repair_threshold_ << endl;
        if (num_failed_chunk == 0) {
          if (CheckStripeDisksToRepair(*iter_stripe) == 1) {
            stripe_disks_to_repair_.erase(*iter_stripe);
          }
        } else {
          if (CheckStripeDisksToRepair(*iter_stripe) == 1) {
            //cout << "Current disk_idx = " << disk_idx << ", current stripe = " << *iter_stripe << endl;
            for (iter_disk = disks_to_repair.begin(); iter_disk < disks_to_repair.end(); iter_disk++) {
              if (find(stripe_disks_to_repair_[*iter_stripe].begin(), 
                    stripe_disks_to_repair_[*iter_stripe].end(), *iter_disk) == 
                  stripe_disks_to_repair_[*iter_stripe].end()) {
                // this disk is not in stripe_disks_to_repair_[*iter_stripe]
                stripe_disks_to_repair_[*iter_stripe].push_back(*iter_disk);
              }
            }
          } else {
            stripe_disks_to_repair_[*iter_stripe] = disks_to_repair;
          }
        }
        continue;
      } else {
        // num_failed_disks >= lazy_repair_threshold;
        // repair stripe_id via repairing disk_idx
        // multiple stripes may be repaired via disk_idx

        // need to check data loss here!!!!
        //cout << "num_failed_chunk = " << num_failed_chunk << ", lazy_repair_threshold = " << lazy_repair_threshold_ << endl;
        for (iter_disk = disks_to_repair.begin(); iter_disk < disks_to_repair.end(); iter_disk++) {
          // avoid adding disk_idx into map_disk_stripes_in_repair
          if (*iter_disk != disk_idx) {
            map_disk_stripes_in_repair[*iter_disk].push_back(*iter_stripe);
          }
        }
        // it is okay to use erase even if *iter_stripe is not in the map
        stripe_disks_to_repair_.erase(*iter_stripe);
      }

      // when this stripe will be repair
      num_stripes_repaired_actual ++;
      cross_rack_download += ComputeRepairTrafficForStripe(num_failed_chunk, 
          num_alive_chunk_same_rack, alive_chunk_same_rack, fail_idx);
    } // end of looping stripe
    num_stripes_repaired_ += num_stripes_repaired_actual;
    // Not consider cross rack upload for eager repair with multiple bad chunks since other disks may be still failed.
    if (cross_rack_download > 0) {
      double repair_bwth = network_.GetAvailCrossRackRepairBwth();
      //cout << "set disk repair, bwth = " << repair_bwth << endl;
      network_.UpdateAvailCrossRackRepairBwth(0.0);
      double repair_time = cross_rack_download * chunk_size_ / repair_bwth / 3600.0; // hours
      Event e = {repair_time + curr_time, Disk::kEventDiskRepair, disk_idx, repair_bwth};
      events_queue_.push(e);
      disk_stripes_in_repair_[disk_idx] = map_disk_stripes_in_repair;
    }
  }
}

void Simulation::SetDiskRepairFollowed(int disk_idx, double curr_time) {
  int rack_id = (int) (disk_idx / (nodes_per_rack_ * disks_per_node_));
  if (network_.GetAvailCrossRackRepairBwth() == 0) {
    Event e = {curr_time, "", disk_idx, 0};
    wait_repair_queue_.push(e);
  } else { // available cross rack repair bwth > 0
    double repair_bwth = network_.GetAvailCrossRackRepairBwth();
    network_.UpdateAvailCrossRackRepairBwth(0.0);
    double cross_rack_upload = 0;
    map<int, vector<int> > map_disk_stripes = disk_stripes_in_repair_[disk_idx];
    for (map<int, vector<int> >::iterator it = map_disk_stripes.begin(); it != map_disk_stripes.end(); it++) {
      cross_rack_upload += (it->second.size());
    }
    double repair_time = cross_rack_upload * chunk_size_ / repair_bwth / 3600.0; // hours
    for (map<int, vector<int> >::iterator it = map_disk_stripes.begin(); it != map_disk_stripes.end(); it ++) {
      if (disks_[it->first].GetCurrState() != Disk::kStateNormal) {
        Event e = {repair_time + curr_time, Disk::kEventDiskRepair, it->first, 
          (double)it->second.size() / cross_rack_upload * repair_bwth};
        events_queue_.push(e);
      } else {
        Event e = {repair_time + curr_time, Disk::kEventChunkRepair, it->first, 
          (double)it->second.size() / cross_rack_upload * repair_bwth};
        events_queue_.push(e);
      }
    }
    disk_stripes_in_repair_.erase(disk_idx);
  }
}

double Simulation::ComputeRepairTrafficForStripe(int num_failed_chunk, 
    int num_alive_chunk_same_rack, vector<int> alive_chunk_same_rack, int fail_idx) {
  double cross_rack_download = 0;
  // Replication, RS
  if (strcmp(code_type_.c_str(), Placement::kCodeTypeReplication.c_str()) == 0 ||
      strcmp(code_type_.c_str(), Placement::kCodeTypeRS.c_str()) == 0) {
    if (num_alive_chunk_same_rack < code_k_) {
      cross_rack_download += (code_k_ - num_alive_chunk_same_rack);
    }
  } else { // LRC
    if (strcmp(code_type_.c_str(), Placement::kCodeTypeLRC.c_str()) == 0) {
      if (num_failed_chunk == 1) {
        // global parity
        if (fail_idx == Placement::kLrcGlobalParity[0] ||
            fail_idx == Placement::kLrcGlobalParity[1]) { 
          cross_rack_download += (code_k_ - num_alive_chunk_same_rack);
        } else { // data chunk or local parity
          // find which group that the failed chunk is in
          int fail_gid = 0;
          for (int gid = 0; gid < code_l_; gid++) {
            const int *p = find(begin(Placement::kLrcDataGroup[gid]), 
                end(Placement::kLrcDataGroup[gid]), fail_idx);
            if (p != end(Placement::kLrcDataGroup[gid])) { // data chunk
              fail_gid = gid;
              break;
            } else { // local parity
              if (fail_idx == Placement::kLrcLocalParity[gid]) {
                fail_gid = gid;
                break;
              }
            }
          }
          // find how many chunks in the same group can be used for repair
          int num_alive_chunk_same_group = 0;
          vector<int>::iterator iter_chunk;
          for (iter_chunk = alive_chunk_same_rack.begin(); 
              iter_chunk < alive_chunk_same_rack.end(); iter_chunk++) {
            const int *p = find(begin(Placement::kLrcDataGroup[fail_gid]), 
                end(Placement::kLrcDataGroup[fail_gid]), *iter_chunk);
            if (p != Placement::kLrcDataGroup[fail_gid]) {
              num_alive_chunk_same_group ++;
            } else {
              if (*iter_chunk == Placement::kLrcLocalParity[fail_gid]) {
                num_alive_chunk_same_group ++;
              }
            }
          }
          if (num_alive_chunk_same_group < code_k_ / code_l_) {
            cross_rack_download += (code_k_ / code_l_ - num_alive_chunk_same_group);
          }
        }
      } else { // num_failed_chunk > 1
        if (num_alive_chunk_same_rack < code_k_) {
          cross_rack_download += (code_k_ - num_alive_chunk_same_rack);
        }
      }
    } else { // DRC
      if (strcmp(code_type_.c_str(), Placement::kCodeTypeDRC.c_str()) == 0) {
        if (num_failed_chunk == 1) {
          if (code_k_ == 5 && code_n_ == 9) {
            cross_rack_download += 1.0;
          } else {
            if (code_k_ == 6 && code_n_ == 9)
              cross_rack_download += 2.0;
            else {
              cout << "Only support DRC - (9,6,3) and (9,5,3)" << endl;
            }
          }
        } else {
          if (num_alive_chunk_same_rack < code_k_) {
            cross_rack_download += (code_k_ - num_alive_chunk_same_rack);
          }
        }
      } else {
        cout << "Wrong code type in SetDiskRepair()!" << endl;
      }
    }
  }
  return cross_rack_download;
}

bool Simulation::GetNextEvent(double curr_time, double *curr_event_time,
    string *curr_event_type, vector<int> *device_idx_set) {
  if (!wait_repair_queue_.empty()) {
    Event e = wait_repair_queue_.top();
    int disk_id = e.element_id;
    int rack_id = (int)(disk_id / nodes_per_rack_ / disks_per_node_);
    if (use_network_ && (network_.GetAvailCrossRackRepairBwth() != 0) &&
        (network_.GetAvailIntraRackRepairBwth(rack_id) != 0)) {
      wait_repair_queue_.pop();
      if (lazy_repair_) {
        SetDiskLazyRepair(disk_id, curr_time);
      } else {
        SetDiskRepair(disk_id, curr_time);
      }
    }
  }
  if (events_queue_.empty()) return false;
  Event event;
  event = events_queue_.top();
  events_queue_.pop();
  if (event.event_time > mission_time_) return false;

  *curr_event_time = event.event_time;
  *curr_event_type = event.event_type;
  device_idx_set->push_back(event.element_id);
  //printf("event_time = %lf, event_type = %s\n", *curr_event_time, (*curr_event_type).c_str());
  // If use network bandwidth to calculate repair time
  vector<double> repair_bwth_set;
  if (use_network_ && ((strcmp(event.event_type.c_str(), Disk::kEventDiskRepair.c_str()) == 0) ||
        (strcmp(event.event_type.c_str(), Disk::kEventChunkRepair.c_str()) == 0))) {
    //cout << "repair bwth = " << event.repair_bwth << endl;
    repair_bwth_set.push_back(event.repair_bwth);
  }

  Event next_event = events_queue_.top();
  // Gather the events with the same occurring time and event type
  while ((!events_queue_.empty()) && next_event.event_time == event.event_time &&
      strcmp(next_event.event_type.c_str(), event.event_type.c_str()) == 0) {
    events_queue_.pop();
    device_idx_set->push_back(next_event.element_id);
    if (use_network_ && ((strcmp(next_event.event_type.c_str(), Disk::kEventDiskRepair.c_str()) == 0) ||
        (strcmp(next_event.event_type.c_str(), Disk::kEventChunkRepair.c_str()) == 0))) {
      repair_bwth_set.push_back(next_event.repair_bwth);
    }
    next_event = events_queue_.top();
  }
  if (strcmp(curr_event_type->c_str(), Disk::kEventDiskFail.c_str()) == 0) {
    double fail_time = *curr_event_time;
    vector<int>::iterator iter_disk;
    for (iter_disk = device_idx_set->begin(); iter_disk < device_idx_set->end(); iter_disk++) {
      if (strcmp(disks_[*iter_disk].GetCurrState().c_str(), Disk::kStateCrashed.c_str()) != 0) {
        // first mark the disks as failed 
        disks_[*iter_disk].FailDisk(fail_time);
      }
    }
    for (iter_disk = device_idx_set->begin(); iter_disk < device_idx_set->end(); iter_disk++) {
      if (!lazy_repair_) {
        SetDiskRepair(*iter_disk, fail_time);
      } else {
          SetDiskLazyRepair(*iter_disk, fail_time);
      }
    }
    return true;
  } else {
    // repair for disk failure
    if (strcmp(curr_event_type->c_str(), Disk::kEventDiskRepair.c_str()) == 0) {
      double repair_time = *curr_event_time;
      vector<int>::iterator iter_disk;
      for (iter_disk = device_idx_set->begin(); iter_disk < device_idx_set->end(); iter_disk++) {
        if (strcmp(disks_[*iter_disk].GetCurrState().c_str(), Disk::kStateCrashed.c_str()) == 0) {
          disks_[*iter_disk].RepairDisk(repair_time);
          if (!use_failure_trace_) {
            SetDiskFail(*iter_disk, repair_time);
          }
        }
      }
      // update the network status
      if (use_network_) {
        vector<double>::iterator iter_bwth;
        for (iter_bwth = repair_bwth_set.begin(); iter_bwth < repair_bwth_set.end(); iter_bwth++) {
          network_.UpdateAvailCrossRackRepairBwth(
              network_.GetAvailCrossRackRepairBwth() + (*iter_bwth));
        }
        if (lazy_repair_) {
          for (iter_disk = device_idx_set->begin(); iter_disk < device_idx_set->end(); iter_disk++) {
            if (disk_stripes_in_repair_.find(*iter_disk) != disk_stripes_in_repair_.end()) {
              SetDiskRepairFollowed(*iter_disk, repair_time);
            }
          }
        }
      }
      return true;
    } else {
      if (strcmp(curr_event_type->c_str(), Disk::kEventChunkRepair.c_str()) == 0) {
        double repair_time = *curr_event_time;
        vector<double>::iterator iter_bwth;
        for (iter_bwth = repair_bwth_set.begin(); iter_bwth < repair_bwth_set.end(); iter_bwth++) {
          network_.UpdateAvailCrossRackRepairBwth(
              network_.GetAvailCrossRackRepairBwth() + (*iter_bwth));
        }
        return true;
      } else {
        if (strcmp(curr_event_type->c_str(), Disk::kEventDiskReplacement.c_str()) == 0) {
          double replacement_time = *curr_event_time;
          vector<int>::iterator iter_disk;
          for (iter_disk = device_idx_set->begin(); iter_disk < device_idx_set->end(); iter_disk++) {
            if (strcmp(disks_[*iter_disk].GetCurrState().c_str(), Disk::kStateNormal.c_str()) != 0) {
              if (lazy_repair_) {
                SetDiskRepair(*iter_disk, replacement_time);
              } else {
                SetDiskLazyRepair(*iter_disk, replacement_time);
              }
            }
          }
          return true;
        } else {
          cout << "Wrong event type of in GetNextEvent()!" << endl;
        }
      }
    }
  }
  return false;
}

unsigned int Simulation::RunIteration(int *num_failed_stripes, int *num_lost_chunks) {
  double curr_time = 0;
  int num_failure_events = 0;
  int num_repair_events = 0;

  while (true) {
    double event_time;
    string event_type;
    vector<int> disk_id_set;
    if (!GetNextEvent(curr_time, &event_time, &event_type, &disk_id_set)) {
      break;
    }
    curr_time = event_time;
    if (curr_time > mission_time_) break;
    
    if (strcmp(event_type.c_str(), Disk::kEventDiskFail.c_str()) == 0) {
      num_failure_events ++;
    } else {
      if (strcmp(event_type.c_str(), Disk::kEventDiskRepair.c_str()) == 0) {
        num_repair_events ++;
      }
    }
    if (!state_.UpdateState(event_type, disk_id_set)) {
      cout << "Update state failed!" << endl;
    }
    if (strcmp(event_type.c_str(), Disk::kEventDiskFail.c_str()) == 0) {
      if (lazy_repair_) {
        bool data_loss = placement_.CheckDataLoss(stripe_disks_to_repair_, num_failed_stripes,
            num_lost_chunks);
        if (data_loss) {
          cout << "num_failure events = " << num_failure_events << ", num_repair_events = " << num_repair_events << endl;
          return 1;
        }
      } else {
        vector<int> failed_disks = state_.GetFailedDisks();
        bool data_loss = placement_.CheckDataLoss(failed_disks, num_failed_stripes, num_lost_chunks);
        if (data_loss) {
          cout << "num_failure events = " << num_failure_events << ", num_repair_events = " << num_repair_events << endl;
          return 1;
        }
      }
    }
  }
  cout << "num_failure events = " << num_failure_events << ", num_repair_events = " << num_repair_events << endl;
  return 0;
}

void Simulation::Run(unsigned int *data_loss, unsigned long *tot_num_failed_stripes,
    unsigned long *tot_num_lost_chunks) {
  *data_loss = 0;
  *tot_num_failed_stripes = 0;
  *tot_num_lost_chunks = 0;

  for (int iter = 0; iter < num_iterations_; iter++) {
    int num_failed_stripes, num_lost_chunks;
    Reset();
    *data_loss += RunIteration(&num_failed_stripes, &num_lost_chunks);
    *tot_num_failed_stripes += num_failed_stripes;
    *tot_num_lost_chunks += num_lost_chunks;
  }
}
