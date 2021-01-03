#include "placement.hpp"

const string Placement::kCodeTypeRS = "RSC"; //"Reed-Solomon Codes";
const string Placement::kCodeTypeLRC = "LRC"; //"Locally Repairable Codes";
const string Placement::kCodeTypeDRC = "DRC"; //"Double Regenerating Codes";
const string Placement::kCodeTypeReplication = "Rep"; //"Replication";

const int Placement::kLrcDataGroup[][6] = {{0, 1, 2, 3, 4, 5}, {8, 9, 10, 11, 12, 13}};
const int Placement::kLrcLocalParity[] = {6, 14};
const int Placement::kLrcGlobalParity[] = {7, 15};

Placement::Placement(){}
Placement::Placement(int num_racks) {num_racks_ = num_racks; }
Placement::Placement(int num_racks, int nodes_per_rack, int disks_per_node, 
                     long capacity_per_disk, long num_stripes, int chunk_size, 
                     string code_type, int code_n, int code_k,
                     int code_l, default_random_engine generator)
  :num_racks_(num_racks), nodes_per_rack_(nodes_per_rack), disks_per_node_(disks_per_node),
   capacity_per_disk_(capacity_per_disk), num_stripes_(num_stripes), 
   chunk_size_(chunk_size), code_type_(code_type), code_n_(code_n), code_k_(code_k),
    code_l_(code_l), generator_(generator){
    num_disks_ = num_racks_ * nodes_per_rack_ * disks_per_node_;
    code_m_ = code_n_ - code_k_;
    num_chunks_ = code_n_ * num_stripes_;
    num_data_chunks_ = code_k_ * num_stripes_;
    disks_per_rack_ = disks_per_node_ * nodes_per_rack_;
    
    GeneratePlacement();
    GenerateNumChunksPerDisk();
}

bool Placement::GeneratePlacement(){
  // Check whether code settings are valid.
  if (code_k_ < 1 || code_n_ <= code_k_) {
    cout << "Invalid value of code_n and code_k" << endl;
    return false;
  }
  if (strcmp(code_type_.c_str(), kCodeTypeLRC.c_str()) == 0 && code_l_ == 0) {
    cout << "code_l should NOT be 0 for LRC!" << endl;
    return false;
  }
  // Generate flat placement: "Each chunk of a stripe resides in different rack"
  if (num_racks_ < code_n_ || disks_per_rack_ < 1) 
    return false;
  for (int stripe_id = 0; stripe_id < num_stripes_; stripe_id++) {
    vector<int> rack_list = GetDiffRacks(code_n_);
    vector<int> disk_list;
    vector<int>::iterator iter_rack;
    for (iter_rack = rack_list.begin(); iter_rack < rack_list.end(); iter_rack++) {
      disk_list.push_back(GetDiskRandomly(*iter_rack));
    }
    stripes_location_.push_back(disk_list);
  }
  return true;
}

// Randomly choose a disk from a rack with rack_id
int Placement::GetDiskRandomly(int rack_id) {
  int min_disk = rack_id * nodes_per_rack_ * disks_per_node_;
  int disk_id = rand() % disks_per_rack_ + min_disk;
  return disk_id;
}

vector<int> Placement::GetDiffRacks(int num_diff_racks){
  if (num_racks_ < num_diff_racks)
    cout << "Wrong num_diff_racks in GetDiffRacks()!" << endl;
  vector<int> diff_racks;
  bool check;
  if (num_diff_racks * 2.0 < num_racks_) {
    diff_racks.resize(num_diff_racks, 0);
    for (int i = 0; i < num_diff_racks; i++) {
      check = false;
      do {
        diff_racks[i] = rand() % (num_racks_ - 1);
        check = true;
        for (int j = 0; (check) && j < i; j++) {
          check = (diff_racks[i] != diff_racks[j]);
        }
      }while(check == false);
    }
  } else {
    // optimize: num_racks_ too few
    for (int idx = 0; idx < num_racks_; idx++) {
      diff_racks.push_back(idx);
    }
    int num_racks_removed = num_racks_ - num_diff_racks;
    vector<int>::iterator it;
    for (int i = 0; i < num_racks_removed; i++) {
      check = false;
      do {
        int rack_id_removed = rand() % (num_racks_ - 1);
        it = find(diff_racks.begin(), diff_racks.end(), rack_id_removed);
        if (it != diff_racks.end()) {
          check = true;
          diff_racks.erase(it);
        }
      }while(check == false);
    }
    shuffle(diff_racks.begin(), diff_racks.end(), generator_);
  }
  return diff_racks;
}

void Placement::GenerateNumChunksPerDisk(){
  num_chunks_per_disk_ = vector<int>(num_disks_, 0);
  stripes_per_disk_.resize(num_disks_ + 1);
  for (int stripe_id = 0; stripe_id < num_stripes_; stripe_id++) {
    vector<int>::iterator iter_disk;
    for (iter_disk = stripes_location_[stripe_id].begin(); iter_disk < stripes_location_[stripe_id].end(); iter_disk++) {
      num_chunks_per_disk_[*iter_disk] ++;
      stripes_per_disk_[*iter_disk].push_back(stripe_id);
    }
  }
}

vector<int> Placement::GetStripesToRepair(int failed_disk_id) {
  if (failed_disk_id < 0 || failed_disk_id >= num_disks_)
    cout << "Wrong failed_disk_id in GetStripesToRepair()!" << endl;
  return stripes_per_disk_[failed_disk_id];
}

vector<int> Placement::GetStripeLocation(int stripe_id) {
  if (stripe_id < 0 || stripe_id >= num_stripes_)
    cout << "Wrong stripe_id in GetStripeLocation()!" << endl;
  return stripes_location_[stripe_id];
}

bool Placement::CheckDataLoss(vector<int> failed_disks_list, int *num_failed_stripes,
    int *num_lost_chunks) {
  set<int> stripe_id_set;
  vector<int>::iterator iter_disk;
  // find all stripes that need to repair
  for (iter_disk = failed_disks_list.begin(); iter_disk < failed_disks_list.end(); iter_disk++) {
    vector<int> stripes = GetStripesToRepair(*iter_disk);
    stripe_id_set.insert(stripes.begin(), stripes.end());
  }

  *num_failed_stripes = 0;
  *num_lost_chunks = 0;
  bool data_loss = false;

  if (strcmp(code_type_.c_str(), kCodeTypeLRC.c_str()) == 0) {
    // LRC
    set<int>::iterator iter_stripe;
    for (iter_stripe = stripe_id_set.begin(); iter_stripe != stripe_id_set.end(); iter_stripe++) {
      int cur_stripe_lost_chunks_num = 0;
      int stripe_failed_disks_num[2] = {0};
      int global_failed_disks_num = 0;
      int idx = 0;
      // get stripe location
      vector<int> stripe_disks_id = GetStripeLocation(*iter_stripe);
      for (iter_disk = stripe_disks_id.begin(); iter_disk < stripe_disks_id.end(); iter_disk++) {
        vector<int>::iterator it = find(failed_disks_list.begin(), failed_disks_list.end(), *iter_disk);
        if (it != failed_disks_list.end()) {
          cur_stripe_lost_chunks_num ++;
          if (idx == kLrcGlobalParity[0] || idx == kLrcGlobalParity[1]) {
            // global parity
            global_failed_disks_num ++;
          } else {
            if (idx != kLrcLocalParity[0] && idx != kLrcLocalParity[1]) {
              // data group
              for (int gid = 0; gid < code_l_; gid++) {
                const int *p = find(begin(kLrcDataGroup[gid]), end(kLrcDataGroup[gid]), idx);
                if (p != end(kLrcDataGroup[gid])) {
                  stripe_failed_disks_num[gid] ++;
                  break;
                }
              }
            }
          }
        } else { // *iter_disk is not in failed_disks_list, check local parity
          for (int gid = 0; gid < code_l_; gid++) {
            if (idx == kLrcLocalParity[gid] && stripe_failed_disks_num[gid] > 0){
              stripe_failed_disks_num[gid] --;
              break;
            }
          }
        }
        idx ++;
      }
      int sum = global_failed_disks_num;
      for (int gid = 0; gid < code_l_; gid ++) {
        sum += stripe_failed_disks_num[gid];
      }
      if (sum > code_n_ - code_k_ - code_l_) {
        (*num_failed_stripes) ++;
        *num_lost_chunks += cur_stripe_lost_chunks_num;
        data_loss = true;
      } 
    }
  } else {
    // RS, replication
    set<int>::iterator iter_stripe;
    for (iter_stripe = stripe_id_set.begin(); iter_stripe != stripe_id_set.end(); iter_stripe++) {
      int stripe_failed_disks_num = 0;
      vector<int> stripe_failed_disks;
      vector<int> stripe_disks_id = GetStripeLocation(*iter_stripe);
      for (iter_disk = stripe_disks_id.begin(); iter_disk < stripe_disks_id.end(); iter_disk++) {
        vector<int>::iterator it = find(failed_disks_list.begin(), failed_disks_list.end(), *iter_disk);
        if (it != failed_disks_list.end()) {
          stripe_failed_disks_num ++;
          stripe_failed_disks.push_back(*iter_disk);
        }
      }
      if (stripe_failed_disks_num > code_m_) {
        cout << "placement === " << stripe_failed_disks_num << endl;
        for (iter_disk = stripe_failed_disks.begin(); iter_disk < stripe_failed_disks.end(); iter_disk++) {
          cout << *iter_disk << " ";
        }
        cout << endl;
        (*num_failed_stripes) ++;
        *num_lost_chunks += stripe_failed_disks_num;
        data_loss = true;
      }
    }
  }
  return data_loss;
}

//overloaded function
bool Placement::CheckDataLoss(map<int, vector<int> > stripe_disks_to_repair, 
    int *num_failed_stripes, int *num_lost_chunks) {

  *num_failed_stripes = 0;
  *num_lost_chunks = 0;
  bool data_loss = false;

  vector<int>::iterator iter_disk;
  if (strcmp(code_type_.c_str(), kCodeTypeLRC.c_str()) == 0) {
    // LRC
    map<int, vector<int> >::iterator iter_stripe;
    for (iter_stripe = stripe_disks_to_repair.begin(); iter_stripe != stripe_disks_to_repair.end(); iter_stripe++) {
      int cur_stripe_lost_chunks_num = 0;
      int stripe_failed_disks_num[2] = {0};
      int global_failed_disks_num = 0;
      int idx = 0;
      // get stripe location
      vector<int> failed_disks_list = iter_stripe->second;
      vector<int> stripe_disks_id = GetStripeLocation(iter_stripe->first);
      for (iter_disk = stripe_disks_id.begin(); iter_disk < stripe_disks_id.end(); iter_disk++) {
        vector<int>::iterator it = find(failed_disks_list.begin(), failed_disks_list.end(), *iter_disk);
        if (it != failed_disks_list.end()) {
          cur_stripe_lost_chunks_num ++;
          if (idx == kLrcGlobalParity[0] || idx == kLrcGlobalParity[1]) {
            // global parity
            global_failed_disks_num ++;
          } else {
            if (idx != kLrcLocalParity[0] && idx != kLrcLocalParity[1]) {
              // data group
              for (int gid = 0; gid < code_l_; gid++) {
                const int *p = find(begin(kLrcDataGroup[gid]), end(kLrcDataGroup[gid]), idx);
                if (p != end(kLrcDataGroup[gid])) {
                  stripe_failed_disks_num[gid] ++;
                  break;
                }
              }
            }
          }
        } else { // *iter_disk is not in failed_disks_list, check local parity
          for (int gid = 0; gid < code_l_; gid++) {
            if (idx == kLrcLocalParity[gid] && stripe_failed_disks_num[gid] > 0){
              stripe_failed_disks_num[gid] --;
              break;
            }
          }
        }
        idx ++;
      }
      int sum = global_failed_disks_num;
      for (int gid = 0; gid < code_l_; gid ++) {
        sum += stripe_failed_disks_num[gid];
      }
      if (sum > code_n_ - code_k_ - code_l_) {
        (*num_failed_stripes) ++;
        *num_lost_chunks += cur_stripe_lost_chunks_num;
        cout << "placement === " << sum << endl;
        data_loss = true;
      }
    }
  } else {
    // RS, replication
    map<int, vector<int> >::iterator iter_stripe;
    for (iter_stripe = stripe_disks_to_repair.begin(); iter_stripe != stripe_disks_to_repair.end(); iter_stripe++) {
      int stripe_failed_disks_num = 0;
      vector<int> stripe_failed_disks; // used to output results
      vector<int> failed_disks_list = iter_stripe->second;
      vector<int> stripe_disks_id = GetStripeLocation(iter_stripe->first);
      for (iter_disk = stripe_disks_id.begin(); iter_disk < stripe_disks_id.end(); iter_disk++) {
        vector<int>::iterator it = find(failed_disks_list.begin(), failed_disks_list.end(), *iter_disk);
        if (it != failed_disks_list.end()) {
          stripe_failed_disks_num ++;
          stripe_failed_disks.push_back(*iter_disk);
        }
      }
      if (stripe_failed_disks_num > code_m_) {
        cout << "placement === " << stripe_failed_disks_num << endl;
        for (iter_disk = stripe_failed_disks.begin(); iter_disk < stripe_failed_disks.end(); iter_disk++) {
          cout << *iter_disk << " ";
        }
        cout << endl;
        (*num_failed_stripes) ++;
        *num_lost_chunks += stripe_failed_disks_num;
        data_loss = true;
      }
    }
  }
  return data_loss;
}
