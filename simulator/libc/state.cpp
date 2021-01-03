#include "state.hpp"

const string State::kCurrStateOK = "system is operational";
const string State::kCurrStateDegraded = "system has at least one failure";

State::State(){}

State::State(int num_disks)
  :num_disks_(num_disks), num_failed_disks_(0), sys_state_(kCurrStateOK){}

void State::UpdateSysState() {
  if (num_failed_disks_ == 0)
    sys_state_ = kCurrStateOK;
  else
    sys_state_ = kCurrStateDegraded;
}

bool State::UpdateState(string event_type, vector<int> disk_id_set) {
  if (strcmp(event_type.c_str(), Disk::kEventDiskFail.c_str()) == 0) {
    vector<int>::iterator iter_disk;
    for (iter_disk = disk_id_set.begin(); iter_disk < disk_id_set.end(); iter_disk++) {
      FailDisk(*iter_disk);
    }
  } else {
    if (strcmp(event_type.c_str(), Disk::kEventDiskRepair.c_str()) == 0) {
      vector<int>::iterator iter_disk;
      for (iter_disk = disk_id_set.begin(); iter_disk < disk_id_set.end(); iter_disk++) {
        RepairDisk(*iter_disk);
      }
    } else {
      if (strcmp(event_type.c_str(), Disk::kEventDiskReplacement.c_str()) != 0 && 
          strcmp(event_type.c_str(), Disk::kEventChunkRepair.c_str()) != 0) {
        cout << "Wrong event_type in update state!" << endl;
        return false;
      }
    }
  }
  UpdateSysState();
  return true;
}

void State::FailDisk(int disk_id) {
  if (disk_id >= num_disks_ || disk_id < 0) {
    cout << "State - FailDisk(): Wrong disk_id!" << endl;
  }
  bm_failed_disks_.set(disk_id);
  bm_avail_disks_.set(disk_id, 0);
  num_failed_disks_ = bm_failed_disks_.count();
}

void State::RepairDisk(int disk_id) {
  if (disk_id >= num_disks_ || disk_id < 0) {
    cout << "State - FailDisk(): Wrong disk_id!" << endl;
  }
  bm_failed_disks_.set(disk_id, 0);
  bm_avail_disks_.set(disk_id);
  num_failed_disks_ = bm_failed_disks_.count();
}

vector<int> State::GetFailedDisks() {
  vector<int> failed_disks;
  for (int i = 0; i < num_disks_; i++) {
    if (bm_failed_disks_.test(i) == 1) {
      failed_disks.push_back(i);
    }
  }
  return failed_disks;
}

void State::Copy(State state) {
  num_disks_ = state.GetNumDisks();
  num_failed_disks_ = state.GetNumFailedDisks();
  bm_failed_disks_ = bitset<MAX>(state.GetBmFailedDisks());
  bm_avail_disks_ = bitset<MAX>(state.GetBmAvailDisks());
  sys_state_ = state.GetSysState();
}

// getters
int State::GetNumDisks() {
  return num_disks_;
}

int State::GetNumFailedDisks() {
  return num_failed_disks_;
}

unsigned long State::GetBmFailedDisks() {
  return bm_failed_disks_.to_ulong();
}

unsigned long State::GetBmAvailDisks() {
  return bm_avail_disks_.to_ulong();
}

string State::GetSysState() {
  return sys_state_;
}

//int main() {
//  State s1 = State(3, 3);
//  vector<int> failed_disks;
//  failed_disks.push_back(0);
//  failed_disks.push_back(2);
//  s1.UpdateState(Disk::kEventDiskFail, failed_disks);
//  State s2 = State(3, 3);
//  s2.Copy(s1);
//  cout << s2.GetBmFailedDisks() << endl;
//  cout << "hello" << endl;
//  return 0;
//}
