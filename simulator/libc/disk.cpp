#include "disk.hpp"

const string Disk::kStateNormal = "state normal";
const string Disk::kStateCrashed = "state crashed";
const string Disk::kEventDiskFail = "disk failure";
const string Disk::kEventDiskRepair = "disk repair";
const string Disk::kEventDiskReplacement = "disk replacement";
const string Disk::kEventChunkRepair = "chunk repair";

Disk::Disk(weibull_distribution<double> disk_fail_distr)
  :state_(kStateNormal), disk_fail_distr_(disk_fail_distr), 
   last_time_update_(0.0),
   begin_time_(0.0), clock_(0.0), unavail_start_(0.0), 
   unavail_clock_(0.0), repair_start_(0.0), repair_clock_(0.0){
}

Disk::Disk()
  :state_(kStateNormal), last_time_update_(0.0),
   begin_time_(0.0), clock_(0.0), unavail_start_(0.0), 
   unavail_clock_(0.0), repair_start_(0.0), repair_clock_(0.0){
}

void Disk::InitClock(double curr_time) {
  begin_time_ = curr_time;
  last_time_update_ = curr_time;
  clock_ = 0.0;
  unavail_start_ = 0.0;
  unavail_clock_ = 0.0;
  repair_start_ = 0.0;
  repair_clock_ = 0.0;
}

void Disk::InitState() {
  state_ = kStateNormal;
}

void Disk::UpdateClock(double curr_time) {
  clock_ += (curr_time - last_time_update_);
  if (strcmp(state_.c_str(), kStateCrashed.c_str()) == 0) {
    repair_clock_ += (curr_time - repair_start_);
  } else 
    repair_clock_ = 0.0;
  last_time_update_ = curr_time;
}

string Disk::GetCurrState() {
  return state_;
}

void Disk::FailDisk(double curr_time) {
  if (strcmp(state_.c_str(), kStateNormal.c_str()) == 0) {
    unavail_start_ = curr_time;
  }
  state_ = kStateCrashed;
  repair_clock_ = 0.0;
  repair_start_ = curr_time;
}

void Disk::RepairDisk(double curr_time) {
  state_ = kStateNormal;
  unavail_clock_ += (curr_time - unavail_start_);
  begin_time_ = last_time_update_;
  clock_ = 0.0;
  repair_clock_ = 0.0;
}

double Disk::GetUnavailTime(double curr_time) {
  if (strcmp(state_.c_str(), kStateNormal.c_str()) == 0) {
    return unavail_clock_;
  } else {
    return unavail_clock_ + (curr_time - unavail_start_);
  }
}
