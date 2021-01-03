#include <cstring>
#include <string>
#include <random>
#include <iostream>
using namespace std;

class Disk{
  private:
    weibull_distribution<double> disk_fail_distr_;
    string state_;
    // last time global update
    double last_time_update_;
    // global begin time
    double begin_time_;
    // local (relative) time of this disk
    double clock_;
    double unavail_start_, unavail_clock_;
    double repair_start_, repair_clock_;
  
  public:
    static const string kStateNormal;
    static const string kStateCrashed;

    static const string kEventDiskFail;
    static const string kEventDiskRepair;
    static const string kEventDiskReplacement;
    static const string kEventChunkRepair;

    Disk();
    Disk(weibull_distribution<double> disk_fail_distr);
    void InitClock(double curr_time);
    void InitState();
    void UpdateClock(double curr_time);
    string GetCurrState();
    void FailDisk(double curr_time);
    void RepairDisk(double curr_time);
    double GetUnavailTime(double curr_time);
};
