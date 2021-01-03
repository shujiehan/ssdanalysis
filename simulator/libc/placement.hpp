#include <algorithm>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include <cstdlib>
#include <map>
#include <set>
using namespace std;

class Placement{
  private:
    int num_racks_, nodes_per_rack_, disks_per_node_, capacity_per_disk_;
    int num_disks_, num_chunks_, num_data_chunks_, num_stripes_, chunk_size_;
    int code_n_, code_k_, code_m_;
    string code_type_;

    int code_l_;

    vector<vector<int> > stripes_location_;
    vector<vector<int> > stripes_per_disk_;
    vector<int> num_chunks_per_disk_;
    int disks_per_rack_;
    default_random_engine generator_;

  public:
    static const string kCodeTypeRS;
    static const string kCodeTypeLRC;
    static const string kCodeTypeDRC;
    static const string kCodeTypeReplication;

    static const string  kPlaceTypeFlat; //"Each chunk of a stripe resides in different rack"

    static const int kLrcDataGroup[2][6];
    static const int kLrcLocalParity[2];
    static const int kLrcGlobalParity[2];
    Placement();
    Placement(int num_racks);
    Placement(int num_racks, int nodes_per_rack, int disks_per_node, 
              long capacity_per_disk, long num_stripes, int chunk_size, 
              string code_type, int code_n, int code_k,
              int code_l, default_random_engine generator);
    bool GeneratePlacement();
    vector<int> GetDiffRacks(int num_diff_racks);
    int GetDiskRandomly(int rack_id);
    void GenerateNumChunksPerDisk();
    vector<int> GetStripesToRepair(int failed_disk_id);
    vector<int> GetStripeLocation(int stripe_id);
    bool CheckDataLoss(vector<int> failed_disks_list, int *num_failed_disks_list,
        int *num_lost_chunks);
    bool CheckDataLoss(map<int, vector<int> > stripe_disks_to_repair, 
        int *num_failed_disks_list, int *num_lost_chunks);
};

