#include <iostream>
#include <vector>
using namespace std;

class Network {
  private:
    int num_racks_;
    int nodes_per_rack_;
    int num_nodes_;
    double max_cross_rack_repair_bwth_;
    double max_intra_rack_repair_bwth_;
    double avail_cross_rack_repair_bwth_;
    vector<double> avail_intra_rack_repair_bwth_;

  public:
    Network();
    Network(int num_racks, int nodes_per_rack, double network_setting[]);
    void UpdateAvailCrossRackRepairBwth(double updated_value);
    void UpdateAvailIntraRackRepairBwth(int rack_id, double updated_value);
    double GetAvailCrossRackRepairBwth();
    double GetAvailIntraRackRepairBwth(int rack_id);
};
