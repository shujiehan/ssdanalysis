#include "network.hpp"

Network::Network(){}

Network::Network(int num_racks, int nodes_per_rack, double network_setting[])
  :num_racks_(num_racks), nodes_per_rack_(nodes_per_rack) {
    num_nodes_ = num_racks_ * nodes_per_rack_;
    max_cross_rack_repair_bwth_ = network_setting[0];
    max_intra_rack_repair_bwth_ = network_setting[1];
    avail_cross_rack_repair_bwth_ = max_cross_rack_repair_bwth_;
    avail_intra_rack_repair_bwth_ = vector<double>(num_racks_, 
        max_intra_rack_repair_bwth_);
}

void Network::UpdateAvailCrossRackRepairBwth(double updated_value) {
  if (updated_value <= (max_cross_rack_repair_bwth_ + 1e-5) && updated_value >= 0) {
    avail_cross_rack_repair_bwth_ = updated_value;
  } else {
    cout << "Wrong updated value! Updated_value = " << updated_value << endl;
  }
}

void Network::UpdateAvailIntraRackRepairBwth(int rack_id, double updated_value) {
  if (updated_value <= max_cross_rack_repair_bwth_ && updated_value >= 0) {
    avail_intra_rack_repair_bwth_[rack_id] = updated_value;
  } else {
    cout << "Wrong updated value! Updated_value = " << updated_value << endl;
  }
}

double Network::GetAvailCrossRackRepairBwth() {
  return avail_cross_rack_repair_bwth_;
}

double Network::GetAvailIntraRackRepairBwth(int rack_id) {
  return avail_intra_rack_repair_bwth_[rack_id];
}
