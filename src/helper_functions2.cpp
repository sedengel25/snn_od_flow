#include <RcppParallel.h>
#include <Rcpp.h>
#include <unordered_map>
#include <utility>

using namespace Rcpp;
using namespace RcppParallel;

// Custom hash function for std::pair to use in unordered_map
struct pair_hash {
	template <class T1, class T2>
	std::size_t operator () (const std::pair<T1,T2> &pair) const {
		auto hash1 = std::hash<T1>{}(pair.first);
		auto hash2 = std::hash<T2>{}(pair.second);
		return hash1 ^ hash2;
	}
};

// Funktion zum Erstellen der Netzwerk-Map
std::unordered_map<int, std::pair<int, int>> create_network_map(const DataFrame &network) {
	IntegerVector ids = network["id"];
	IntegerVector sources = network["source"];
	IntegerVector targets = network["target"];
	std::unordered_map<int, std::pair<int, int>> net_map;
	for (int i = 0; i < ids.size(); i++) {
		net_map[ids[i]] = std::make_pair(sources[i], targets[i]);
	}
  return net_map;
}

// Funktion zum Erstellen der Distanz-Map
std::unordered_map<std::pair<int, int>, int, pair_hash> create_dist_map(const DataFrame &dist_mat) {
	IntegerVector sources = dist_mat["source"];
	IntegerVector targets = dist_mat["target"];
	IntegerVector distances = dist_mat["m"];

	std::unordered_map<std::pair<int, int>, int, pair_hash> dist_map;
	for (int i = 0; i < sources.size(); i++) {
		dist_map[{std::min(sources[i], targets[i]), std::max(sources[i], targets[i])}] = distances[i];
	}
	return dist_map;
}

// Worker class to parallel process networks
struct ProcessNetworksWorker : public Worker {
	// Inputs
	const RVector<int> sub_ids;
	const RVector<int> sub_line_ids;
	const RVector<int> sub_starts;
	const RVector<int> sub_ends;
	const RVector<int> full_ids;
	const RVector<int> full_line_ids;
	const RVector<int> full_starts;
	const RVector<int> full_ends;
	const std::unordered_map<int, std::pair<int, int>>& network_map;
	const std::unordered_map<std::pair<int, int>, int, pair_hash>& dist_map;
	
	// Outputs
	RVector<int> from_points;
	RVector<int> to_points;
	RVector<int> distances;
	
	// Constructor
	ProcessNetworksWorker(const IntegerVector sub_ids, 
                       const IntegerVector sub_line_ids,
                       const IntegerVector sub_starts, 
                       const IntegerVector sub_ends,
                       const IntegerVector full_ids, 
                       const IntegerVector full_line_ids,
                       const IntegerVector full_starts, 
                       const IntegerVector full_ends,
                       const std::unordered_map<int, 
                                                std::pair<int, int>>& network_map,
                       const std::unordered_map<std::pair<int, int>, 
                                                int, pair_hash>& dist_map,
                       IntegerVector from_points, 
                       IntegerVector to_points, 
                       IntegerVector distances)
		: sub_ids(sub_ids), 
    sub_line_ids(sub_line_ids), 
    sub_starts(sub_starts), 
    sub_ends(sub_ends),
    full_ids(full_ids), 
    full_line_ids(full_line_ids), 
    full_starts(full_starts), 
    full_ends(full_ends),
    network_map(network_map), 
    dist_map(dist_map), 
    from_points(from_points), 
    to_points(to_points), 
    distances(distances) {}
	
	// Parallel operator
	void operator()(std::size_t begin, std::size_t end) {
		for (size_t i = begin; i < end; ++i) {
			int point_ij = sub_ids[i];
			int line_ij = sub_line_ids[i];
			int start_ij = sub_starts[i];
			int end_ij = sub_ends[i];
			//std::cout << "Processing sub point: " << point_ij << ", line: " << line_ij << std::endl;
			for (size_t j = 0; j < full_ids.length(); ++j) {
				int point_kl = full_ids[j];
				int line_kl = full_line_ids[j];
				int start_kl = full_starts[j];
				int end_kl = full_ends[j];
				//std::cout << "Comparing with full point: " << point_kl << ", line: " << line_kl << std::endl;
				
				if (point_ij == point_kl) continue;
				
				if (network_map.find(line_ij) != network_map.end() && network_map.find(line_kl) != network_map.end()) {
					int source_ij = network_map.at(line_ij).first;
					int target_ij = network_map.at(line_ij).second;
					int source_kl = network_map.at(line_kl).first;
					int target_kl = network_map.at(line_kl).second;
					
					int dist_ik = dist_map.count({source_ij, source_kl}) ? dist_map.at({source_ij, source_kl}) : std::numeric_limits<int>::infinity();
					int dist_jl = dist_map.count({target_ij, target_kl}) ? dist_map.at({target_ij, target_kl}) : std::numeric_limits<int>::infinity();
					int dist_il = dist_map.count({source_ij, target_kl}) ? dist_map.at({source_ij, target_kl}) : std::numeric_limits<int>::infinity();
					int dist_jk = dist_map.count({source_kl, target_ij}) ? dist_map.at({source_kl, target_ij}) : std::numeric_limits<int>::infinity();
					
					int total_distance = std::min({start_ij + start_kl + dist_ik,
                                      end_ij + end_kl + dist_jl,
                                      start_ij + end_kl + dist_il,
                                      end_ij + start_kl + dist_jk});
					//std::cout << "Distance calculated: " << total_distance << " for points " << point_ij << " to " << point_kl << std::endl;
					from_points[i] = point_ij;
					to_points[i] = point_kl;
					distances[i] = total_distance;
				}
			}
		}
	}
};

// [[Rcpp::export]]
void checkInput(DataFrame df) {
	IntegerVector ids = df["id_new"];
	Rcout << "Erste 10 'line_id' Werte: ";
	for (int i = 0; i < 10; ++i) {
		Rcout << ids[i] << " ";
	}
	Rcout << std::endl;
	IntegerVector line_ids = df["id_edge"];
	Rcout << "Erste 10 'line_id' Werte: ";
	for (int i = 0; i < 10; ++i) {
		Rcout << line_ids[i] << " ";
	}
	Rcout << std::endl;
	IntegerVector starts = df["dist_to_start"];
	Rcout << "Erste 10 'line_id' Werte: ";
	for (int i = 0; i < 10; ++i) {
		Rcout << starts[i] << " ";
	}
	Rcout << std::endl;
	IntegerVector ends = df["dist_to_end"];
	Rcout << "Erste 10 'line_id' Werte: ";
	for (int i = 0; i < 10; ++i) {
		Rcout << ends[i] << " ";
	}
	Rcout << std::endl;
}

// [[Rcpp::export]]
DataFrame process_networks_parallel(DataFrame dt_od_pts_sub, 
                                    DataFrame dt_od_pts_full, 
                                    DataFrame dt_network, 
                                    DataFrame dt_dist_mat) {
	// Create maps
	auto network_map = create_network_map(dt_network);
	auto dist_map = create_dist_map(dt_dist_mat);

	// Prepare inputs
	IntegerVector sub_ids = dt_od_pts_sub["id_new"];
	IntegerVector sub_line_ids = dt_od_pts_sub["id_edge"];
	IntegerVector sub_starts = dt_od_pts_sub["dist_to_start"];
	IntegerVector sub_ends = dt_od_pts_sub["dist_to_end"];
	IntegerVector full_ids = dt_od_pts_full["id_new"];
	IntegerVector full_line_ids = dt_od_pts_full["id_edge"];
	IntegerVector full_starts = dt_od_pts_full["dist_to_start"];
	IntegerVector full_ends = dt_od_pts_full["dist_to_end"];
	

	
	
	// Prepare outputs
	IntegerVector from_points(sub_ids.size()), to_points(sub_ids.size());
	IntegerVector distances(sub_ids.size());
	
	// Create worker
	ProcessNetworksWorker worker(sub_ids, sub_line_ids, sub_starts, sub_ends,
                              full_ids, full_line_ids, full_starts, full_ends,
                              network_map, dist_map, from_points, to_points, distances);
	
	// Execute in parallel
	parallelFor(0, sub_ids.size(), worker);
	
	// Return results
	return DataFrame::create(Named("from") = from_points,
                          Named("to") = to_points,
                          Named("distance") = distances);
}
