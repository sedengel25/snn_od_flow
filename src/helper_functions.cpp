#include <Rcpp.h>
// [[Rcpp::depends(RcppParallel)]]
#include <RcppParallel.h>
#include <map>
#include <mutex>
#include <chrono>
#include <fstream>

using namespace Rcpp;
using namespace RcppParallel;

std::once_flag map_initialized; // Flag für einmalige Initialisierung

// Hilfsfunktion zur Erstellung der Netzwerk-Map
std::map<int, std::pair<int, int>> create_network_map(DataFrame dt_network) {
	IntegerVector ids = dt_network["id"];
	IntegerVector starts = dt_network["source"];
	IntegerVector ends = dt_network["target"];
	std::map<int, std::pair<int, int>> network_map;
	for (int i = 0; i < ids.size(); i++) {
		network_map[ids[i]] = std::make_pair(starts[i], ends[i]);
	}
	return network_map;
}

// Funktion zum Laden der dist_map
void initialize_map(std::map<std::pair<int, int>, int>& dist_map, DataFrame dt_dist_mat) {
	IntegerVector sources = dt_dist_mat["source"];
	IntegerVector targets = dt_dist_mat["target"];
	IntegerVector distances = dt_dist_mat["m"];
	for (int i = 0; i < sources.size(); ++i) {
		int source = sources[i];
		int target = targets[i];
		int distance = distances[i];
		dist_map[std::make_pair(std::min(source, target), std::max(source, target))] = distance;
	}
}

// Funktion zum Abrufen der Distanz
double get_distance(const std::map<std::pair<int, int>, int>& dist_map, int source, int target) {
	auto it = dist_map.find(std::make_pair(std::min(source, target), std::max(source, target)));
	return (it != dist_map.end()) ? it->second : -1.0;
}

// Struktur zur parallelen Verarbeitung von Netzwerken
struct NetworkProcessor : public Worker {
	const IntegerVector& od_pts_full_id;
	const IntegerVector& od_pts_full_line_id;
	const IntegerVector& od_pts_full_start;
	const IntegerVector& od_pts_full_end;
	const std::map<int, std::pair<int, int>>& network_map;
	const std::map<std::pair<int, int>, int>& dist_map;
	
	std::vector<int>& from_points;
	std::vector<int>& to_points;
	std::vector<int>& distances;
	std::mutex& mtx; // Mutex für thread-sicheren Zugriff
	
	NetworkProcessor(const IntegerVector& od_pts_full_id,
                  const IntegerVector& od_pts_full_line_id,
                  const IntegerVector& od_pts_full_start,
                  const IntegerVector& od_pts_full_end,
                  const std::map<int, std::pair<int, int>>& network_map,
                  const std::map<std::pair<int, int>, int>& dist_map,
                  std::vector<int>& from_points,
                  std::vector<int>& to_points,
                  std::vector<int>& distances,
                  std::mutex& mtx)
		: od_pts_full_id(od_pts_full_id),
    od_pts_full_line_id(od_pts_full_line_id),
    od_pts_full_start(od_pts_full_start),
    od_pts_full_end(od_pts_full_end),
    network_map(network_map),
    dist_map(dist_map),
    from_points(from_points),
    to_points(to_points),
    distances(distances),
    mtx(mtx) {}
	
	void operator()(std::size_t begin, std::size_t end) {
		for (std::size_t i = begin; i < end; ++i) {

			int point_ij = od_pts_full_id[i];
			int line_ij = od_pts_full_line_id[i];
			int start_ij = od_pts_full_start[i];
			int end_ij = od_pts_full_end[i];

			
			auto it_ij = network_map.find(line_ij);
			if (it_ij == network_map.end()) continue;
			
			int source_ij = it_ij->second.first;
			int target_ij = it_ij->second.second;
			
			for (std::size_t j = i; j < od_pts_full_id.size(); ++j) {
				int point_kl = od_pts_full_id[j];
				int line_kl = od_pts_full_line_id[j];
				int start_kl = od_pts_full_start[j];
				int end_kl = od_pts_full_end[j];
				if (point_ij == point_kl) continue;
				
				auto it_kl = network_map.find(line_kl);
				if (it_kl == network_map.end()) continue;
				
				if (it_ij == it_kl) {
					int om_on_distance_diff = start_kl - start_ij;
					int om_on_distance = std::abs(om_on_distance_diff);
					std::lock_guard<std::mutex> guard(mtx);
					from_points.push_back(point_ij);
					to_points.push_back(point_kl);
					distances.push_back(om_on_distance);
					continue;
				}
				
				int source_kl = it_kl->second.first;
				int target_kl = it_kl->second.second;
				
				int nd_ik = get_distance(dist_map, source_ij, source_kl);
				int nd_jl = get_distance(dist_map, target_ij, target_kl);
				int nd_il = get_distance(dist_map, source_ij, target_kl);
				int nd_jk = get_distance(dist_map, source_kl, target_ij);
				
				if (nd_ik == -1.0 || nd_jl == -1.0 || nd_il == -1.0 || nd_jk == -1.0) continue;
				
				int om_on_distance = std::min({
					start_ij + start_kl + nd_ik,
					end_ij + end_kl + nd_jl,
					start_ij + end_kl + nd_il,
					end_ij + start_kl + nd_jk
				});
				
				std::lock_guard<std::mutex> guard(mtx);
				from_points.push_back(point_ij);
				to_points.push_back(point_kl);
				distances.push_back(om_on_distance);
			}
		}
	}
};


void write_results_to_file(const std::string& filename, 
                           const std::vector<int>& from_points,
                           const std::vector<int>& to_points, 
                           const std::vector<int>& distances) {
	std::ofstream file(filename);
	if (file.is_open()) {
		file << "from,to,distance\n";
		for (size_t i = 0; i < from_points.size(); ++i) {
			file << from_points[i] << "," << to_points[i] << "," << distances[i] << "\n";
		}
		file.close();
	} else {
		Rcpp::Rcerr << "Unable to open file";
	}
}

// Funktion zur parallelen Verarbeitung von Netzwerken
// [[Rcpp::export]]
DataFrame parallel_process_networks(DataFrame dt_od_pts_full,
                                       DataFrame dt_network,
                                       DataFrame dt_dist_mat,
                                       int num_cores) {
	
	
	
	auto start = std::chrono::system_clock::now();
	std::time_t start_time = std::chrono::system_clock::to_time_t(start);
	
	
	static std::map<std::pair<int, int>, int> dist_map;
	std::call_once(map_initialized, initialize_map, std::ref(dist_map), dt_dist_mat);
	
	auto end = std::chrono::system_clock::now();
	std::time_t end_time = std::chrono::system_clock::to_time_t(end);
	std::chrono::duration<double> elapsed_seconds = end - start;
	Rcpp::Rcout << "Time needed for converting dt_dist_mat into cpp map: " << elapsed_seconds.count() << " seconds." << std::endl;
	

	IntegerVector od_pts_full_id = dt_od_pts_full["id"];
	IntegerVector od_pts_full_line_id = dt_od_pts_full["id_edge"];
	IntegerVector od_pts_full_start = dt_od_pts_full["dist_to_start"];
	IntegerVector od_pts_full_end = dt_od_pts_full["dist_to_end"];
	

	auto network_map = create_network_map(dt_network);
	
	std::vector<int> from_points;
	std::vector<int> to_points;
	std::vector<int> distances;
	
	std::mutex mtx; // Mutex für diese Funktion
	
	// Berechnung der Chunk-Größe
	std::size_t chunk_size = (od_pts_full_id.size() + num_cores - 1) / num_cores;

	
	// Initialisieren des Workers
	NetworkProcessor worker(od_pts_full_id, od_pts_full_line_id, od_pts_full_start, 
                         od_pts_full_end, network_map, dist_map, from_points, 
                         to_points, distances, mtx);
	
	// Parallele Verarbeitung der Blöcke
	auto start2 = std::chrono::system_clock::now();
	std::time_t start_time2 = std::chrono::system_clock::to_time_t(start);
	
	parallelFor(0, od_pts_full_id.size(), worker, chunk_size);
	
	auto end2 = std::chrono::system_clock::now();
	std::time_t end_time2 = std::chrono::system_clock::to_time_t(end);
	std::chrono::duration<double> elapsed_seconds2 = end2 - start2;
	Rcpp::Rcout << "Time needed for calculating network distances: " << elapsed_seconds2.count() << " seconds." << std::endl;
	
	//write_results_to_file(filename, from_points, to_points, distances);
	
	return DataFrame::create(Named("from") = from_points,
                          Named("to") = to_points,
                          Named("distance") = distances);
}



// [[Rcpp::export]]
Rcpp::DataFrame cpp_find_knn(Rcpp::DataFrame df, int k, IntegerVector flow_ids) {
	Rcpp::IntegerVector all_flows = df["from"];
	Rcpp::IntegerVector flow_m = df["from"];
	Rcpp::IntegerVector flow_n = df["to"];
	Rcpp::IntegerVector distance = df["distance"];
	
	// Create a map to hold the nearest neighbors for each flow_m
	std::map<int, std::vector<std::pair<int, int>>> nn_map;
	
	// Populate the map with distances and neighbors
	for (int i = 0; i < flow_m.size(); ++i) {
		int m = flow_m[i];
		int n = flow_n[i];
		int dist = distance[i];
		nn_map[m].push_back(std::make_pair(dist, n));
	}
	
	// Create a set to hold unique flow_m values
	std::set<int> unique_flow_m_set(flow_ids.begin(), flow_ids.end());
	std::vector<int> unique_flow_m(unique_flow_m_set.begin(), 
                                unique_flow_m_set.end());
	
	// Create vectors for the results
	std::vector<int> result_flow_m;
	std::vector<std::vector<int>> result_nns(unique_flow_m.size(), 
                                          std::vector<int>(k, NA_INTEGER));
	
	// Find the k nearest neighbors for each flow_m
	for (size_t i = 0; i < unique_flow_m.size(); ++i) {
		int m = unique_flow_m[i];
		result_flow_m.push_back(m);
		
		// Sort the neighbors by distance
		std::sort(nn_map[m].begin(), nn_map[m].end());
		
		// Fill the result vectors with the k nearest neighbors
		for (int j = 0; j < std::min(k, (int)nn_map[m].size()); ++j) {
			result_nns[i][j] = nn_map[m][j].second;
		}
	}
	
	// Add missing flow_m values with NA for nearest neighbors
	for (int m : unique_flow_m) {
		if (nn_map.find(m) == nn_map.end()) {
			result_flow_m.push_back(m);
			result_nns.push_back(std::vector<int>(k, NA_INTEGER));
		}
	}
	
	// Create the result DataFrame
	Rcpp::List result;
	result["flow_ref"] = result_flow_m;
	for (int j = 0; j < k; ++j) {
		std::string col_name = "NN" + std::to_string(j + 1);
		Rcpp::IntegerVector col(result_flow_m.size());
		for (size_t i = 0; i < result_flow_m.size(); ++i) {
			col[i] = result_nns[i][j];
		}
		result[col_name] = col;
	}
	
	return Rcpp::DataFrame(result);
}





// [[Rcpp::export]]
List cpp_calc_density_n_get_dr_df(IntegerMatrix dt_knn, int eps, int int_k) {
	// Initiate variables
	int n = dt_knn.nrow();
	std::vector<int> flows;
	std::vector<int> shared_densities;
	List dr_flows(n);
	
	// Loop through all flows of dt_knn
	for (int i = 0; i < n; ++i) {
		int flow = dt_knn(i, 0); 
		
		// Inititate 'knn_i', which is a integer vector that gets filled with the knn
		// of flow i
		std::vector<int> knn_i;
		std::vector<int> dr_flows_i;
		for (int j = 1; j <= int_k; ++j) {
			if(dt_knn(i, j)>0){
				knn_i.push_back(dt_knn(i, j));
			}
		}
		
		// Sort that vector
		std::sort(knn_i.begin(), knn_i.end());
		
		// Inititiate variable 'shared density'
		int shared_density = 0;
		
		// Loop through all the knn-flows of flow i
		for (int k : knn_i) {
			
			// Subtract 1 by k to get the proper row in the matrix (c++ indexing starts at 0)
			int idx = k - 1;
			
			// Inititate 'knn_k', which is a integer vector that gets filled with the knn
			// of all flows k contained in 'knn_i'
			std::vector<int> knn_k;
			for (int m = 1; m <= int_k; ++m) {
				if(dt_knn(idx, m)>0){
					knn_k.push_back(dt_knn(idx, m));
				}
			}
			
			
			
			// Sort that vector
			std::sort(knn_k.begin(), knn_k.end());
			
			// Initiate vector 'intersection' which contains the flows contained
			// in both 'knn_i' and 'knn_k'
			std::vector<int> intersection;
			std::set_intersection(knn_i.begin(), 
                         knn_i.end(), 
                         knn_k.begin(), 
                         knn_k.end(), 
                         std::back_inserter(intersection));
			
			// If the length of 'intersection' is greater than 'eps', this means
			// that the flows considered share more than 'eps' flows, which makes
			// them 'directly reachable' per definition. Therefore, flow k is added 
			// to 'dr_flows_i' and the density of flow i is increased by 1
			if (intersection.size() >= eps) {
				shared_density += 1;
				dr_flows_i.push_back(k);
			}
		}
		
		
		flows.push_back(flow);
		shared_densities.push_back(shared_density);
		dr_flows[i] = wrap(dr_flows_i);
	}
	
	// Create the snn density dataframe
	DataFrame snn_density = DataFrame::create(Named("flow") = flows, 
                                           Named("shared_density") = shared_densities);
	
	
	
	// We also want to have a dataframe containing the directly reachable flows for
	// each flow:
	// Initiate two integer-vectors that will form the 2 columns of the dataframe.
	std::vector<int> from;   
	std::vector<int> dr_flows_final; 
	
	// Loop through the 'dr_flows'-list to fill these two vectors
	for (int i = 0; i < dr_flows.size(); i++) {
		
		// Extract the 'directly reachable' flows of flow i...
		IntegerVector temp = as<IntegerVector>(dr_flows[i]);
		if (temp.size() == 0) {
			continue;
		}
		// and fill the vectors 'from' and 'dr_flows_final'
		for (int j = 0; j < temp.size(); j++) {
			from.push_back(flows[i]); 
			dr_flows_final.push_back(temp[j]);
		}
	}
	
	// Eventually, we have the second dataframe
	DataFrame df_dr_flows = DataFrame::create(Named("from") = from, 
                                           Named("to") = dr_flows_final);
	
	// Return a list of both dataframes
	List result = List::create(Named("snn_density") = snn_density, 
                            Named("dr_flows") = df_dr_flows);
	
	return result;
}





// [[Rcpp::export]]
std::map<int, std::vector<int>> cpp_create_knn_list_from_r_df(DataFrame df) {
	IntegerVector flow_ref = df["flow_ref"];
	int nCols = df.size() - 1; // Anzahl der Spalten ohne flow_ref
	int nRows = df.nrows();
	
	std::map<int, std::vector<int>> flowMap;
	
	for (int i = 0; i < nRows; i++) {
		std::vector<int> values;
		//values.reserve(nCols);
		for (int j = 1; j <= nCols; j++) { // Startet bei 1, um flow_ref zu überspringen
			IntegerVector col = df[j];
			values.push_back(col[i]);
		}
		flowMap[flow_ref[i]] = values;
	}
	
	return flowMap;
}


// [[Rcpp::export]]
std::map<int, std::vector<int>> cpp_assign_clusters(DataFrame dt_cluster,
                                                    DataFrame dt_knn_r,
                                                    IntegerVector flows_reachable_from_core_flow,
                                                    int int_k) {
	
	// Convert knn-df into a C++-map
	IntegerVector flow_ref = dt_knn_r["flow_ref"];
	int nCols = dt_knn_r.size() - 1; 
	int nRows = dt_knn_r.nrows();
	
	std::map<int, std::vector<int>> knn_map;
	std::map<int, std::vector<int>> cluster_map;
	
	for (int i = 0; i < nRows; i++) {
		std::vector<int> values;
		for (int j = 1; j <= nCols; j++) { // Starts at 1, to skip flow_ref 
			IntegerVector col = dt_knn_r[j];
			values.push_back(col[i]);
		}
		knn_map[flow_ref[i]] = values;
	}
	
	//Initiate variables
	IntegerVector flows = dt_cluster["flow"];
	IntegerVector cluster_ids = dt_cluster["cluster_pred"];
	int n = dt_cluster.nrows();
	
	// Loop through all flows contained in the cluster-dataframe...
	for (int i = 0; i < n; ++i) {
		int flow_i = flows[i];
		int cluster_i = cluster_ids[i];
		
		//...get all the knn-flows of the considered flow as a vector.
		std::vector<int> knn_flows = knn_map.at(flow_i);
		// Check if flow is reachable and has no cluster...
		if (cluster_i == 0 && std::find(flows_reachable_from_core_flow.begin(),
                                  flows_reachable_from_core_flow.end(),
                                  flow_i) != flows_reachable_from_core_flow.end()) {
			
			//...if so, we loop through the corresponding knn-flows... 
			for (int j = 0; j < int_k; ++j) {
				int flow_k = knn_flows[j];
				if (std::find(flows.begin(), flows.end(), flow_k) != flows.end()) { // Safety condition
					//...get the row of flow k in the cluster-datafram...
					int pos = std::distance(flows.begin(), std::find(flows.begin(), flows.end(), flow_k));
					//...get the cluster of the corresponding flow...
					int cluster_k = cluster_ids[pos];
					
					//...check if flow k has no clsuter assigned yet...
					if (cluster_k != 0) {
						cluster_map[cluster_k].push_back(flow_i);
						break;
					}
				}
			}
		} else if (cluster_i != 0 && 
			std::find(flows_reachable_from_core_flow.begin(), 
             flows_reachable_from_core_flow.end(), 
             flow_i) != flows_reachable_from_core_flow.end()) {
			cluster_map[cluster_i].push_back(flow_i);
		}
	}
	return cluster_map;
}





