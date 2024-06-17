#include <Rcpp.h>
#include <fstream>
//#include <gperftools/heap-profiler.h>
using namespace Rcpp;


// Hier wird der R-dataframe, der das road network angibt übergeben und in ein
// C++ Objekt umgewandelt: 
	// Die einzelnen Spalten werden erst als einfache C++ Integer-Vektoren gespeichert. 
// Dann werden "map" und "pair" kombiniert um für jede id des road networks source
// und target zu hinterlegen.
std::map<int, std::pair<int, int>> create_network_map(DataFrame network) {
	IntegerVector ids = network["id"];
	IntegerVector sources = network["source"];
	IntegerVector targets = network["target"];
	std::map<int, std::pair<int, int>> net_map;
	for (int i = 0; i < ids.size(); i++) {
		net_map[ids[i]] = std::make_pair(sources[i], targets[i]);
	}
	return net_map;
}

// Hier wird der R-dataframe, der die local node distance matrix angibt übergeben 
// und in ein C++ Objekt umgewandelt: 
	// Die einzelnen Spalten werden erst als einfache C++ Integer- und Numeric-Vektoren gespeichert. 
// Dann werden "map" und "pair" kombiniert um für jedes node poir die entsprechende
// Netzwerk-Distanz als double zu hinterlegen.
std::map<std::pair<int, int>, int> create_dist_map(DataFrame dist_mat) {
	IntegerVector sources = dist_mat["source"];
	IntegerVector targets = dist_mat["target"];
	IntegerVector m = dist_mat["m"];
	std::map<std::pair<int, int>, int> dist_map;
	for (int i = 0; i < sources.size(); i++) {
		int min_idx = std::min(sources[i], targets[i]);
		int max_idx = std::max(sources[i], targets[i]);
		dist_map[std::make_pair(min_idx, max_idx)] = m[i];
	}
	return dist_map;
}

// Funktion, die in der C++ dist-mat-map nach dem übergegeben node-pair sucht und
// dann die entsprechende Netzwerk-Distanz zurückgibt.
// "end()" zeigt auf das letzte Element einer map. Wenn mit "find()" kein Wert in 
// der Distanz-Matrix gefunden wurde, dann zeigt "it" auf das Ende der map. Wenn
// also it!=dist_map.end(), dann wird mit "second" auf das zweite Element des Paares
// it zugegriffen, was der Distanz entspricht. Andernfalls, wenn für gegeben source
// und target kein Eintrag gefunden wurde, dann wird -1 zurückgegeben
double get_distance(const std::map<std::pair<int, int>, int>& dist_map, int source, int target) {
	auto it = dist_map.find(std::make_pair(std::min(source, target), 
                                        std::max(source, target)));
	return (it != dist_map.end()) ? it->second : -1.0;
}

void printMemoryUsage(const std::string& note) {
	std::ifstream file("/proc/self/status");
	std::string line;
	while (std::getline(file, line)) {
		if (line.substr(0, 6) == "VmSize") {
			Rcpp::Rcout << note << " " << line << "\n";
			break;
		}
	}
}


template <typename T>
void printVectorSize(const std::vector<T>& v, const std::string& name) {
	Rcpp::Rcout << "Memory usage of " << name << ": " << (v.capacity() * sizeof(T)) / 1024.0 << " KB" << std::endl;
}

template <typename T>
void printActualVectorSize(const std::vector<T>& v, const std::string& name) {
	Rcpp::Rcout << "Actual memory usage of " << name << ": " 
             << (v.size() * sizeof(T)) / 1024.0 << " KB" << std::endl;
}

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
DataFrame process_networks(DataFrame dt_od_pts_sub, 
													 DataFrame dt_od_pts_full, 
													 DataFrame dt_network, 
													 DataFrame dt_dist_mat) {
	
	//Split R-dataframes in C++ vectors
	IntegerVector od_pts_sub_id = dt_od_pts_sub["id_new"];
	IntegerVector od_pts_sub_line_id = dt_od_pts_sub["id_edge"];
	IntegerVector od_pts_sub_start = dt_od_pts_sub["dist_to_start"];
	IntegerVector od_pts_sub_end = dt_od_pts_sub["dist_to_end"];
	IntegerVector od_pts_full_id = dt_od_pts_full["id_new"];
	IntegerVector od_pts_full_line_id = dt_od_pts_full["id_edge"];
	IntegerVector od_pts_full_start = dt_od_pts_full["dist_to_start"];
	IntegerVector od_pts_full_end = dt_od_pts_full["dist_to_end"];
	
	auto network_map = create_network_map(dt_network);
	auto dist_map = create_dist_map(dt_dist_mat);
	
	int n_sub = od_pts_sub_id.size();
	int n_full = od_pts_full_id.size();
	
	std::vector<int> from_points;
	std::vector<int> to_points;
	std::vector<int> distances;
	from_points.reserve(n_sub * 10); 
	to_points.reserve(n_sub * 10);
	distances.reserve(n_sub * 10);;

	for (int i = 0; i < n_sub; ++i) {

		int point_ij = od_pts_sub_id[i];
		int line_ij = od_pts_sub_line_id[i];
		int start_ij = od_pts_sub_start[i];
		int end_ij = od_pts_sub_end[i];

		
		//if (network_map.find(line_ij) == network_map.end()) continue;
		
		int source_ij = network_map[line_ij].first;
		int target_ij = network_map[line_ij].second;

		
		// Rcpp::Rcout << "From point: " << i << std::endl;
		
		for (int j = i; j < n_full; ++j) {
			int point_kl = od_pts_full_id[j];

			int line_kl = od_pts_full_line_id[j];
			int start_kl = od_pts_full_start[j];
			int end_kl = od_pts_full_end[j];
			
			// Skip, if points considered are the same
			if (point_ij == point_kl) continue;
			
			// Case: points are on the same line
			if (network_map.find(line_ij) == network_map.find(line_kl)){
				int om_on_distance_diff = start_kl - start_ij;
				int om_on_distance = std::abs(om_on_distance_diff);  
				from_points.push_back(point_ij);
				to_points.push_back(point_kl);
				distances.push_back(om_on_distance);
				continue;
			};

			
			// Case: points are on different lines
			int source_kl = network_map[line_kl].first;
			int target_kl = network_map[line_kl].second;
			int nd_ik = get_distance(dist_map, source_ij, source_kl);
			int nd_jl = get_distance(dist_map, target_ij, target_kl);
			int nd_il = get_distance(dist_map, source_ij, target_kl);
			int nd_jk = get_distance(dist_map, source_kl, target_ij);

			// If one of the points involved lies on 
			if (nd_ik == -1.0 || 
          nd_jl == -1.0 || 
          nd_il == -1.0 || 
          nd_jk == -1.0) continue;

			int om_on_distance = std::min({
				start_ij + start_kl + nd_ik,
				end_ij + end_kl + nd_jl,
				start_ij + end_kl + nd_il,
				end_ij + start_kl + nd_jk
			});

			
			from_points.push_back(point_ij);
			to_points.push_back(point_kl);
			distances.push_back(om_on_distance);

			
		}
		if (i % 100 == 0) {  
			printVectorSize(from_points, "from_points during");
			printVectorSize(to_points, "to_points during");
			printVectorSize(distances, "distances during");
			printActualVectorSize(from_points, "from_points during");
			printActualVectorSize(to_points, "to_points during");
			printActualVectorSize(distances, "distances during");
		}
		
	}
	return DataFrame::create(Named("from") = from_points, 
													 Named("to") = to_points, 
													 Named("distance") = distances);

}




// [[Rcpp::export]]
List cpp_calc_density_n_get_dr_df(IntegerMatrix dt_knn, IntegerVector id, int eps, int int_k) {
	int n = dt_knn.nrow();
	std::vector<int> flows;
	std::vector<int> shared_densities;
	List dr_flows(n);
	
	for (int i = 0; i < n; ++i) {
		int flow = dt_knn(i, 0); 
		//Rcpp::Rcout << "Flow: " << flow << std::endl;
		
		std::vector<int> knn_i;
		std::vector<int> dr_flows_i;
		for (int j = 1; j <= int_k; ++j) {
			knn_i.push_back(dt_knn(i, j));
		}
		
		std::sort(knn_i.begin(), knn_i.end());
		// if(flow==1){
			// 	for (auto v : knn_i) Rcpp::Rcout << v << " ";
			// 	Rcpp::Rcout << "\n";
			// }
		
		int shared_density = 0;
		
		for (int k : knn_i) {
			if (std::find(id.begin(), id.end(), k) != id.end()) {
				// if(flow==1){
					// 	Rcpp::Rcout << "k: " << k << std::endl;
					// }
				int idx = std::find(id.begin(), id.end(), k) - id.begin();
				std::vector<int> knn_k;
				for (int m = 1; m <= int_k; ++m) {
					knn_k.push_back(dt_knn(idx, m));
				}
				
				std::sort(knn_i.begin(), knn_i.end());
				std::sort(knn_k.begin(), knn_k.end());
				std::vector<int> intersection;
				std::set_intersection(knn_i.begin(), 
															knn_i.end(), 
															knn_k.begin(), 
															knn_k.end(), 
															std::back_inserter(intersection));
				// if(flow==1){
					// 	for (auto v : intersection) Rcpp::Rcout << v << " ";
					// 	Rcpp::Rcout << "\n";
					// 	break;
					// }
				if (intersection.size() >= eps) {
					shared_density += 1;
					dr_flows_i.push_back(k);
				}
			}
		}
		
		
		flows.push_back(flow);
		shared_densities.push_back(shared_density);
		dr_flows[i] = wrap(dr_flows_i);
	}
	
	DataFrame snn_density = DataFrame::create(Named("flow") = flows, 
																						Named("shared_density") = shared_densities);
	
	std::vector<int> from;   // Speichert die Indexwerte 
	std::vector<int> dr_flows_final; // Speichert die Werte in den Vektoren
	
	Rcpp::Rcout << "flows.size(): " << flows.size() << std::endl;
	Rcpp::Rcout << "dr_flows.size(): " << dr_flows.size() << std::endl;
	// Durchgehen der Liste
	for (int i = 0; i < dr_flows.size(); i++) {
		
		IntegerVector temp = as<IntegerVector>(dr_flows[i]);
		// Füge den aktuellen Index so oft hinzu, wie es Elemente im Vektor gibt
		if (temp.size() == 0) {
			
			continue;
		}
		for (int j = 0; j < temp.size(); j++) {
			// -----------------------------------------------------------------------FLOWS STATT INDEX NEHMEN--------------------
				from.push_back(flows[i]); // i + 1 für 1-basierte Indexierung
			dr_flows_final.push_back(temp[j]);
		}
	}
	
	DataFrame df_dr_flows = DataFrame::create(Named("from") = from, 
																						Named("to") = dr_flows_final);
	
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
	IntegerVector cluster_ids = dt_cluster["cluster_id"];
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





