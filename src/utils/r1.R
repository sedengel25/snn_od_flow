# Documentation: r1_create_chunks
# Usage: r1_create_chunks(cores, n)
# Description: Splits the ids in squared increasing chunks for parallelization
# Args/Options: cores, n
# Returns: int
# Output: ...
# Action: ... 
r1_create_chunks <- function(cores, n) {
	chunk_size <- n / cores
	
	# Calculate the breaks
	breaks <- round(seq(1, n, by = chunk_size))
	
	# Ensure the last break is equal to 'n' to include all elements
	if(breaks[length(breaks)] != n) {
		breaks <- c(breaks, n)
	}
	
	# Return the breaks
	return(breaks[-1])
}