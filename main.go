package main

import "bq-to-es-cli/cmd"

func main() {
	cmd.Execute()
}


// ./bq-to-es-cli import --project-id=world-fishing-827 --query="SELECT * FROM scratch_megan.peru_track_data WHERE CAST(ssvid AS INT64) =  636091690" --elastic-search-url="https://alvaro@test@elastic.gfw.org"