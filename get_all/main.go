package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/go-resty/resty/v2"
)

const (
	baseURL        = "http://localhost:10001"
	indexName      = "scg_es_product_en"
	searchEndpoint = "_search"
	propertyName   = "skuNumber"
	searchProperty = "skuName_en"
	searchTerm     = "Array"
	batchSize      = 100
)

type Hit struct {
	Source map[string]string `json:"_source"`
}

type SearchResult struct {
	Hits struct {
		Hits  []Hit `json:"hits"`
		Total int   `json:"total"`
	} `json:"hits"`
}

func main() {
	// Create a new Resty client
	client := resty.New()

	// Set the Elasticsearch endpoint URL
	endpoint := fmt.Sprintf("%s/%s/%s", baseURL, indexName, searchEndpoint)

	// Set the search request body
	requestBody := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]string{
				searchProperty: searchTerm,
			},
		},
		"_source": []string{propertyName},
		"size":    batchSize,
		"sort":    []string{"category5.code"},
	}

	// Open the output file in append mode
	file, err := os.OpenFile("output_get_all.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Error opening output file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Initialize variables for tracking the number of results
	// and the starting index for the next page of results
	var numResults int
	var fromIndex int

	for {
		// Set the starting index for this page of results
		requestBody["from"] = fromIndex

		// Send the search request and retrieve the response
		response, err := client.R().
			SetHeader("Content-Type", "application/json").
			SetBody(requestBody).
			Post(endpoint)
		if err != nil {
			fmt.Printf("Error sending search request: %v\n", err)
			os.Exit(1)
		}

		// Parse the response and extract the properties
		var searchResult SearchResult
		err = json.Unmarshal(response.Body(), &searchResult)
		if err != nil {
			fmt.Printf("Error decoding search response: %v\n", err)
			os.Exit(1)
		}

		for _, hit := range searchResult.Hits.Hits {
			// Write the title to the output file
			_, err := file.WriteString(fmt.Sprintf("%s\n", hit.Source[propertyName]))
			if err != nil {
				fmt.Printf("Error writing to output file: %v\n", err)
				os.Exit(1)
			}
		}

		// Update the number of results and the starting index for the next page
		numResults += len(searchResult.Hits.Hits)
		fromIndex += len(searchResult.Hits.Hits)

		// Print the number of results retrieved so far
		fmt.Printf("Retrieved %d results of %d\n", numResults, searchResult.Hits.Total)

		// Exit the loop if we've retrieved all the results
		if numResults >= searchResult.Hits.Total {
			break
		}
	}
}
