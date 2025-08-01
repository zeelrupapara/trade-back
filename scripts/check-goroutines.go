package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

func main() {
	// Check goroutine count via HTTP endpoint
	url := "http://localhost:8080/debug/pprof/goroutine?debug=1"
	
	for {
		resp, err := http.Get(url)
		if err != nil {
			fmt.Printf("Error fetching pprof: %v\n", err)
			time.Sleep(5 * time.Second)
			continue
		}
		
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		
		if err != nil {
			fmt.Printf("Error reading response: %v\n", err)
			time.Sleep(5 * time.Second)
			continue
		}
		
		// Count goroutines by counting "goroutine" occurrences
		content := string(body)
		count := 0
		for i := 0; i < len(content); i++ {
			if i+9 < len(content) && content[i:i+9] == "goroutine" {
				count++
			}
		}
		
		fmt.Printf("[%s] Goroutines: %d\n", time.Now().Format("2006-01-02 15:04:05"), count)
		time.Sleep(5 * time.Second)
	}
}