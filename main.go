package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

func main() {
	fmt.Println("This is a quick converter for the sysomos data format")
	fmt.Println("It will take a csv file, pull out all the twitter data,")
	fmt.Println("and put it into the old format in a file with the \"fixed_\"")
	fmt.Println("prefix. Your input file should be in this directory")
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter file name: ")
	text, _ := reader.ReadString('\n')
	text = strings.Trim(text, "\n ")

	f, err := os.Open(text)
	if err != nil {
		log.Fatalf("Failed to open file: %v\n%v", text, err)
	}
	r := csv.NewReader(bufio.NewReader(f))

	outname := "fixed_" + text
	outfile, err := os.Create(outname)
	if err != nil {
		log.Fatalf("Failed to create output file: %v\n%v", outname, err)
	}
	w := csv.NewWriter(bufio.NewWriter(outfile))
	w.Write([]string{
		"source",
		"host",
		"link",
		"time (ET)",
		"time (GMT)",
		"auth",
		"age",
		"gender",
		"country",
		"location",
		"tags",
		"star",
		"assigned",
		"sentiment",
		"title",
		"snippet",
		"contents",
		"uniqueid",
		"language",
		"followers",
		"following",
	})

	for {
		record, err := r.Read()
		// Stop at EOF.
		if err == io.EOF {
			break
		}

		if len(record) > 5 {
			if record[0] == "Source" {
				continue
			}
			if record[0] != "TWITTER" {
				continue
			}

			var tags []string
			for _, tag := range record[9:18] {
				if tag != "" {
					tags = append(tags, tag)
				}
			}

			newrow := []string{
				strings.ToLower(record[0]),  // source
				record[1],                   // host
				record[2],                   // link
				record[3],                   // time (ET)
				"",                          // time (GMT)
				record[4],                   // auth
				record[5],                   // age
				strings.ToLower(record[6]),  // gender
				strings.ToLower(record[7]),  // country
				record[8],                   // location
				strings.Join(tags, " "),     // tags
				record[19],                  // star
				record[20],                  // assigned
				strings.ToLower(record[21]), // sentiment
				record[22],                  // title
				record[23],                  // snippet
				record[24],                  // contents
				record[25],                  // uniqueid
				strings.ToLower(record[26]), // language
				record[27],                  // followers
				record[28],                  // following
			}

			w.Write(newrow)
		}
	}
	fmt.Println("Done. Your file should be called: " + outname)
}
