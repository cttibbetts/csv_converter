package main

import (
    "bufio"
    "encoding/csv"
    "fmt"
    "io"
    "os"
    "strings"
)

func main() {
    fmt.Println("This is a quick converter for the sysomos data format.")
    fmt.Println("It will take a csv file, pull out all the twitter data,")
    fmt.Println("and put it into the old format in a file called 'output.csv'")

    fmt.Print("Enter file path (root here): ")

    var filepath string
    if _, err := fmt.Scanln(&filepath); err != nil {
        fmt.Printf("Failed to receive filepath: %v\n%v", filepath, err)
        fmt.Scanln()
        os.Exit(1)
    }

    f, err := os.Open(filepath)
    defer f.Close()
    if err != nil {
        fmt.Printf("Failed to open file: %v\n%v", filepath, err)
        fmt.Scanln()
        os.Exit(1)
    }
    r := csv.NewReader(bufio.NewReader(f))

    outfile, err := os.Create("output.csv")
    defer outfile.Close()
    if err != nil {
        fmt.Printf("Failed to create output file: %v", err)
        fmt.Scanln()
        os.Exit(1)
    }

    w := csv.NewWriter(bufio.NewWriterSize(outfile, 10))
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

            // fix nil age
            if record[5] == "" {
                record[5] = "0"
            }
            // fix strange star value
            if record[19] == "none+" {
                record[19] = ""
            }

            newrow := []string{
                strings.ToLower(record[0]),        // source
                strings.Join(strings.Split(record[2], "/")[0:4], "/"), // host
                record[2],                         // link
                record[3],                         // time (ET)
                record[3],                         // time (GMT) // Hardcoded to be ET as GMT doesn't exist
                record[4],                         // auth
                record[5],                         // age
                strings.ToLower(record[6]),        // gender
                strings.ToLower(record[7]),        // country
                record[8],                         // location
                strings.Join(tags, " "),           // tags
                record[19],                        // star
                strings.ToLower(record[20]),       // assigned
                strings.ToLower(record[21]),       // sentiment
                record[22],                        // title
                strings.Replace(strings.Trim(record[23], "\r\n "), ",", "", -1), // snippet
                strings.Replace(strings.Trim(record[24], "\r\n "), ",", "", -1), // contents
                record[25],                        // uniqueid
                strings.ToLower(record[26]),       // language
                record[27],                        // followers
                record[28],                        // following
            }

            err := w.Write(newrow)
            if err != nil {
                fmt.Println(err)
            }
        }
    }
    w.Flush()

    fmt.Println("Done. Your file should be called: output.csv")
    fmt.Scanln()
}
