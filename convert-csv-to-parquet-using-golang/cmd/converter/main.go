package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type PriceHistory struct {
	Date       string  `parquet:"name=date, type=BYTE_ARRAY, convertedtype=UTF8"`
	Symbol     string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	Open       float64 `parquet:"name=open, type=DOUBLE"`
	High       float64 `parquet:"name=high, type=DOUBLE"`
	Low        float64 `parquet:"name=low, type=DOUBLE"`
	Close      float64 `parquet:"name=close, type=DOUBLE"`
	VolumeUsdt int64   `parquet:"name=volume_usdt, type=INT64"`
	TradeCount int64   `parquet:"name=trade_count, type=INT64"`
	Token      string  `parquet:"name=token, type=BYTE_ARRAY, convertedtype=UTF8"`
	Hour       int     `parquet:"name=hour, type=INT32"`
	Day        string  `parquet:"name=day, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func main() {
	fw, err := local.NewLocalFileWriter("crypto-price-history.parquet")
	if err != nil {
		log.Printf("can't create local file: %w \n", err)
		return
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, new(PriceHistory), 2)
	if err != nil {
		log.Printf("can't create parquet writer: %w \n", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128MB
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	csvFile, _ := os.Open("crypto-price-history.csv")
	reader := csv.NewReader(bufio.NewReader(csvFile))

	for {
		csvLine, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}

		// Parse the CSV line into the PriceHistory struct
		openFloat, _ := strconv.ParseFloat(csvLine[2], 32)
		highFloat, _ := strconv.ParseFloat(csvLine[3], 32)
		lowFloat, _ := strconv.ParseFloat(csvLine[4], 32)
		closeFloat, _ := strconv.ParseFloat(csvLine[5], 32)
		volumeUsdtInt64, _ := strconv.ParseInt(csvLine[6], 10, 64)
		tradeCountInt64, _ := strconv.ParseInt(csvLine[7], 10, 64)
		hourInt, _ := strconv.Atoi(csvLine[9])

		priceRow := PriceHistory{
			Date:       csvLine[0],
			Symbol:     csvLine[1],
			Open:       openFloat,
			High:       highFloat,
			Low:        lowFloat,
			Close:      closeFloat,
			VolumeUsdt: volumeUsdtInt64,
			TradeCount: tradeCountInt64,
			Token:      csvLine[8],
			Hour:       hourInt,
			Day:        csvLine[10],
		}

		if err = pw.Write(priceRow); err != nil {
			log.Printf("parquet write error: %w \n", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		log.Printf("WriteStop error: %w", err)
		return
	}

	log.Println("CSV to Parquet conversion is done!")
}
