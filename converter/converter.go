package converter

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/f-taxes/kraken_conversion/conf"
	"github.com/f-taxes/kraken_conversion/global"
	"github.com/kataras/golog"
	"github.com/shopspring/decimal"
	"go.uber.org/ratelimit"
)

var limiter = ratelimit.New(9, ratelimit.Per(time.Minute))

type priceResponse struct {
	Errors []string       `json:"error"`
	Result map[string]any `json:"result"`
}

func PriceAtTime(asset, targetCurrency string, ts time.Time) (decimal.Decimal, error) {
	if price := fromArchiveFile(asset, targetCurrency, ts); !price.IsZero() {
		return price, nil
	}

	if conf.App.Bool("skipApi") {
		return decimal.Zero, nil
	}

	limiter.Take()
	ts = global.StartOfMinute(ts)

	resp, err := http.Get(fmt.Sprintf("https://api.kraken.com/0/public/OHLC?pair=%s%s&interval=1&since=%d", asset, targetCurrency, ts.Add(-5*time.Minute).Unix()))
	if err != nil {
		return decimal.Zero, err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return decimal.Zero, err
	}

	respData := priceResponse{}
	err = json.Unmarshal(body, &respData)
	if err != nil {
		return decimal.Zero, err
	}

	if len(respData.Errors) > 0 {
		return decimal.Zero, errors.New(respData.Errors[0])
	}

	for _, records := range respData.Result {
		if list, ok := records.([]any); ok {
			for _, rec := range list {
				r := rec.([]any)
				t := time.Unix(int64(r[0].(float64)), 0).UTC()
				if ts.Equal(t) || (ts.After(t) && ts.Before(t.Add(time.Minute))) {
					if p, ok := r[1].(string); ok {
						return global.StrToDecimal(p), nil
					}
				}
			}
		}
	}

	return decimal.Zero, nil
}

func fromArchiveFile(asset, targetCurrency string, ts time.Time) decimal.Decimal {
	if !conf.App.Bool("historicDataArchive.enabled") {
		return decimal.Zero
	}

	invert := false
	if asset == "USD" && targetCurrency == "EUR" {
		invert = true
		a := asset
		asset = targetCurrency
		targetCurrency = a
	}

	if asset == "BTC" {
		asset = "XBT"
	}

	archivePath := filepath.Join(conf.App.String("historicDataArchive.path"), fmt.Sprintf("%s%s_1.csv", asset, targetCurrency))

	if _, err := os.Stat(archivePath); os.IsNotExist(err) {
		golog.Warnf("Archive file %s does not exist. Falling back to API request.", archivePath)
		return decimal.Zero
	}

	file, err := os.Open(archivePath)
	if err != nil {
		golog.Errorf("Error opening file: %v", err)
		return decimal.Zero
	}

	defer file.Close()

	ts = global.StartOfMinute(ts)
	reader := csv.NewReader(file)

	firstTs := time.Time{}
	lastTs := time.Time{}

	for {
		r, err := reader.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				golog.Error(err)
			}
			break
		}

		if len(r) < 2 {
			continue
		}

		unixTs, err := strconv.ParseInt(r[0], 10, 64)
		if err != nil {
			continue
		}

		t := time.Unix(unixTs, 0).UTC()
		if firstTs.IsZero() {
			firstTs = t
		}

		lastTs = t

		// If we encounter a timestamp that comes after the one we're looking for, we know that there weren't any trades at the desired point of time and hence there is no record in the file.
		// In that case we use the first record after unless it's too far off the desired time.
		if t.After(ts) {
			deltaAfter := t.Sub(ts)

			if deltaAfter < time.Hour*6 {
				if invert {
					return global.StrToDecimal("1").Div(global.StrToDecimal(r[1], decimal.Zero))
				}

				return global.StrToDecimal(r[1], decimal.Zero)
			}
		}

		if ts.Equal(t) {
			if invert {
				return global.StrToDecimal("1").Div(global.StrToDecimal(r[1], decimal.Zero))
			}

			return global.StrToDecimal(r[1], decimal.Zero)
		}
	}

	golog.Warnf("Failed to find a record with timestamp %s in %s. Range of file was %s - %s.", ts, archivePath, firstTs, lastTs)

	return decimal.Zero
}
