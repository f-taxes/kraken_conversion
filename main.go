package main

import (
	"encoding/json"
	"os"

	"github.com/f-taxes/kraken_conversion/conf"
	"github.com/f-taxes/kraken_conversion/ctl"
	"github.com/f-taxes/kraken_conversion/global"
	"github.com/kataras/golog"
)

func init() {
	manifestContent, err := os.ReadFile("./manifest.json")

	if err != nil {
		golog.Fatalf("Failed to read manifest file: %v", err)
		os.Exit(1)
	}

	err = json.Unmarshal(manifestContent, &global.Plugin)

	if err != nil {
		golog.Fatalf("Failed to parse manifest: %v", err)
		os.Exit(1)
	}
}

func main() {
	conf.LoadAppConfig("./config.yaml")

	ctl.Start(global.Plugin.Ctl.Address)
}
