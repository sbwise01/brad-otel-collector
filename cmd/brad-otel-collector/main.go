package main

import (
	"log"

	version "github.com/sbwise01/brad-otel-collector"
	"github.com/sbwise01/brad-otel-collector/pkg/components"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/service"
)

func main() {
	factories, err := components.Components()
	if err != nil {
		log.Fatal("faild to build brad-otel-collector components: %v", err)
	}
	info := component.BuildInfo{
		Command:     "brad-otel-collector",
		Description: "Brad OpenTelemetry Collector",
		Version:     version.Version,
	}

	err = run(service.CollectorSettings{BuildInfo: info, Factories: factories})
	if err != nil {
		log.Fatal(err)
	}
}

func run(settings service.CollectorSettings) error {
	return runInteractive(settings)
}

func runInteractive(params service.CollectorSettings) error {
	cmd := service.NewCommand(params)
	err := cmd.Execute()
	if err != nil {
		log.Fatal("collector server run finished with error: %v", err)
	}
	return nil
}
