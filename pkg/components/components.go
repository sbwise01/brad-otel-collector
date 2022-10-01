package components

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/pprofextension"
	"github.com/sbwise01/brad-otel-collector/pkg/receiver/awss3receiver"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/ballastextension"
	"go.opentelemetry.io/collector/extension/zpagesextension"
)

func Components() (component.Factories, error) {
	var err error
	factories := component.Factories{}

	//Extensions
	factories.Extensions, err = component.MakeExtensionFactoryMap(
		ballastextension.NewFactory(),
		zpagesextension.NewFactory(),
		healthcheckextension.NewFactory(),
		pprofextension.NewFactory(),
	)
	if err != nil {
		return component.Factories{}, err
	}

	//Receivers
	factories.Receivers, err = component.MakeReceiverFactoryMap(
		awss3receiver.NewFactory(),
	)
	if err != nil {
		return component.Factories{}, err
	}

	//Exporters
	factories.Exporters, err = component.MakeExporterFactoryMap(
		fileexporter.NewFactory(),
	)
	if err != nil {
		return component.Factories{}, err
	}

	return factories, nil
}
