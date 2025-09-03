package executorclient

import (
	"fmt"
	"time"
)

// NamespaceConfig represents configuration for a single namespace
type NamespaceConfig struct {
	Namespace         string        `yaml:"namespace"`
	HeartBeatInterval time.Duration `yaml:"heartbeat_interval"`
}

// Config represents configuration for multiple namespaces
type Config struct {
	Namespaces []NamespaceConfig `yaml:"namespaces"`
}

// GetConfigForNamespace returns the config for a specific namespace
func (c *Config) GetConfigForNamespace(namespace string) (*NamespaceConfig, error) {
	for _, ns := range c.Namespaces {
		if ns.Namespace == namespace {
			return &ns, nil
		}
	}
	return nil, fmt.Errorf("namespace %s not found in config", namespace)
}

// GetSingleConfig returns the config if there's exactly one namespace, otherwise returns an error
func (c *Config) GetSingleConfig() (*NamespaceConfig, error) {
	if len(c.Namespaces) == 0 {
		return nil, fmt.Errorf("no namespaces configured")
	}
	if len(c.Namespaces) > 1 {
		return nil, fmt.Errorf("multiple namespaces configured (%d), must specify which namespace to use", len(c.Namespaces))
	}
	return &c.Namespaces[0], nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if len(c.Namespaces) == 0 {
		return fmt.Errorf("at least one namespace must be configured")
	}

	seenNamespaces := make(map[string]bool)
	for i, ns := range c.Namespaces {
		if ns.Namespace == "" {
			return fmt.Errorf("namespace %d: namespace name cannot be empty", i)
		}
		if ns.HeartBeatInterval <= 0 {
			return fmt.Errorf("namespace %d (%s): heartbeat_interval must be greater than 0", i, ns.Namespace)
		}
		if seenNamespaces[ns.Namespace] {
			return fmt.Errorf("duplicate namespace: %s", ns.Namespace)
		}
		seenNamespaces[ns.Namespace] = true
	}
	return nil
}
