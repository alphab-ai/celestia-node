package share

import (
	"fmt"

	"github.com/celestiaorg/celestia-node/nodebuilder/node"
	"github.com/celestiaorg/celestia-node/share/availability/light"
	"github.com/celestiaorg/celestia-node/share/p2p/discovery"
	"github.com/celestiaorg/celestia-node/share/p2p/peers"
	"github.com/celestiaorg/celestia-node/share/p2p/shrexeds"
	"github.com/celestiaorg/celestia-node/share/p2p/shrexnd"
	"github.com/celestiaorg/celestia-node/share/store"
)

// TODO: some params are pointers and other are not, Let's fix this.
type Config struct {
	// EDSStoreParams sets eds store configuration parameters
	EDSStoreParams *store.Parameters

	UseShrEx bool
	UseShwap bool
	// ShrExEDSParams sets shrexeds client and server configuration parameters
	ShrExEDSParams *shrexeds.Parameters
	// ShrExNDParams sets shrexnd client and server configuration parameters
	ShrExNDParams *shrexnd.Parameters
	// PeerManagerParams sets peer-manager configuration parameters
	PeerManagerParams peers.Parameters

	LightAvailability light.Parameters `toml:",omitempty"`
	Discovery         *discovery.Parameters
}

func DefaultConfig(tp node.Type) Config {
	cfg := Config{
		EDSStoreParams:    store.DefaultParameters(),
		Discovery:         discovery.DefaultParameters(),
		ShrExEDSParams:    shrexeds.DefaultParameters(),
		ShrExNDParams:     shrexnd.DefaultParameters(),
		UseShrEx:          true,
		PeerManagerParams: peers.DefaultParameters(),
	}

	if tp == node.Light {
		cfg.LightAvailability = light.DefaultParameters()
	}

	return cfg
}

// Validate performs basic validation of the config.
func (cfg *Config) Validate(tp node.Type) error {
	if tp == node.Light {
		if err := cfg.LightAvailability.Validate(); err != nil {
			return fmt.Errorf("nodebuilder/share: %w", err)
		}
	}

	if err := cfg.Discovery.Validate(); err != nil {
		return fmt.Errorf("nodebuilder/share: %w", err)
	}

	if err := cfg.ShrExNDParams.Validate(); err != nil {
		return fmt.Errorf("nodebuilder/share: %w", err)
	}

	if err := cfg.ShrExEDSParams.Validate(); err != nil {
		return fmt.Errorf("nodebuilder/share: %w", err)
	}

	if err := cfg.PeerManagerParams.Validate(); err != nil {
		return fmt.Errorf("nodebuilder/share: %w", err)
	}

	return nil
}
