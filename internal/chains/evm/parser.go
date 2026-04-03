package evm

import (
	"strings"

	"github.com/1hoodlab/go-indexing.git/internal/core"
	"github.com/1hoodlab/go-indexing.git/pkg/utils"
)

type EventParser struct{}

func NewEventParser() *EventParser { return &EventParser{} }

func (p *EventParser) EventKey(eventIdentifier string) string {
	return utils.EventTopic0(eventIdentifier)
}

func (p *EventParser) ParseEvent(raw core.RawEvent) (map[string]interface{}, error) {
	abiJSON := raw.Metadata["abi"]
	if abiJSON == "" {
		return nil, nil
	}

	fullTopics := []string{raw.EventKey}
	if t := raw.Metadata["topics"]; t != "" {
		for _, topic := range strings.Split(t, ",") {
			if topic != "" {
				fullTopics = append(fullTopics, topic)
			}
		}
	}

	data := raw.Metadata["data"]
	if data == "" {
		data = "0x"
	}

	return nil, nil
}
