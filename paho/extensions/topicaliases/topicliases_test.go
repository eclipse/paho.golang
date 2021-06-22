package topicaliases

import (
	"testing"

	"github.com/ChIoT-Tech/paho.golang/paho"
	"github.com/stretchr/testify/assert"
)

func TestTAHandler_PublishHook(t *testing.T) {
	tests := []struct {
		name            string
		aliasMax        uint16
		aliases         []string
		p               *paho.Publish
		expected        *paho.Publish
		expectedAliases []string
	}{
		{
			name:     "has alias",
			aliasMax: 4,
			aliases:  []string{"", "", "", "test", ""},
			p: &paho.Publish{
				Topic: "test",
			},
			expected: &paho.Publish{
				Properties: &paho.PublishProperties{
					TopicAlias: paho.Uint16(3),
				},
			},
			expectedAliases: []string{"", "", "", "test", ""},
		},
		{
			name:     "reset alias",
			aliasMax: 4,
			aliases:  []string{"", "", "", "test", ""},
			p: &paho.Publish{
				Topic: "test2",
				Properties: &paho.PublishProperties{
					TopicAlias: paho.Uint16(3),
				},
			},
			expected: &paho.Publish{
				Topic: "test2",
				Properties: &paho.PublishProperties{
					TopicAlias: paho.Uint16(3),
				},
			},
			expectedAliases: []string{"", "", "", "test2", ""},
		},
		{
			name:     "no alias",
			aliasMax: 4,
			aliases:  []string{"", "", "", "", ""},
			p: &paho.Publish{
				Topic: "test",
			},
			expected: &paho.Publish{
				Properties: &paho.PublishProperties{
					TopicAlias: paho.Uint16(1),
				},
			},
			expectedAliases: []string{"", "test", "", "", ""},
		},
		{
			name:     "properties no alias",
			aliasMax: 4,
			aliases:  []string{"", "", "", "", ""},
			p: &paho.Publish{
				Topic:      "test",
				Properties: &paho.PublishProperties{},
			},
			expected: &paho.Publish{
				Properties: &paho.PublishProperties{
					TopicAlias: paho.Uint16(1),
				},
			},
			expectedAliases: []string{"", "test", "", "", ""},
		},
		{
			name:     "no alias free",
			aliasMax: 1,
			aliases:  []string{"", "full"},
			p: &paho.Publish{
				Topic: "test",
			},
			expected: &paho.Publish{
				Topic: "test",
			},
			expectedAliases: []string{"", "full"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ta := &TAHandler{
				aliasMax: tt.aliasMax,
				aliases:  tt.aliases,
			}
			ta.PublishHook(tt.p)
			assert.Equal(t, tt.expected, tt.p)
			assert.Equal(t, tt.expectedAliases, ta.aliases)
		})
	}
}
