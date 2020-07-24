package graph

import (
	"encoding/json"
)

// NodeConfig contains configuration data meant for customizing the functionality
// of a Node.
type NodeConfig struct {
	config map[string]interface{}
}

func NewNodeConfig(cfg map[string]interface{}) NodeConfig {
	return NodeConfig{config: cfg}
}

func (c *NodeConfig) Get(key string) interface{} {
	val, _ := c.config[key]
	// TODO: Handle non-exist better..
	//if !ok {
	//	fmt.Printf("key not found '%s'\n", key)
	//}
	return val
}

func (c *NodeConfig) GetString(key string) string {
	if val, ok := c.config[key]; ok {
		if v, ok := val.(string); ok {
			return v
		}
	}
	return ""
}

func (c *NodeConfig) GetStringMap(key string) map[string]interface{} {
	val, ok := c.config[key]
	if !ok {
		return nil
	}
	if m, ok := val.(map[string]interface{}); ok {
		return m
	}
	return nil
}

func (c *NodeConfig) GetInt32(key string) int32 {
	if val, ok := c.config[key]; ok {
		switch t := val.(type) {
		case int32:
			return t
		case int8:
			return int32(t)
		}
	}
	return 0
}

func (c *NodeConfig) GetInt64(key string) int64 {
	if val, ok := c.config[key]; ok {
		switch t := val.(type) {
		case int64:
			return t
		case int32:
			return int64(t)
		case int8:
			return int64(t)
		}
	}
	return 0
}

func (c *NodeConfig) GetBool(key string) bool {
	if val, ok := c.config[key]; ok {
		if v, ok := val.(bool); ok {
			return v
		}
	}
	return false
}

func (c *NodeConfig) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.config)
}

func (c *NodeConfig) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &c.config)
}
