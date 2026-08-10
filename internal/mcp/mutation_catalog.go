package mcp

// Keep mutation-specific schema augmentation in one place so every capability
// mutation remains financially/retry safe without expanding the ordinary
// consumer surface. The definitions are built as maps during package init;
// this pass adds the shared idempotency field before any Server is used.
func init() {
	for _, spec := range orderedToolSpecs {
		switch toolName(spec) {
		case "atos_register_capability", "atos_update_capability", "atos_pause_capability",
			"atos_deliver_job", "atos_request_settlement":
			requireStringProperty(spec.Definition, "idempotency_key")
		}
	}
}

func requireStringProperty(definition map[string]any, property string) {
	input, ok := definition["inputSchema"].(map[string]any)
	if !ok {
		return
	}
	properties, ok := input["properties"].(map[string]any)
	if !ok {
		properties = make(map[string]any)
		input["properties"] = properties
	}
	properties[property] = map[string]any{
		"type":        "string",
		"minLength":   1,
		"description": "Caller-generated key. Same principal + key + request hash replays the original result; a changed request conflicts.",
	}

	required, _ := input["required"].([]string)
	for _, existing := range required {
		if existing == property {
			return
		}
	}
	input["required"] = append(required, property)
}
