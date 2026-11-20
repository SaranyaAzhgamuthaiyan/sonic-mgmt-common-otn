package transformer

// findPayloadList walks down nested JSON objects and returns the first JSON
// array it finds, together with the object that holds it and its key.
func findPayloadList(m map[string]interface{}) (map[string]interface{}, string, []interface{}, bool) {
	for k, v := range m {
		switch val := v.(type) {
		case []interface{}:
			return m, k, val, true
		case map[string]interface{}:
			if parent, key, list, ok := findPayloadList(val); ok {
				return parent, key, list, true
			}
		}
	}
	return nil, "", nil, false
}

// mergeNsPayloads merges the per-entity payloads of one namespace into a
// single payload. Each payload holds the list instances either at the top
// level ({"amplifier":[...]}) or below a container ({"amplifiers":{"amplifier":[...]}});
// the nesting of the first payload is kept.
func mergeNsPayloads(payloads []map[string]interface{}) map[string]interface{} {
	if len(payloads) == 0 {
		return map[string]interface{}{}
	}
	merged := payloads[0]
	parent, key, list, ok := findPayloadList(merged)
	if !ok {
		return merged
	}
	for _, p := range payloads[1:] {
		if _, _, instances, found := findPayloadList(p); found {
			list = append(list, instances...)
		}
	}
	parent[key] = list
	return merged
}
