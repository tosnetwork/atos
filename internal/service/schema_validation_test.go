package service

import "testing"

func TestValidateSchemaDocument_ValidSchema(t *testing.T) {
	if err := validateSchemaDocument("input_schema", map[string]any{
		"type": "object", "properties": map[string]any{"x": map[string]any{"type": "string"}},
	}); err != nil {
		t.Fatalf("expected a valid schema to pass: %v", err)
	}
}

func TestValidateSchemaDocument_EmptyObjectIsValid(t *testing.T) {
	// {} is a valid JSON Schema (matches everything) -- must not be
	// rejected just for being permissive.
	if err := validateSchemaDocument("input_schema", map[string]any{}); err != nil {
		t.Fatalf("expected {} to be a valid schema: %v", err)
	}
}

func TestValidateSchemaDocument_InvalidType(t *testing.T) {
	err := validateSchemaDocument("input_schema", map[string]any{"type": "not_a_real_json_schema_type"})
	if err == nil {
		t.Fatal("expected an error for an invalid \"type\" value")
	}
}

func TestValidateSchemaDocument_InvalidRegexPattern(t *testing.T) {
	err := validateSchemaDocument("input_schema", map[string]any{
		"type": "string", "pattern": "[unterminated",
	})
	if err == nil {
		t.Fatal("expected an error for a malformed regex pattern")
	}
}

func TestValidateSchemaDocument_SelfContradictoryMinMax(t *testing.T) {
	// minimum > maximum is not itself a JSON-Schema-syntax error (the
	// schema is well-formed; it would just reject every instance), so
	// this documents that validateSchemaDocument checks document
	// VALIDITY, not semantic satisfiability -- consistent with the
	// roadmap's "validate the schema documents themselves" framing.
	err := validateSchemaDocument("input_schema", map[string]any{
		"type": "integer", "minimum": 10, "maximum": 1,
	})
	if err != nil {
		t.Fatalf("a syntactically valid (if unsatisfiable) schema should compile: %v", err)
	}
}

func TestValidateSchemaDocument_NilRejected(t *testing.T) {
	if err := validateSchemaDocument("input_schema", nil); err == nil {
		t.Fatal("expected an error for a nil schema")
	}
}

func TestValidateCapabilitySchemas_BothMustBeValid(t *testing.T) {
	valid := map[string]any{"type": "object"}
	invalid := map[string]any{"type": "nonsense"}

	if err := validateCapabilitySchemas(invalid, valid); err == nil {
		t.Fatal("expected input_schema failure to be reported")
	}
	if err := validateCapabilitySchemas(valid, invalid); err == nil {
		t.Fatal("expected output_schema failure to be reported")
	}
	if err := validateCapabilitySchemas(valid, valid); err != nil {
		t.Fatalf("expected two valid schemas to pass: %v", err)
	}
}

func TestValidateAgainstSchema_ValidInstancePasses(t *testing.T) {
	schema := map[string]any{
		"type": "object", "required": []any{"name"},
		"properties": map[string]any{"name": map[string]any{"type": "string"}, "count": map[string]any{"type": "integer"}},
	}
	instance := map[string]any{"name": "hello", "count": 3}
	if err := validateAgainstSchema("input", schema, instance); err != nil {
		t.Fatalf("expected a valid instance to pass: %v", err)
	}
}

func TestValidateAgainstSchema_MissingRequiredFieldFails(t *testing.T) {
	schema := map[string]any{"type": "object", "required": []any{"name"}}
	instance := map[string]any{"other": "x"}
	if err := validateAgainstSchema("input", schema, instance); err == nil {
		t.Fatal("expected an error for a missing required field")
	}
}

func TestValidateAgainstSchema_WrongTypeFails(t *testing.T) {
	schema := map[string]any{"type": "object", "properties": map[string]any{"count": map[string]any{"type": "integer"}}}
	instance := map[string]any{"count": "not a number"}
	if err := validateAgainstSchema("input", schema, instance); err == nil {
		t.Fatal("expected an error for a type mismatch")
	}
}

func TestValidateAgainstSchema_EmptySchemaAcceptsAnything(t *testing.T) {
	if err := validateAgainstSchema("input", map[string]any{}, map[string]any{"anything": true}); err != nil {
		t.Fatalf("expected {} to accept any instance: %v", err)
	}
}
