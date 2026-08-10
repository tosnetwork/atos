package service

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/tosnetwork/atos/internal/domain"
)

// schemaResourceURL is a fixed, arbitrary resource identifier for
// compiling one Capability schema document. A fresh jsonschema.Compiler is
// created per validation call (see validateSchemaDocument), so there is no
// cross-call collision risk from reusing this constant.
const schemaResourceURL = "urn:atos:capability-schema"

// validateSchemaDocument proves raw is itself a syntactically and
// semantically valid JSON Schema document -- not that some instance
// conforms to it, but that the document a provider is asking ATOS to
// store as a Capability's input_schema/output_schema could ever validate
// anything at all. name is used only to make the returned error legible
// ("input_schema" or "output_schema").
func validateSchemaDocument(name string, raw map[string]any) error {
	if raw == nil {
		return fmt.Errorf("%s is required", name)
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(schemaResourceURL, doc); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if _, err := compiler.Compile(schemaResourceURL); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

// validateCapabilitySchemas validates both of a Capability's schema
// documents together, wrapping any failure as a domain.ErrValidationFailed
// so callers get the same error shape every other Register/Update
// validation failure already produces. It is called BEFORE persistence in
// both Register and Update, so a schema-invalid request never reaches the
// store -- no partial Capability, no version bump, no changed manifest
// commitment.
func validateCapabilitySchemas(inputSchema, outputSchema map[string]any) error {
	if err := validateSchemaDocument("input_schema", inputSchema); err != nil {
		return domain.NewError(domain.ErrValidationFailed, "invalid input_schema: "+err.Error(), false)
	}
	if err := validateSchemaDocument("output_schema", outputSchema); err != nil {
		return domain.NewError(domain.ErrValidationFailed, "invalid output_schema: "+err.Error(), false)
	}
	return nil
}
