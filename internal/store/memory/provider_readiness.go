package memory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

func healthCheckKey(capabilityID, capabilityVersion string, transport domain.EndpointAdapterType) string {
	return capabilityID + ":" + capabilityVersion + ":" + string(transport)
}

func (s *Store) PutHealthCheck(ctx context.Context, check domain.AdapterHealthCheck) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healthChecks[healthCheckKey(check.CapabilityID, check.CapabilityVersion, check.Transport)] = check
	return nil
}

func (s *Store) HealthCheck(ctx context.Context, capabilityID, capabilityVersion string, transport domain.EndpointAdapterType) (domain.AdapterHealthCheck, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	check, ok := s.healthChecks[healthCheckKey(capabilityID, capabilityVersion, transport)]
	return check, ok, nil
}

// certificationContentHash summarizes the identity fields that must never
// change once a certification is opened -- mirrors disputeContentHash's
// role for domain.Dispute.
func certificationContentHash(c domain.SandboxCertification) string {
	encoded, _ := json.Marshal(struct {
		ProviderID, CapabilityID, CapabilityVersion string
		Transport                                   domain.EndpointAdapterType
		EndpointRef, IdempotencyKey                 string
	}{
		c.ProviderID, c.CapabilityID, c.CapabilityVersion, c.Transport, c.EndpointRef, c.IdempotencyKey,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func certIdempotencyKey(providerID, key string) string {
	return providerID + ":" + key
}

func (s *Store) OpenCertification(ctx context.Context, providerID string, cert domain.SandboxCertification) (domain.SandboxCertification, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idemKey := certIdempotencyKey(providerID, cert.IdempotencyKey)
	if existingID, ok := s.certByIdempotencyKey[idemKey]; ok {
		existing := s.certifications[existingID]
		if certificationContentHash(existing) != certificationContentHash(cert) {
			return domain.SandboxCertification{}, false, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with different certification content", false)
		}
		return existing, false, nil
	}
	s.certifications[cert.ID] = cert
	s.certByIdempotencyKey[idemKey] = cert.ID
	return cert, true, nil
}

func (s *Store) GetCertification(ctx context.Context, id string) (domain.SandboxCertification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.certifications[id]
	if !ok {
		return domain.SandboxCertification{}, store.ErrNotFound
	}
	return c, nil
}

func (s *Store) CertificationByIdempotencyKey(ctx context.Context, providerID, key string) (domain.SandboxCertification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.certByIdempotencyKey[certIdempotencyKey(providerID, key)]
	if !ok {
		return domain.SandboxCertification{}, store.ErrNotFound
	}
	return s.certifications[id], nil
}

func (s *Store) CertificationsByCapability(ctx context.Context, capabilityID string) ([]domain.SandboxCertification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.SandboxCertification
	for _, c := range s.certifications {
		if c.CapabilityID == capabilityID {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (s *Store) UpdateCertification(ctx context.Context, id string, fn func(domain.SandboxCertification, bool) (domain.SandboxCertification, error)) (domain.SandboxCertification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.certifications[id]
	next, err := fn(current, exists)
	if err != nil {
		return domain.SandboxCertification{}, err
	}
	if exists {
		if next.ID != current.ID {
			return domain.SandboxCertification{}, domain.NewError(domain.ErrIdempotencyConflict, "certification update must not change the certification id", false)
		}
		if certificationContentHash(current) != certificationContentHash(next) {
			return domain.SandboxCertification{}, domain.NewError(domain.ErrIdempotencyConflict, "certification update must not change identity fields", false)
		}
	}
	s.certifications[id] = next
	return next, nil
}
