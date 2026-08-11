package financial

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var issuanceLimitPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]+)?$`)

type BlnkConfig struct {
	BaseURL              string
	APIKey               string
	Timeout              time.Duration
	GenesisIssuanceLimit string
}

type BlnkClient struct {
	baseURL              *url.URL
	apiKey               string
	client               *http.Client
	timeout              time.Duration
	genesisIssuanceLimit json.Number
}

type LedgerTransaction struct {
	TransactionID string      `json:"transaction_id"`
	Source        string      `json:"source"`
	Destination   string      `json:"destination"`
	Reference     string      `json:"reference"`
	PreciseAmount json.Number `json:"precise_amount"`
	Currency      string      `json:"currency"`
	Description   string      `json:"description"`
	Status        string      `json:"status"`
	CreatedAt     time.Time   `json:"created_at"`
}

type ledgerBalance struct {
	Balance   json.Number `json:"balance"`
	BalanceID string      `json:"balance_id"`
	Indicator string      `json:"indicator"`
	Currency  string      `json:"currency"`
}

func NewBlnkClient(config BlnkConfig) (*BlnkClient, error) {
	parsed, err := url.Parse(strings.TrimSpace(config.BaseURL))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("financial: Blnk base URL must be absolute without query or fragment")
	}
	if config.Timeout <= 0 || config.Timeout > 2*time.Minute {
		return nil, errors.New("financial: Blnk timeout must be positive and at most two minutes")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	issuanceLimit := strings.TrimSpace(config.GenesisIssuanceLimit)
	if issuanceLimit == "" {
		issuanceLimit = "0"
	}
	if value, ok := new(big.Rat).SetString(issuanceLimit); !ok || value.Sign() < 0 || !issuanceLimitPattern.MatchString(issuanceLimit) {
		return nil, errors.New("financial: invalid genesis issuance limit")
	}
	return &BlnkClient{baseURL: parsed, apiKey: config.APIKey, client: &http.Client{Timeout: config.Timeout}, timeout: config.Timeout, genesisIssuanceLimit: json.Number(issuanceLimit)}, nil
}

func (c *BlnkClient) request(ctx context.Context, method, path string, input, output any) (int, error) {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(encoded)
	}
	endpoint := *c.baseURL
	endpoint.Path = c.baseURL.Path + path
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("X-Blnk-Key", c.apiKey)
	}
	response, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, 2<<20)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		payload, _ := io.ReadAll(io.LimitReader(limited, 4096))
		return response.StatusCode, fmt.Errorf("financial: Blnk HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(payload)))
	}
	if output != nil {
		decoder := json.NewDecoder(limited)
		decoder.UseNumber()
		if err := decoder.Decode(output); err != nil {
			return response.StatusCode, fmt.Errorf("financial: decode Blnk response: %w", err)
		}
	}
	return response.StatusCode, nil
}

func (c *BlnkClient) Submit(ctx context.Context, event Event, allowOverdraft bool) (LedgerTransaction, error) {
	if allowOverdraft {
		limit, _ := new(big.Rat).SetString(c.genesisIssuanceLimit.String())
		if event.EventType != EventAccountGenesis || limit == nil || limit.Sign() <= 0 {
			return LedgerTransaction{}, errors.New("financial: genesis issuance is disabled or invalid")
		}
	}
	payload := struct {
		TransactionID  string      `json:"transaction_id"`
		PreciseAmount  json.Number `json:"precise_amount"`
		Precision      float64     `json:"precision"`
		AllowOverdraft bool        `json:"allow_overdraft"`
		OverdraftLimit json.Number `json:"overdraft_limit"`
		SkipQueue      bool        `json:"skip_queue"`
		Atomic         bool        `json:"atomic"`
		Source         string      `json:"source"`
		Destination    string      `json:"destination"`
		Reference      string      `json:"reference"`
		Description    string      `json:"description"`
		Currency       string      `json:"currency"`
	}{event.LedgerTransactionID, json.Number(event.AtomicAmount), math.Pow10(event.Decimals),
		allowOverdraft, c.genesisIssuanceLimit, true, true, event.SourceIndicator, event.DestinationIndicator,
		event.LedgerReference, "atos-financial-v1:" + event.Digest, event.Asset}
	var transaction LedgerTransaction
	_, err := c.request(ctx, http.MethodPost, "/transactions", payload, &transaction)
	return transaction, err
}

func (c *BlnkClient) Lookup(ctx context.Context, reference string) (LedgerTransaction, bool, error) {
	var transaction LedgerTransaction
	status, err := c.request(ctx, http.MethodGet, "/transactions/reference/"+url.PathEscape(reference), nil, &transaction)
	if status == http.StatusNotFound {
		return LedgerTransaction{}, false, nil
	}
	if err != nil {
		return LedgerTransaction{}, false, err
	}
	return transaction, true, nil
}

func (c *BlnkClient) balanceByIndicator(ctx context.Context, indicator, currency string) (ledgerBalance, bool, error) {
	var balance ledgerBalance
	status, err := c.request(ctx, http.MethodGet, "/balances/indicator/"+url.PathEscape(indicator)+"/currency/"+url.PathEscape(currency), nil, &balance)
	if status == http.StatusNotFound {
		return ledgerBalance{}, false, nil
	}
	if err != nil {
		return ledgerBalance{}, false, err
	}
	return balance, true, nil
}

func (c *BlnkClient) Balance(ctx context.Context, indicator, currency string) (string, bool, error) {
	balance, found, err := c.balanceByIndicator(ctx, indicator, currency)
	if err != nil || !found {
		return "", found, err
	}
	return balance.Balance.String(), true, nil
}

func (c *BlnkClient) Verify(ctx context.Context, event Event, transaction LedgerTransaction) error {
	if transaction.TransactionID != event.LedgerTransactionID || transaction.Reference != event.LedgerReference ||
		transaction.PreciseAmount.String() != event.AtomicAmount || transaction.Currency != event.Asset ||
		transaction.Description != "atos-financial-v1:"+event.Digest || transaction.Status != "APPLIED" {
		return ErrIdempotencyConflict
	}
	source, found, err := c.balanceByIndicator(ctx, event.SourceIndicator, event.Asset)
	if err != nil || !found {
		return fmt.Errorf("financial: resolve Blnk source: found=%t: %w", found, err)
	}
	destination, found, err := c.balanceByIndicator(ctx, event.DestinationIndicator, event.Asset)
	if err != nil || !found {
		return fmt.Errorf("financial: resolve Blnk destination: found=%t: %w", found, err)
	}
	if transaction.Source != source.BalanceID || transaction.Destination != destination.BalanceID {
		return ErrIdempotencyConflict
	}
	return nil
}

type blnkExternalTransaction struct {
	ID          string    `json:"id"`
	Amount      float64   `json:"amount"`
	Reference   string    `json:"reference"`
	Currency    string    `json:"currency"`
	Description string    `json:"description"`
	Date        time.Time `json:"date"`
	Source      string    `json:"source"`
}

// ReconcileLedger feeds independently reconstructed ATOS ledger evidence into
// Blnk's existing generic reconciliation engine. ATOS has already verified
// precise_amount and immutable balance IDs above; the engine independently
// matches amount/reference/currency/description against its journal.
func (c *BlnkClient) ReconcileLedger(ctx context.Context, events []Event) (string, error) {
	if len(events) == 0 {
		return "", nil
	}
	if len(events) > 10000 {
		return "", errors.New("financial: Blnk reconciliation batch exceeds 10000 events")
	}
	hashInput := strings.Builder{}
	transactions := make([]blnkExternalTransaction, len(events))
	for i, event := range events {
		hashInput.WriteString(event.Digest)
		hashInput.WriteByte(0)
		atomicAmount, ok := new(big.Int).SetString(event.AtomicAmount, 10)
		if !ok {
			return "", errors.New("financial: invalid reconciliation amount")
		}
		scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(event.Decimals)), nil)
		amount, _ := new(big.Rat).SetFrac(atomicAmount, scale).Float64()
		transactions[i] = blnkExternalTransaction{
			ID: "atos_" + strings.TrimPrefix(event.Digest, "sha256:"), Amount: amount,
			Reference: event.LedgerReference, Currency: event.Asset,
			Description: "atos-financial-v1:" + event.Digest,
			Date:        time.UnixMilli(event.OccurredUnixMillis).UTC(), Source: "atos-financial-v1",
		}
	}
	reconciliationID, err := stableID("recon_atos_", "tos.atos.blnk-reconciliation-id.v1", hashInput.String())
	if err != nil {
		return "", err
	}
	payload := struct {
		ReconciliationID     string                    `json:"reconciliation_id"`
		ExternalTransactions []blnkExternalTransaction `json:"external_transactions"`
		Strategy             string                    `json:"strategy"`
		DryRun               bool                      `json:"dry_run"`
		MatchingRuleIDs      []string                  `json:"matching_rule_ids"`
	}{reconciliationID, transactions, "one_to_one", true, []string{"atos_financial_integrity_v1"}}
	var started struct {
		ReconciliationID string `json:"reconciliation_id"`
	}
	status, err := c.request(ctx, http.MethodPost, "/reconciliation/start-instant", payload, &started)
	if err != nil {
		if status == http.StatusConflict || status == http.StatusBadRequest {
			return "", err
		}
		return "", errors.Join(ErrLedgerUncertain, err)
	}
	if started.ReconciliationID != reconciliationID {
		return "", errors.New("financial: Blnk reconciliation identity substitution")
	}
	deadline := time.Now().Add(c.timeout)
	for time.Now().Before(deadline) {
		var result struct {
			ReconciliationID      string `json:"reconciliation_id"`
			Status                string `json:"status"`
			MatchedTransactions   int    `json:"matched_transactions"`
			UnmatchedTransactions int    `json:"unmatched_transactions"`
		}
		status, err = c.request(ctx, http.MethodGet, "/reconciliation/"+url.PathEscape(reconciliationID), nil, &result)
		if err != nil {
			if status >= 500 || status == 0 {
				return "", errors.Join(ErrLedgerUncertain, err)
			}
			return "", err
		}
		switch result.Status {
		case "completed":
			if result.ReconciliationID != reconciliationID || result.MatchedTransactions != len(events) || result.UnmatchedTransactions != 0 {
				return "", errors.New("financial: Blnk reconciliation mismatch")
			}
			return reconciliationID, nil
		case "failed":
			return "", errors.New("financial: Blnk reconciliation failed")
		}
		select {
		case <-ctx.Done():
			return "", errors.Join(ErrLedgerUncertain, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	return "", errors.Join(ErrLedgerUncertain, errors.New("financial: Blnk reconciliation timed out"))
}
