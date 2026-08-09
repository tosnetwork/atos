package httpadapter

import (
	"context"
	"fmt"
	"net"
	"os"
)

// allowPrivateNetworksEnv is the explicit development/test escape hatch for
// the outbound network policy below. It defaults to production-safe
// (private/loopback/link-local destinations blocked) and must be set
// deliberately, never implied by any other configuration.
const allowPrivateNetworksEnv = "ATOS_ALLOW_PRIVATE_PROVIDER_ENDPOINTS"

// allowPrivateNetworksFromEnv reports the escape hatch's current value.
// Read at dial time (not cached) so tests can toggle it with t.Setenv.
func allowPrivateNetworksFromEnv() bool {
	return os.Getenv(allowPrivateNetworksEnv) == "1"
}

// blockedIP reports whether ip must never be dialed by the outbound
// provider policy: loopback, private (RFC1918/RFC4193), link-local
// (unicast or multicast), unspecified (0.0.0.0/::), or multicast. This is
// deliberately IP-based, not hostname-based -- it runs against the address
// actually being dialed after DNS resolution, so a provider-controlled
// hostname cannot bypass it via DNS rebinding.
func blockedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	switch {
	case ip.IsLoopback(),
		ip.IsPrivate(),
		ip.IsLinkLocalUnicast(),
		ip.IsLinkLocalMulticast(),
		ip.IsInterfaceLocalMulticast(),
		ip.IsMulticast(),
		ip.IsUnspecified():
		return true
	default:
		return false
	}
}

// policyDialContext wraps a base dialer's DialContext with the outbound
// provider-endpoint policy: resolve the target, reject it if any resolved
// address is blocked (unless the explicit escape hatch is set), and dial
// only the accepted address directly -- never re-resolving after the
// check, which would reopen the DNS-rebinding window the IP check exists
// to close.
func policyDialContext(base func(ctx context.Context, network, addr string) (net.Conn, error)) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("httpadapter: invalid dial address %q: %w", addr, err)
		}
		allowPrivate := allowPrivateNetworksFromEnv()
		if ip := net.ParseIP(host); ip != nil {
			if blockedIP(ip) && !allowPrivate {
				return nil, fmt.Errorf("httpadapter: outbound policy rejected destination %s (private/loopback/link-local network)", host)
			}
			return base(ctx, network, addr)
		}
		var resolver net.Resolver
		ips, err := resolver.LookupIP(ctx, "ip", host)
		if err != nil {
			return nil, fmt.Errorf("httpadapter: resolve %q: %w", host, err)
		}
		if len(ips) == 0 {
			return nil, fmt.Errorf("httpadapter: no addresses resolved for %q", host)
		}
		var chosen net.IP
		for _, ip := range ips {
			if blockedIP(ip) && !allowPrivate {
				continue
			}
			chosen = ip
			break
		}
		if chosen == nil {
			return nil, fmt.Errorf("httpadapter: outbound policy rejected every resolved address for %q (private/loopback/link-local network)", host)
		}
		return base(ctx, network, net.JoinHostPort(chosen.String(), port))
	}
}
