package balancer

// MQTT 5.0 user-property keys used to carry the one-hop redirect credential.
//
// The admission controller returns a Server Reference whose query string holds
// the token (see ParseServerRef). A client must never be given that URI as a
// Server Reference: MQTT 5.0 treats the value as a connectable endpoint and a
// query string is not a host. The broker therefore splits the reference into a
// clean endpoint (PropServerReference) plus these three user properties on the
// CONNACK. A cooperating client copies them into its next CONNECT; the target
// node validates them with ValidateRedirectToken and force-accepts the
// connection, which closes the single-hop loop.
const (
	RedirectTokenProperty = "grovemq-redirect-token"
	RedirectNodeProperty  = "grovemq-redirect-node"
	RedirectTimeProperty  = "grovemq-redirect-ts"
)
