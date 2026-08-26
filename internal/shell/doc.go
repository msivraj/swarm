// Package shell is the imperative shell: everything that performs I/O — gossip,
// gRPC, stores, the WASM host, process execution, the clock, randomness. Shell
// packages gather inputs, call the pure cores, and execute the commands they
// return.
//
// Rule: shell packages may import core packages; core packages may never import
// shell. This is enforced by depguard (.golangci.yml) and fcischeck.
package shell
