GO ?= go
CORE := ./internal/core/...
COVER_MIN ?= 90

.PHONY: gate-full gate-fast build fmt-check vet lint fcis test cover tidy

build:
	$(GO) build ./...

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed on:"; echo "$$out"; exit 1; fi

vet:
	$(GO) vet ./...

lint:
	golangci-lint run

# fcischeck is the authoritative core-purity gate: no I/O imports, no clock,
# no randomness inside internal/core.
fcis:
	$(GO) run ./tools/fcischeck $(CORE)

test:
	$(GO) test -race ./...

cover:
	@COVER_MIN=$(COVER_MIN) ./scripts/coverage.sh

# gate-fast — the local edit-hook loop: quick structural feedback for builders.
gate-fast: fmt-check vet fcis test

# gate-full — the authoritative gate CI runs; must be green for a ticket to merge.
gate-full: build fmt-check vet lint fcis test cover

tidy:
	$(GO) mod tidy

# Regenerate gRPC/proto Go from proto/*.proto (requires buf + plugins on PATH).
proto:
	@PATH="$$PATH:$$($(GO) env GOPATH)/bin" buf generate

# ---- P4 local FoundationDB harness (#167). NOT part of gate-full / CI. ----
# The GitHub gate stays hermetic and FDB/CGo-free: default build tags exclude
# the //go:build fdb adapter. This target exercises that adapter against a live
# local single-node fdbserver. Requires FoundationDB clients+server + libfdb.
# See docs/fdb-local.md.
.PHONY: test-fdb install-hooks

test-fdb:
	@command -v fdbcli >/dev/null 2>&1 || { echo "fdbcli not found — install FoundationDB clients+server. See docs/fdb-local.md."; exit 1; }
	@fdbcli --exec "status minimal" 2>/dev/null | grep -qi "database is available" || { echo "local FoundationDB not available — start fdbserver. See docs/fdb-local.md."; exit 1; }
	$(GO) test -tags fdb ./internal/shell/store/... -run FDB -v

install-hooks:
	git config core.hooksPath scripts/hooks
	@echo "git hooks wired (core.hooksPath=scripts/hooks). FDB pre-push enforcement active."
