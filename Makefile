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
