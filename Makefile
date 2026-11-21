IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
PLATFORM ?= linux/amd64
DEV   ?= dev
FIXTURES ?= fixtures
ANSWER ?= dev-answer
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures run-dev check-dev native native-fixtures

build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

fixtures: build
	python3 scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

check-dev:
	python3 scripts/compare.py out $(ANSWER)

# Local native build without Docker
native:
	mkdir -p build && $(CXX) -O3 -march=native -std=c++17 -pthread -o build/pipeline src/pipeline.cpp

native-fixtures: native
	python3 scripts/run_fixtures.py $(FIXTURES) -- build/pipeline

# SIMD boundary/quoting differential checks, with memory and UB sanitizers.
.PHONY: scanner-check
scanner-check:
	mkdir -p build
	$(CXX) -O1 -g -march=native -std=c++17 -pthread -fsanitize=address,undefined -o build/check-scanner tools/check_scanner.cpp
	ASAN_OPTIONS=detect_leaks=0 UBSAN_OPTIONS=halt_on_error=1 build/check-scanner
