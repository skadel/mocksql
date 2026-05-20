.PHONY: check-all

check-all:
	$(MAKE) -C back check
	cd front && npm test -- --run --passWithNoTests
