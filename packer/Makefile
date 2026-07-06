PACKER_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
OUTPUT_DIR := $(PACKER_DIR)output
HTTP_DIR   := $(PACKER_DIR)http
BUILD_PASSWORD ?= packer-build-temp

.PHONY: cidata clean

cidata: $(OUTPUT_DIR)/cidata.iso

$(OUTPUT_DIR)/cidata.iso: $(HTTP_DIR)/user-data $(HTTP_DIR)/meta-data
	@mkdir -p $(OUTPUT_DIR)
	@# Generate password hash and render user-data
	@HASH=$$(echo '$(BUILD_PASSWORD)' | openssl passwd -6 -stdin); \
	sed "s|__BUILD_PASSWORD_HASH__|$$HASH|" $(HTTP_DIR)/user-data > $(OUTPUT_DIR)/user-data
	@cp $(HTTP_DIR)/meta-data $(OUTPUT_DIR)/meta-data
	genisoimage -output $@ -volid cidata -joliet -rock \
		$(OUTPUT_DIR)/user-data $(OUTPUT_DIR)/meta-data
	@rm -f $(OUTPUT_DIR)/user-data $(OUTPUT_DIR)/meta-data

clean:
	rm -f $(OUTPUT_DIR)/cidata.iso
