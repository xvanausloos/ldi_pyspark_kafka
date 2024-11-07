ifndef STAGE
$(error STAGE is not set)
endif

install-requirements:
	pip install -r requirements.txt

