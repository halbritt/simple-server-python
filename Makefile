.PHONY: all ci-tests clean exec install run test

VENV = ./venv
VIRTUALENV_FLAGS = "--no-site-packages"
REQUIREMENTS = requirements.txt
# Use `make REQUIREMENTS+=requirements-optional.txt` to use optional pip deps.

TARGET = $(VENV)/.t
ENTRYPOINT_INIS = $(shell find FactoryTx -name '*.ini')

all: $(TARGET)/entrypoints $(TARGET)/requirements

ci-tests: all
	$(VENV)/bin/nosetests tests --with-xunit --with-coverage --cover-xml --cover-erase --verbose

clean:
	rm -fr $(VENV)

exec: all
	. $(VENV)/bin/activate && $(CMD)

install: $(TARGET)/requirements  # provides entrypoints
	$(VENV)/bin/python setup.py install
	test -d $(TARGET) || mkdir $(TARGET) ; touch $(TARGET)/entrypoints

run: all
	$(VENV)/bin/factorytx

test: all
	$(VENV)/bin/nosetests tests

$(TARGET)/virtualenv:
	virtualenv $(VIRTUALENV_FLAGS) $(VENV)
	test -d $(TARGET) || mkdir $(TARGET) ; touch $@

$(TARGET)/requirements: $(TARGET)/virtualenv $(REQUIREMENTS)
	$(VENV)/bin/pip install $(shell grep numpy requirements.txt)  # Required for pandas wheel.
	$(VENV)/bin/pip install $(foreach req,$(REQUIREMENTS),-r $(req))
	test -d $(TARGET) || mkdir $(TARGET) ; touch $@

$(TARGET)/entrypoints: $(TARGET)/requirements setup.py $(ENTRYPOINT_INIS)
	$(VENV)/bin/python setup.py develop
	test -d $(TARGET) || mkdir $(TARGET) ; touch $@
