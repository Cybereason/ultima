#!/bin/bash

set -eux -o pipefail

# Log some general info about the environment
uname -a
env | sort

# print some general information about our python environment
python -c "import sys, struct, ssl; print('#' * 70); print('python:', sys.version); print('version_info:', sys.version_info); print('bits:', struct.calcsize('P') * 8); print('openssl:', ssl.OPENSSL_VERSION, ssl.OPENSSL_VERSION_INFO); print('#' * 70)"

#
# build and install the library
#

# this is needed to make sure "git describe" is completely accurate under github action workflow
# see https://github.com/actions/checkout/issues/290#issuecomment-680260080
git fetch --tags --force
python -m pip install -U 'quicklib>=2.3'
quicklib-setup sdist --formats=zip
python -m pip install dist/*.zip

#
# run the tests
#

rm -rf workdir && mkdir workdir && cd workdir
INSTALL_DIR=$(python -c "import os, ultima; print(os.path.dirname(ultima.__file__))")
pip install -r ../test_requirements.txt
pytest -r a --verbose ${INSTALL_DIR} --cov=ultima --cov-report html:cov_html --cov-report term ${PYTEST_ARGS:-}
if (which zip); then
  zip -r cov_html.zip cov_html
fi
