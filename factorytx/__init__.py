import os
import yaml
MODULE_DIR = os.path.dirname(__file__)

PARSERS_DIRS = [os.path.join(MODULE_DIR, 'components/dataplugins/parsers')]
DATAPLUGINS_DIRS = [os.path.join(MODULE_DIR, 'components/dataplugins')]
TRANSFORMS_DIRS = [os.path.join(MODULE_DIR, 'components/transforms')]
TX_DIRS = [os.path.join(MODULE_DIR, 'components/tx')]
FILTERS_DIRS = [os.path.join(MODULE_DIR, 'components/filters')]
TRANSPORTS_DIRS = [os.path.join(MODULE_DIR, 'components/dataplugins/transports')]

R_PARSERS_DIRS = [os.path.join(MODULE_DIR, 'reservecomponents/dataplugins/parsers')]
R_DATAPLUGINS_DIRS = [os.path.join(MODULE_DIR, 'reservecomponents/dataplugins')]
R_TRANSFORMS_DIRS = [os.path.join(MODULE_DIR, 'reservecomponents/transforms')]
R_TX_DIRS = [os.path.join(MODULE_DIR, 'reservecomponents/tx')]
R_FILTERS_DIRS = [os.path.join(MODULE_DIR, 'reservecomponents/filters')]
R_TRANSPORTS_DIRS = [os.path.join(MODULE_DIR, 'reservecomponents/dataplugins/transports')]
