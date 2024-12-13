# -*- coding: utf-8 -*-

import argparse
from os.path import join
from iaea.orbs.utils import logger


class HelpFormatter(
    argparse.RawTextHelpFormatter,
    argparse.ArgumentDefaultsHelpFormatter
):
    """
    A custom help formatter class that combines RawTextHelpFormatter and
    ArgumentDefaultsHelpFormatter
    """
