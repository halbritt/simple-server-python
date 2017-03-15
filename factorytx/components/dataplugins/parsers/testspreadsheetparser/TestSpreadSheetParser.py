"""Dummy parser for integration tests.

"""

from FactoryTx.components.dataplugins.parsers.spreadsheetparser import SpreadSheetParser


class TestSpreadSheetParser(SpreadSheetParser):

    def parse_sheet(self, sheet, filename):

        return self.sheet_to_sslogs(sheet)
