import os
import tempfile
import unittest

from scripts.parse_spreadsheet import parse_csv


class ParseSpreadsheetTests(unittest.TestCase):
    def _parse_csv(self, content):
        fd, path = tempfile.mkstemp(suffix='.csv')
        os.close(fd)
        try:
            with open(path, 'w', encoding='utf-8') as f:
                f.write(content)
            return parse_csv(path)
        finally:
            os.unlink(path)

    def test_rejects_blank_date_and_blank_value(self):
        rows, meta = self._parse_csv(
            'data;descricao;valor\n'
            ';sem data;10\n'
            '01/01/2024;sem valor;\n'
            '01/02/2024;ok;1,23\n'
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(meta['skippedInvalidDate'], 1)
        self.assertEqual(meta['skippedInvalidValue'], 1)
        self.assertEqual(meta['rejectedRows'][0]['reason'], 'DATA_INVALIDA')
        self.assertEqual(meta['rejectedRows'][1]['reason'], 'VALOR_INVALIDO')

    def test_rejected_row_uses_source_line_number(self):
        rows, meta = self._parse_csv(
            'data;descricao;valor\n'
            '01/01/2024;ok;10\n'
            '32/13/2024;data ruim;50\n'
            '01/02/2024;ok2;20\n'
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(meta['skippedInvalidDate'], 1)
        self.assertEqual(meta['rejectedRows'][0]['row'], 2)


if __name__ == '__main__':
    unittest.main()
