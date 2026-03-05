import unittest

from dagster_weather_intelligence_platform.assets.weather_enriched import _extract_json_array


class ExtractJsonArrayTestCase(unittest.TestCase):
    def test_plain_json_array(self) -> None:
        text = '[{"day_utc": "2026-03-03", "label": "clear"}]'
        result = _extract_json_array(text)
        self.assertEqual(1, len(result))
        self.assertEqual("clear", result[0]["label"])

    def test_json_array_wrapped_in_markdown_code_block(self) -> None:
        text = '```json\n[{"day_utc": "2026-03-03", "label": "rainy"}]\n```'
        result = _extract_json_array(text)
        self.assertEqual(1, len(result))
        self.assertEqual("rainy", result[0]["label"])

    def test_json_array_wrapped_in_bare_code_block(self) -> None:
        text = '```\n[{"day_utc": "2026-03-03", "label": "windy"}]\n```'
        result = _extract_json_array(text)
        self.assertEqual(1, len(result))
        self.assertEqual("windy", result[0]["label"])

    def test_json_array_embedded_in_text(self) -> None:
        text = 'Here is the output:\n[{"day_utc": "2026-03-03", "label": "cloudy"}]\nDone.'
        result = _extract_json_array(text)
        self.assertEqual(1, len(result))
        self.assertEqual("cloudy", result[0]["label"])

    def test_raises_on_no_array_found(self) -> None:
        with self.assertRaises(ValueError):
            _extract_json_array("no array here")

    def test_list_of_dicts_input(self) -> None:
        result = _extract_json_array([{"text": '[{"label": "clear"}]'}])
        self.assertEqual(1, len(result))
        self.assertEqual("clear", result[0]["label"])

    def test_multiple_rows(self) -> None:
        text = (
            "```json\n"
            '[{"day_utc": "2026-03-03", "label": "clear"}, '
            '{"day_utc": "2026-03-04", "label": "rainy"}]\n'
            "```"
        )
        result = _extract_json_array(text)
        self.assertEqual(2, len(result))


if __name__ == "__main__":
    unittest.main()
