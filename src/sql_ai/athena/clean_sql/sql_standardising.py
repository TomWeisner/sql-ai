import re
from typing import Callable, Optional

from sql_ai.athena.clean_sql.cleaning_sql import (
    SQLCleaning,
)


class SQLStandards(SQLCleaning):
    """Standardise SQL queries. Styling, spacing, etc."""

    def __init__(self, nickname: str = "🤓 Standardising styling") -> None:
        super().__init__(nickname=nickname)
        self.methods = [
            self.replace_plain_join_with_inner_join,
            self.pad_keywords_basic,
            self.pad_ands_in_between_clauses,
            self.align_fields,
        ]

    @SQLCleaning.auto_log_replacements("Replacing JOIN with INNER JOIN")
    def replace_plain_join_with_inner_join(self, *args) -> tuple[str, Callable]:
        # Match 'JOIN' not preceded by INNER, LEFT, RIGHT, FULL, OUTER, or CROSS
        pattern = r"""
            (?<!\bINNER\s)      # not preceded by 'INNER '
            (?<!\bLEFT\s)       # not preceded by 'LEFT '
            (?<!\bRIGHT\s)      # not preceded by 'RIGHT '
            (?<!\bFULL\s)       # not preceded by 'FULL '
            (?<!\bOUTER\s)      # not preceded by 'OUTER '
            (?<!\bCROSS\s)      # not preceded by 'CROSS '
            \bJOIN\b            # match the standalone word 'JOIN'
            """

        def replacer(match: re.Match) -> tuple[str, str]:
            return match.group(0), "INNER JOIN"

        return pattern, replacer

    @SQLCleaning.auto_log_replacements(
        "Align fields in SELECT / GROUP BY / ORDER BY blocks"
    )
    def align_fields(self, *args) -> tuple[str, Callable]:
        target_clauses = ["SELECT", "GROUP BY", "ORDER BY"]
        stop_clauses = [
            "FROM",
            "WHERE",
            "GROUP BY",
            "ORDER BY",
            "HAVING",
            "LIMIT",
            "UNION",
            "EXCEPT",
            "INTERSECT",
        ]
        indent = 5
        spaces = " " * indent

        def build_clause_pattern(clauses: list[str]) -> str:
            return "|".join(
                (rf"(?<!\w){re.escape(c)}(?!\w)" if " " in c else rf"\b{re.escape(c)}\b")
                for c in clauses
            )

        clause_pattern = build_clause_pattern(target_clauses)
        lookahead_pattern = build_clause_pattern(stop_clauses)

        # Pattern: match clause body up to next clause or end
        pattern = rf"(?i)({clause_pattern})(.*?)(?=\s*({lookahead_pattern})|$)"

        def split_top_level_commas(text: str) -> str:
            result = []
            buffer = ""
            paren_level = 0
            for char in text:
                if char in "([":
                    paren_level += 1
                elif char in ")]":
                    paren_level = max(paren_level - 1, 0)

                if char == "," and paren_level == 0:
                    result.append(buffer.strip())
                    buffer = ""
                else:
                    buffer += char

            if buffer.strip():
                result.append(buffer.strip())

            return f"\n{spaces}, ".join(result)

        def replacer_func(match: re.Match) -> tuple[Optional[str], str]:
            clause = match.group(1).upper()
            body = match.group(2).strip()

            if not body:
                return match.group(0), match.group(0)

            formatted = split_top_level_commas(body)
            after = f"{clause} {formatted}"
            return None, after

        return pattern, replacer_func

    @SQLCleaning.auto_log_replacements("Ensure KEYWORD indents")
    def pad_keywords_basic(self, *args) -> tuple[str, Callable]:
        clause_keywords = [
            # "ORDER BY",
            # "GROUP BY",
            "CASE WHEN",
            "THEN",
            "ELSE",
            "END",
            "INNER JOIN",
            "LEFT JOIN",
            "CROSS JOIN",
            "WITH",
            "SELECT",
            "FROM",
            "ON",
            "BETWEEN",
            "WHERE",
            "AND",
            "GROUP BY",
            "ORDER BY",
            "HAVING",
            "LIMIT",
        ]

        clause_keyword_spacing = {
            kw: (
                13
                if kw == "END"
                else (
                    12
                    if kw in ["THEN", "ELSE"]
                    else (7 if kw in ["CASE WHEN", "BETWEEN"] else 6 - len(kw.split()[0]))
                )
            )
            for kw in clause_keywords
        }

        # Sort long keywords first to avoid partial overlaps
        # (e.g. "JOIN" inside "INNER JOIN")
        # clause_keywords.sort(key=len, reverse=True)

        # Create a pattern that matches the clause keywords as whole words
        clause_pattern = "|".join(
            rf"(?<![\w\"]){re.escape(kw)}(?![\w\"])" for kw in clause_keywords
        )

        # Final pattern that safely matches clauses and their contents
        pattern = rf"(\s*)({clause_pattern})(.*?)(?=\s*({clause_pattern})|$)"

        def replacer(m: re.Match) -> tuple[str, list[str]]:
            keyword = m.group(2).upper()
            content = m.group(3).strip()
            new_line = "\n"
            if m.start() == 0:
                new_line = ""
            num_spaces = clause_keyword_spacing[keyword]
            after = f"{new_line}{' ' * num_spaces}{keyword} {content}"
            after_display = f"{clause_keyword_spacing[keyword]} spaces"
            return keyword, [after, after_display]

        return pattern, replacer

    @SQLCleaning.auto_log_replacements("Padding ANDs in BETWEEN clauses")
    def pad_ands_in_between_clauses(self, *args) -> tuple[str, Callable]:
        # Match pattern: anything before, then BETWEEN <expr1> AND <expr2>
        pattern = r"(.*?)\bBETWEEN\b\s+(.*?)\s+\bAND\b\s+(.*?)"

        def replacer(match: re.Match) -> tuple[str, list[str]]:
            # Capture BETWEEN <expr1> AND <expr2>
            lhs = match.group(1)
            expr1 = match.group(2)
            expr2 = match.group(3)
            indent = 7
            after = f"{lhs}BETWEEN {expr1}\n{' ' * indent}AND {expr2}"
            after_display = f"{indent} spaces"
            return "AND", [after, after_display]

        return pattern, replacer
