import difflib
import re
from typing import Callable

from sql_ai.athena.clean_sql.cleaning_sql import (
    SQLCleaning,
)
from sql_ai.athena.datatypes_list import (
    athena_allowed_datatypes,
    common_datatype_conversions,
)
from sql_ai.athena.functions_list import (
    allowed_athena_functions,
    athena_datetime_literals,
    common_function_conversions,
)
from sql_ai.athena.table import Table


class SQLAthena(SQLCleaning):
    """Fix SQL queries to Athena specifications. This class is an
    implementation of the SQLCleaning class, and so has the same
    methods controlling the SQL cleaning. Additionally, it has methods
    to fix SQL queries to Athena specifications.
    """

    def __init__(self, nickname: str = "🧐 Ensuring Athena compliance") -> None:
        super().__init__(nickname=nickname)
        self.methods = [
            self._use_show_create_table_for_metadata_query,
            self._clean_aliases,
            self._clean_functions,
            self._clean_from_join,
            self._clean_intervals,
            self._clean_rand,
            self._clean_date_diff,
            self._clean_datetime_literals,
            self._clean_date_sub,
            self._clean_cast,
            self._clean_partially_qualified_columns,
            self._use_table_aliases,
        ]

    def _use_show_create_table_for_metadata_query(self, *args) -> str:
        sql = args[0]
        sql = sql.replace("DESCRIBE ", "SHOW CREATE TABLE ")
        return sql

    def _use_table_aliases(self, sql: str, tables: list[Table]) -> str:
        """
        Replace fully qualified table names with table aliases, provided table
        not being specified in a FROM or JOIN clause.

        :param sql: The SQL query to modify
        :param tables: The list of tables to modify
        :return: The modified SQL query
        """
        table_aliases = []

        for table in tables:
            alias = table.name[0]
            i = 1
            while alias in table_aliases:
                alias = f"{alias}{i}"
                i += 1
            table_aliases.append(alias)

            sql = self._add_table_alias_to_from_or_join_clause(sql, table, alias)
            sql = self._use_table_alias_in_table_reference(sql, table, alias)

        return sql

    @SQLCleaning.auto_log_replacements("Adding table aliases to FROM and JOIN clauses")
    def _add_table_alias_to_from_or_join_clause(self, _, table: Table, alias: str):
        pattern = rf"""
            \b(FROM|JOIN)\s+   # Match FROM or JOIN followed by whitespace
            ({re.escape(table.qualified_name())})  # Match the table name
            (?!\s+(AS\s+)      # If not already followed by:
            (["']?[a-zA-Z_][^.\s]*["']?)
            )
        """

        def replacer(match: re.Match) -> tuple[str, str]:
            before = match.group(0)
            after = f'{before} AS "{alias}" '
            return before, after

        return pattern, replacer

    @SQLCleaning.auto_log_replacements("Adding table aliases to table references")
    def _use_table_alias_in_table_reference(self, _, table: Table, alias: str):

        qualified = re.escape(table.qualified_name())
        pattern = rf"(?<!\bWITH\s)({qualified})\." r'(?P<column>\w+|"[^"]+")'

        def replacer(match: re.Match) -> tuple[str, str]:
            column = match.group("column")
            after = f"{alias}.{column}"
            return match.group(0), after

        return pattern, replacer

    def __clean_table_name(self, m: re.Match, tables: list[Table]) -> str:
        """
        Clean a table name from a regex match, given a list of Table objects.

        The match should be the result of a regex that matches a table name with
        optional catalog and database parts. The table name is "cleaned" by
        looking up the table in the list of Table objects, and replacing the
        input table name with the qualified name of the matched table.

        Args:
            m (re.Match): The regex match
            tables (list[Table]): The list of Table objects to search for the table name

        Returns:
            str: The cleaned table name
        """
        keyword = m.group("keyword")
        catalog = m.group("catalog")
        database = m.group("database")
        database_or_catalog = m.group("database_or_catalog")
        table = m.group("table")

        # If catalog and database are provided (3-part form)
        if catalog and database:
            catalog = catalog.replace('"', "")
            database = database.replace('"', "")
            table = table.replace('"', "")

            matches = [
                t
                for t in tables
                if t.catalog == catalog and t.database == database and t.name == table
            ]

        # If it's a metadata query like FROM "information_schema"."columns",
        # skip rewriting
        elif (
            database_or_catalog
            and database_or_catalog.replace('"', "") == "information_schema"
        ):
            return m.group(0)

        # 2-part or 1-part: need to look up table metadata
        else:
            supposed_table = table.replace('"', "").replace("`", "")
            matches = [t for t in tables if t.name == supposed_table]

        if not matches:
            raise ValueError(f"Table {supposed_table} not found in tables list")

        output = f"{keyword} {matches[0].qualified_name()} "
        return output

    @SQLCleaning.auto_log_replacements("Fully qualifying column names")
    def _clean_partially_qualified_columns(self, _, tables: list):
        pattern = r"""
            (?P<prefix>\s+|\()        # leading space or opening bracket
            (
                # db.table.column
                (?P<db>"[^"]+"|\w+)\.      # quoted or unquoted db
                (?P<table>"[^"]+"|\w+)\.   # quoted or unquoted table
                (?P<column>"[^"]+"|\w+)    # quoted or unquoted column
                (?=[\s,)])  # lookahead to stop at whitespace, comma, closing paren
            |
                # table.column
                (?P<table_only>"[^"]+"|\w+)\.  # quoted or unquoted table
                (?P<column_only>"[^"]+"|\w+)  # quoted or unquoted column
                (?=[\s,)])                  # stop at same boundaries
            )
        """

        def replacer(match: re.Match) -> tuple[str, list[str]]:
            prefix = match.group("prefix") or ""
            table_name = match.group("table") or match.group("table_only")
            column_name = match.group("column") or match.group("column_only")
            if not table_name:
                return match.group(0), match.group(0)

            table_matches = [t for t in tables if t.name == table_name.replace('"', "")]
            if not table_matches:
                return match.group(0), match.group(0)

            table = table_matches[0]
            column = column_name.replace('"', "")
            after = f'{prefix}{table.qualified_name()}."{column}"'
            before = match.group(0).replace("(", "").strip()
            after_custom = after.replace("(", "")
            return before, [after, after_custom]

        return pattern, replacer

    @SQLCleaning.auto_log_replacements(
        "Fully qualifying table names in SHOW CREATE TABLE queries"
    )
    def _clean_show_create_table(self, _, tables: list[Table]) -> tuple[str, Callable]:
        pattern = r"""
            (?P<keyword>SHOW\s+CREATE\s+TABLE)\s+
            (?:
                (?P<catalog>`[^`]+`|\w+)\.
                (?P<database>`[^`]+`|\w+)\.
            |
               (?P<database_or_catalog>`[^`]+`|\w+)\.
            )?
            (?P<table>`[^`]+`|\w+)
            \s*;?
            """

        def replacer(match: re.Match) -> tuple[str, str]:
            after = self.__clean_table_name(match, tables=tables).replace('"', "`")
            return match.group(0), after

        return pattern, replacer

    @SQLCleaning.auto_log_replacements(
        "Fully qualifying table names in FROM and JOIN clauses"
    )
    def _clean_from_join(self, _, tables: list[Table]) -> tuple[str, Callable]:
        """
        Regex to match FROM or JOIN clauses that reference a table,
        using fully-qualified or partially-qualified names:

        Matches:
            FROM "catalog"."database"."table"
            FROM "database"."table"
            FROM "table"

        Doublequotes omitted in the above is also matched.

        Groups:
            keyword - FROM / JOIN
            catalog - optional catalog (if 3-part form)
            database - optional database (if 3-part form)
            database_or_catalog - optional database (if 2-part form)
            table - required table name
        """
        pattern = r"""
            \b(?P<keyword>FROM|JOIN)\s+
            (?:
                (?P<catalog>"[^"]+"|\w+)\.
                (?P<database>"[^"]+"|\w+)\.
            |
               (?P<database_or_catalog>"[^"]+"|\w+)\.
            )?
            (?P<table>"[^"]+"|\w+)
            \s*;?
            """

        def replacer(match: re.Match) -> tuple[str, str]:
            before = match.group(0).strip()
            after = self.__clean_table_name(match, tables=tables)
            return before, after

        return pattern, replacer

    def _clean_rand(self, sql: str, _) -> str:
        sql = self._clean_rand_followed_by_brackets(sql)
        sql = self._clean_stand_alone_rand(sql)
        return sql

    @SQLCleaning.auto_log_replacements(
        "Replacing RAND(<non-number>) with AND (<non-number>)"
    )
    def _clean_rand_followed_by_brackets(self, _) -> tuple[str, Callable]:
        pattern = r"""
            \bRAND\s*         # Match the word RAND with optional spaces
            (\(\s*[^\d\s][^)]*?\s*\))
            # Capturing group for the entire parenthesis expression:
            #   \( — opening parenthesis
            #   \s* — optional spaces
            #   [^\d\s] — first non-digit, non-space character
            #   [^)]*? — rest of the content, non-greedy
            #   \s*\) — optional spaces and closing parenthesis
            """

        def replacer(match: re.Match) -> tuple[str, str]:
            return match.group(0), f"AND {match.group(1)}"

        return pattern, replacer

    @SQLCleaning.auto_log_replacements("Replacing ' RAND ' with ' AND '")
    def _clean_stand_alone_rand(self, _) -> tuple[str, Callable]:
        pattern = r"(?<=\s)RAND(?=\s)"

        def replacer(match: re.Match) -> tuple[str, str]:
            return match.group(0), " AND "

        return pattern, replacer

    @SQLCleaning.auto_log_replacements("Adding quotes around aliases")
    def _clean_aliases(self, *args) -> tuple[str, Callable]:
        datatypes = athena_allowed_datatypes + list(common_datatype_conversions.keys())

        pattern = rf"""
            \bAS\s+                          # AS and whitespace
            (?P<alias>
                (?:
                    "[^"]+"                  # double-quoted
                    |'[^']+'                 # single-quoted
                    |`[^`]+`                 # backtick-quoted
                    |\b(?!{"|".join(datatypes)})\w+\b    # unquoted and not a data type
                )
            )
        """

        def replacer(m: re.Match) -> tuple[str, str]:
            before = m.group(0)
            alias = m.group("alias").strip("\"`'")
            after = f'AS "{alias}"'
            return before, after

        return pattern, replacer

    def _clean_functions(self, sql: str, tables: list[Table]) -> str:
        sql = self._replace_common_function_conversions(sql)
        sql = self._replace_closest_invalid_functions(sql)
        return sql

    @SQLCleaning.auto_log_replacements(
        "Replacing invalid Athena function with known conversion"
    )
    def _replace_common_function_conversions(self, sql: str) -> tuple[str, Callable]:
        pattern = (
            r"\b("
            + "|".join(map(re.escape, common_function_conversions.keys()))
            + r")\s*\("
        )

        def replacer(match: re.Match) -> tuple[str, str]:
            func = match.group(1)
            corrected = common_function_conversions[func]
            after = f"{corrected}("
            return match.group(0), after

        return pattern, replacer

    @SQLCleaning.auto_log_replacements(
        "Replacing invalid Athena function with closest match"
    )
    def _replace_closest_invalid_functions(self, sql: str) -> tuple[str, Callable]:
        function_pattern = r"\b(\w+)\s*\("
        functions = [match.group(1) for match in re.finditer(function_pattern, sql)]
        invalid_funcs = [
            func for func in functions if func.lower() not in allowed_athena_functions
        ]

        if not invalid_funcs:
            return r"$^", lambda m: (m.group(0), m.group(0))  # No replacements

        # Dynamically build a pattern for all closest matches
        patterns = []
        func_map = {}

        for func in invalid_funcs:
            matches = difflib.get_close_matches(
                func.lower(), allowed_athena_functions, n=1, cutoff=0.85
            )
            if matches:
                closest = matches[0].upper()
                patterns.append(re.escape(func))
                func_map[func.lower()] = closest

        if not patterns:
            return r"$^", lambda m: (m.group(0), m.group(0))  # No-ops

        pattern = r"\b(" + "|".join(patterns) + r")\s*\("

        def replacer(match: re.Match) -> tuple[str, str]:
            original = match.group(1)
            corrected = func_map[original.lower()]
            after = f"{corrected}("
            return match.group(0), after

        return pattern, replacer

    @SQLCleaning.auto_log_replacements(
        "Adding quotes around INTERVAL number", flags=re.IGNORECASE
    )
    def _clean_intervals(self, *args) -> tuple[str, Callable]:
        """INTERVAL <number> <unit> -> INTERVAL '<number>' <unit>
        i.e. add single quotes around the number"""
        pattern = r"INTERVAL (\d+) (\w+)"

        def replacer(match: re.Match) -> tuple[str, str]:
            after = f"INTERVAL '{match.group(1)}' {match.group(2)}"
            return match.group(0), after

        return pattern, replacer

    @SQLCleaning.auto_log_replacements(
        "Adding quotes around DATE_DIFF unit", flags=re.IGNORECASE
    )
    def _clean_date_diff(self, *args) -> tuple[str, Callable]:
        """
        Ensures that any DATE_DIFF(unit, ...) calls in the SQL have
        the unit wrapped in single quotes.
        For example: DATE_DIFF(SECOND, → DATE_DIFF('SECOND',
        This version only matches up to the first comma.
        """
        # Match DATE_DIFF(unit, — only up to the first comma
        pattern = r"DATE_DIFF\(\s*([a-zA-Z_']+)\s*,"

        def replacer(match) -> tuple[str, str]:
            before = match.group(0)
            unit = match.group(1)

            if unit.lower() in ["nanosecond", "ns"]:
                unit = "millisecond"

            # If unit is already quoted, do nothing
            if unit.startswith("'") and unit.endswith("'"):
                return before, before
            after = f"DATE_DIFF('{unit}',"
            return before, after

        return pattern, replacer

    @SQLCleaning.auto_log_replacements(
        "Replacing DATE_SUB with DATE_ADD", flags=re.IGNORECASE
    )
    def _clean_date_sub(self, *args) -> tuple[str, Callable]:
        """DATE_SUB(date, <expr with number>) -> DATE_ADD('day', -1 * number, date)"""
        pattern = (
            r"DATE_SUB\("  # Match 'DATE_SUB(' literal
            r"\s*"  # Optional whitespace after opening parenthesis
            r"(?P<date>[^,]+?)"  # Capture group 'date': anything up to the comma
            r"\s*,\s*"  # Comma surrounded by optional whitespace
            r"(?P<expr>"  # Capture group 'expr': begins here
            r".+?"  # Any characters (e.g., INTERVAL keyword)
            r"(?P<number>\d+)"  # Capture group 'number': one or more digits
            r".+?"  # More characters (e.g., DAY)
            r")"  # End of 'expr' group
            r"\s*\)"  # Optional whitespace and closing parenthesis
        )

        def replacer(match: re.Match) -> tuple[str, str]:
            before = match.group(0)
            date = match.group("date").strip()
            number = int(match.group("number").strip())
            after = f"DATE_ADD('day', {-1 * number}, {date})"
            return before, after

        return pattern, replacer

    @SQLCleaning.auto_log_replacements(
        "Removing brackets after datetime literals", flags=re.IGNORECASE
    )
    def _clean_datetime_literals(self, *args) -> tuple[str, Callable]:
        funcs_pattern = "|".join(map(re.escape, athena_datetime_literals))
        pattern = rf"\b({funcs_pattern})\(\)"

        def replacer(match: re.Match) -> tuple[str, str]:
            matched_func = match.group(1)
            return match.group(0), matched_func  # before, after

        return pattern, replacer

    @SQLCleaning.auto_log_replacements(
        "Ensuring cast has a valid datatype conversion and ends with a )"
    )
    def _clean_cast(self, *args) -> tuple[str, Callable]:
        ensure_closing_bracket_pattern = r"""
            CAST  # Match the keyword 'CAST'
            \s*\(  # Optional whitespace, then opening parenthesis
            (?P<col>.*?)  # Lazily capture anything for 'col'
            \s+AS\s+  # Match ' AS ' (required spaces)
            (?P<type>\w+)  # Capture datatype as a single word
            \s*\)  # Match closing parenthesis
        """

        def closing_bracket_replacer(m: re.Match) -> tuple[str, str]:
            col = m.group(1)
            datatype = m.group("type").upper()
            if (
                datatype not in athena_allowed_datatypes
                and datatype in common_datatype_conversions
            ):
                datatype = common_datatype_conversions[datatype]
            after = f"CAST({col} AS {datatype})"
            return m.group(0), after

        return ensure_closing_bracket_pattern, closing_bracket_replacer
