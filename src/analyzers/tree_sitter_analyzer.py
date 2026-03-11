"""Multi-language AST parsing with tree-sitter, sqlglot, and PyYAML.

Provides deep structural extraction for:
- Python: tree-sitter AST (imports, functions, classes, decorators, complexity)
- SQL: sqlglot AST (statement type, tables, CTEs, joins, aggregations, window functions)
- YAML: PyYAML (hierarchical keys, nesting depth, list detection)
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml
import sqlglot
from sqlglot import exp
import tree_sitter
from tree_sitter_languages import get_parser, get_language

from src.models.schemas import ModuleNode


# Map file extension to tree-sitter language name
EXT_TO_LANG = {
    ".py": "python",
    ".sql": "sql",
    ".yml": "yaml",
    ".yaml": "yaml",
    ".js": "javascript",
    ".ts": "typescript",
    ".jsx": "javascript",
    ".tsx": "typescript",
}

# Node types that contribute to cyclomatic complexity
COMPLEXITY_NODES = {
    "python": {"if_statement", "elif_clause", "for_statement", "while_statement", "try_statement", "except_clause", "with_statement", "match_statement", "case_clause", "and", "or", "conditional_expression"},
    "javascript": {"if_statement", "for_statement", "while_statement", "do_statement", "switch_statement", "case", "catch_clause", "ternary_expression"},
    "typescript": {"if_statement", "for_statement", "while_statement", "do_statement", "switch_statement", "case", "catch_clause", "ternary_expression"},
    "sql": {"case_expression", "when_clause"},
}


# --- Data Classes for Extracted Structures ---


@dataclass
class PythonImport:
    """Represents a Python import statement."""
    module: str
    alias: Optional[str] = None
    is_relative: bool = False
    relative_level: int = 0  # number of dots for relative imports
    imported_names: list[str] = field(default_factory=list)  # for "from x import a, b, c"
    is_star_import: bool = False  # for "from x import *"
    is_conditional: bool = False  # import inside if/try block
    is_lazy: bool = False  # __import__() or importlib calls


@dataclass
class PythonStructure:
    """Extracted structure from a Python file."""
    imports: list[PythonImport] = field(default_factory=list)
    import_strings: list[str] = field(default_factory=list)  # simplified for ModuleNode
    functions: list[str] = field(default_factory=list)
    classes: list[str] = field(default_factory=list)
    decorators: list[str] = field(default_factory=list)
    global_variables: list[str] = field(default_factory=list)
    complexity: int = 1

    @property
    def star_imports(self) -> list[str]:
        """Return list of modules with star imports."""
        return [imp.module for imp in self.imports if imp.is_star_import]

    @property
    def conditional_imports(self) -> list[str]:
        """Return list of conditionally imported modules."""
        return [imp.module for imp in self.imports if imp.is_conditional]

    @property
    def lazy_imports(self) -> list[str]:
        """Return list of lazily imported modules."""
        return [imp.module for imp in self.imports if imp.is_lazy]

    @property
    def relative_imports(self) -> list[PythonImport]:
        """Return list of relative imports."""
        return [imp for imp in self.imports if imp.is_relative]


@dataclass
class SQLStructure:
    """Extracted structure from a SQL file."""
    statement_type: str = "unknown"  # SELECT, INSERT, CREATE, etc.
    tables_referenced: list[str] = field(default_factory=list)
    tables_written: list[str] = field(default_factory=list)
    ctes: list[str] = field(default_factory=list)  # WITH clause names
    columns_selected: list[str] = field(default_factory=list)
    joins: list[dict] = field(default_factory=list)  # [{type: "LEFT JOIN", table: "x"}]
    subqueries: int = 0
    has_aggregation: bool = False
    has_window_function: bool = False
    complexity: int = 1


@dataclass
class YAMLStructure:
    """Extracted structure from a YAML file."""
    root_keys: list[str] = field(default_factory=list)
    nested_keys: dict[str, list[str]] = field(default_factory=dict)  # {parent: [children]}
    key_paths: list[str] = field(default_factory=list)  # ["models.customers.columns"]
    list_keys: list[str] = field(default_factory=list)  # keys that have list values
    scalar_count: int = 0
    depth: int = 0


# --- Abstract Analyzer Interface ---


class LanguageAnalyzer(ABC):
    """Abstract base class for language-specific analyzers."""

    @abstractmethod
    def analyze(self, source: str, path: str) -> Any:
        """Analyze source code and return extracted structure."""
        pass

    @abstractmethod
    def get_complexity(self, source: str, path: str) -> int:
        """Calculate cyclomatic complexity or equivalent."""
        pass


# --- Language Router ---


class LanguageRouter:
    """Selects the correct tree-sitter grammar based on file extension."""

    def __init__(self) -> None:
        self._parsers: dict[str, tree_sitter.Parser] = {}
        self._langs: dict[str, tree_sitter.Language] = {}

    def get_parser(self, path: str | Path) -> Optional[tree_sitter.Parser]:
        """Return a parser for the file at path, or None if unsupported."""
        ext = Path(path).suffix.lower()
        lang_name = EXT_TO_LANG.get(ext)
        if not lang_name:
            return None
        if lang_name not in self._parsers:
            try:
                lang = get_language(lang_name)
                if lang is None:
                    return None
                self._langs[lang_name] = lang
                # tree-sitter 0.21 API: create parser and set language
                p = tree_sitter.Parser()
                p.set_language(lang)
                self._parsers[lang_name] = p
            except Exception:
                return None
        return self._parsers.get(lang_name)

    def get_language(self, path: str | Path) -> Optional[tree_sitter.Language]:
        """Return the tree-sitter Language object for the file."""
        ext = Path(path).suffix.lower()
        lang_name = EXT_TO_LANG.get(ext)
        if not lang_name:
            return None
        if lang_name not in self._langs:
            self.get_parser(path)  # This will populate _langs
        return self._langs.get(lang_name)

    def language_for_path(self, path: str | Path) -> Optional[str]:
        """Return language name for path, or None."""
        ext = Path(path).suffix.lower()
        return EXT_TO_LANG.get(ext)


# --- Main Analyzer ---


class TreeSitterAnalyzer:
    """Extract structure from source files using tree-sitter AST.
    
    Provides deep structural extraction for:
    - Python: imports (with aliases, relative levels), functions, classes, decorators
    - SQL: statement type, tables, CTEs, joins, aggregations, window functions
    - YAML: hierarchical keys, nested structure, list detection
    """

    def __init__(self) -> None:
        self.router = LanguageRouter()

    def parse_file(self, path: str | Path, source_bytes: bytes) -> Optional[tree_sitter.Tree]:
        """Parse file content and return AST, or None if unsupported/unparseable."""
        parser = self.router.get_parser(path)
        if parser is None:
            return None
        try:
            return parser.parse(source_bytes)
        except Exception:
            return None

    # --- Python Analysis ---

    def extract_python_structure(self, path: str | Path, source: str) -> PythonStructure:
        """Extract detailed structure from Python source using AST.
        
        Handles tricky import patterns:
        - Regular imports with aliases (import x as y)
        - From imports with relative levels (from ..x import y)
        - Star imports (from x import *)
        - Conditional imports (inside if/try blocks)
        - Lazy imports (__import__(), importlib.import_module())
        """
        b = source.encode("utf-8")
        tree = self.parse_file(path, b)
        result = PythonStructure()

        if tree is None:
            return result

        root = tree.root_node

        def _is_conditional_context(node: tree_sitter.Node) -> bool:
            """Check if node is inside an if/try/except block."""
            parent = node.parent
            while parent:
                if parent.type in ("if_statement", "try_statement", "except_clause", "with_statement"):
                    return True
                parent = parent.parent
            return False

        def visit(node: tree_sitter.Node) -> None:
            is_conditional = _is_conditional_context(node)

            # Import statements: import x, import x.y, import x as y
            if node.type == "import_statement":
                for child in node.children:
                    if child.type == "dotted_name":
                        module = _get_text(b, child)
                        result.imports.append(PythonImport(
                            module=module,
                            is_conditional=is_conditional,
                        ))
                        result.import_strings.append(module)
                    elif child.type == "aliased_import":
                        name_node = child.child_by_field_name("name")
                        alias_node = child.child_by_field_name("alias")
                        if name_node:
                            module = _get_text(b, name_node)
                            alias = _get_text(b, alias_node) if alias_node else None
                            result.imports.append(PythonImport(
                                module=module,
                                alias=alias,
                                is_conditional=is_conditional,
                            ))
                            result.import_strings.append(module)
                return

            # From imports: from x import y, from ..x import y, from x import *
            if node.type == "import_from_statement":
                module_node = node.child_by_field_name("module_name")
                module = _get_text(b, module_node) if module_node else ""
                
                # Count relative level (dots)
                relative_level = 0
                for child in node.children:
                    if child.type == "import_prefix":
                        relative_level = _get_text(b, child).count(".")
                        break
                    elif child.type == "relative_import":
                        prefix = _get_text(b, child)
                        relative_level = prefix.count(".")

                is_relative = relative_level > 0 or (module and module.startswith("."))
                if module.startswith("."):
                    relative_level = len(module) - len(module.lstrip("."))
                    module = module.lstrip(".")

                # Check for star import
                is_star = False
                imported_names: list[str] = []
                for child in node.children:
                    if child.type == "wildcard_import":
                        is_star = True
                    elif child.type == "dotted_name" and child != module_node:
                        imported_names.append(_get_text(b, child))
                    elif child.type == "aliased_import":
                        name_node = child.child_by_field_name("name")
                        if name_node:
                            imported_names.append(_get_text(b, name_node))

                imp = PythonImport(
                    module=module,
                    is_relative=is_relative,
                    relative_level=relative_level,
                    imported_names=imported_names,
                    is_star_import=is_star,
                    is_conditional=is_conditional,
                )
                result.imports.append(imp)
                if module:
                    result.import_strings.append(module)
                return

            # Lazy imports: __import__('module') or importlib.import_module('module')
            if node.type == "call":
                func_node = node.child_by_field_name("function")
                if func_node:
                    func_text = _get_text(b, func_node)
                    if func_text in ("__import__", "importlib.import_module", "import_module"):
                        # Extract module name from first argument
                        args_node = node.child_by_field_name("arguments")
                        if args_node:
                            for arg_child in args_node.children:
                                if arg_child.type == "string":
                                    module_name = _get_text(b, arg_child).strip("'\"")
                                    if module_name:
                                        result.imports.append(PythonImport(
                                            module=module_name,
                                            is_lazy=True,
                                            is_conditional=is_conditional,
                                        ))
                                        result.import_strings.append(module_name)
                                    break

            # Decorators
            if node.type == "decorator":
                dec_text = _get_text(b, node).strip().lstrip("@").split("(")[0].split("\n")[0]
                if dec_text and dec_text not in result.decorators:
                    result.decorators.append(dec_text)

            # Functions
            if node.type == "function_definition":
                name_node = node.child_by_field_name("name")
                if name_node:
                    name = _get_text(b, name_node)
                    if not name.startswith("_"):
                        result.functions.append(name)

            # Classes
            if node.type == "class_definition":
                name_node = node.child_by_field_name("name")
                if name_node:
                    name = _get_text(b, name_node)
                    if not name.startswith("_"):
                        result.classes.append(name)

            # Global variable assignments (top-level only)
            if node.type == "expression_statement" and node.parent and node.parent.type == "module":
                for child in node.children:
                    if child.type == "assignment":
                        left = child.child_by_field_name("left")
                        if left and left.type == "identifier":
                            var_name = _get_text(b, left)
                            if var_name.isupper():  # Constants
                                result.global_variables.append(var_name)

            # Complexity
            if node.type in COMPLEXITY_NODES.get("python", set()):
                result.complexity += 1

            for i in range(node.child_count):
                visit(node.child(i))

        visit(root)
        return result

    # --- SQL Analysis (using sqlglot) ---

    def extract_sql_structure(self, path: str | Path, source: str) -> SQLStructure:
        """Extract detailed structure from SQL source using sqlglot AST.
        
        Extracts:
        - Statement type (SELECT, INSERT, CREATE, etc.)
        - Tables referenced (FROM, JOIN)
        - Tables written (INSERT, UPDATE, CREATE)
        - CTEs (WITH clause)
        - Joins with types
        - Columns selected
        - Aggregation functions
        - Window functions
        - Subquery count
        - Complexity
        """
        result = SQLStructure()

        # Strip Jinja templates for parsing
        clean_sql = _strip_jinja_for_sqlglot(source)

        try:
            statements = sqlglot.parse(clean_sql, error_level=sqlglot.ErrorLevel.IGNORE)
        except Exception:
            return result

        for stmt in statements:
            if stmt is None:
                continue

            # Determine statement type
            if isinstance(stmt, exp.Select):
                result.statement_type = "SELECT"
            elif isinstance(stmt, exp.Insert):
                result.statement_type = "INSERT"
            elif isinstance(stmt, exp.Update):
                result.statement_type = "UPDATE"
            elif isinstance(stmt, exp.Delete):
                result.statement_type = "DELETE"
            elif isinstance(stmt, exp.Create):
                result.statement_type = "CREATE"
            elif isinstance(stmt, exp.Drop):
                result.statement_type = "DROP"
            elif isinstance(stmt, exp.Alter):
                result.statement_type = "ALTER"

            # Extract CTEs
            for cte in stmt.find_all(exp.CTE):
                alias = cte.alias
                if alias and alias not in result.ctes:
                    result.ctes.append(alias)

            # Extract tables from FROM clause
            for table in stmt.find_all(exp.Table):
                table_name = table.name
                if table_name:
                    # Check if it's a source (FROM) or target (INSERT/UPDATE)
                    parent = table.parent
                    is_write_target = isinstance(parent, (exp.Insert, exp.Update, exp.Create))
                    
                    if is_write_target:
                        if table_name not in result.tables_written and table_name not in result.ctes:
                            result.tables_written.append(table_name)
                    else:
                        if table_name not in result.tables_referenced and table_name not in result.ctes:
                            result.tables_referenced.append(table_name)

            # Extract joins
            for join in stmt.find_all(exp.Join):
                join_type = "JOIN"
                if join.args.get("side"):
                    join_type = f"{join.args['side'].upper()} JOIN"
                if join.args.get("kind"):
                    join_type = f"{join.args['kind'].upper()} {join_type}"
                
                # Get joined table
                join_table_expr = join.this
                if isinstance(join_table_expr, exp.Table):
                    join_table = join_table_expr.name
                    if join_table:
                        result.joins.append({"type": join_type, "table": join_table})
                        if join_table not in result.tables_referenced:
                            result.tables_referenced.append(join_table)

            # Extract columns from SELECT
            for select in stmt.find_all(exp.Select):
                for expr in select.expressions:
                    if isinstance(expr, exp.Column):
                        col_name = expr.name
                        if col_name and col_name not in result.columns_selected:
                            result.columns_selected.append(col_name)
                    elif isinstance(expr, exp.Alias):
                        alias_name = expr.alias
                        if alias_name and alias_name not in result.columns_selected:
                            result.columns_selected.append(alias_name)

            # Detect aggregation functions
            agg_funcs = {"COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", "ARRAY_AGG", "STRING_AGG", "LISTAGG"}
            for func in stmt.find_all(exp.Func):
                func_name = func.name.upper() if hasattr(func, "name") else type(func).__name__.upper()
                if func_name in agg_funcs or any(agg in func_name for agg in agg_funcs):
                    result.has_aggregation = True
                    break

            # Detect window functions
            if stmt.find(exp.Window):
                result.has_window_function = True

            # Count subqueries
            for subq in stmt.find_all(exp.Subquery):
                result.subqueries += 1

            # Complexity from CASE expressions
            for case in stmt.find_all(exp.Case):
                result.complexity += 1
                # Each WHEN adds complexity
                for when in case.args.get("ifs", []):
                    result.complexity += 1

        return result

    # --- YAML Analysis (using PyYAML) ---

    def extract_yaml_structure(self, path: str | Path, source: str) -> YAMLStructure:
        """Extract detailed structure from YAML source using PyYAML.
        
        Extracts:
        - Root keys
        - Nested key structure
        - Full key paths (e.g., "models.customers.columns")
        - List keys (keys with array values)
        - Nesting depth
        - Scalar count
        """
        result = YAMLStructure()

        try:
            # Parse YAML (use safe_load to avoid security issues)
            data = yaml.safe_load(source)
        except Exception:
            return result

        if data is None or not isinstance(data, dict):
            return result

        def traverse(obj: Any, current_path: list[str], depth: int) -> None:
            """Recursively traverse YAML structure to extract keys."""
            result.depth = max(result.depth, depth)

            if isinstance(obj, dict):
                for key, value in obj.items():
                    key_str = str(key)
                    new_path = current_path + [key_str]
                    result.key_paths.append(".".join(new_path))

                    # Track root keys
                    if depth == 1:
                        result.root_keys.append(key_str)

                    # Track nested structure
                    if current_path:
                        parent = current_path[-1]
                        if parent not in result.nested_keys:
                            result.nested_keys[parent] = []
                        if key_str not in result.nested_keys[parent]:
                            result.nested_keys[parent].append(key_str)

                    # Check if value is a list
                    if isinstance(value, list):
                        if key_str not in result.list_keys:
                            result.list_keys.append(key_str)
                        # Traverse list items
                        for item in value:
                            if isinstance(item, dict):
                                traverse(item, new_path, depth + 1)
                            else:
                                result.scalar_count += 1
                    elif isinstance(value, dict):
                        traverse(value, new_path, depth + 1)
                    else:
                        result.scalar_count += 1
            elif isinstance(obj, list):
                for item in obj:
                    if isinstance(item, dict):
                        traverse(item, current_path, depth)
                    else:
                        result.scalar_count += 1

        traverse(data, [], 0)
        return result

    # --- Metrics ---

    def compute_metrics(self, source: str, language: str = "") -> dict:
        """Compute LOC, comment ratio, and language-specific metrics."""
        lines = source.split("\n")
        total = len(lines)

        # Language-specific comment detection
        if language == "python":
            comment_chars = ("#",)
        elif language == "sql":
            comment_chars = ("--", "/*")
        elif language in ("yaml", "yml"):
            comment_chars = ("#",)
        elif language in ("javascript", "typescript"):
            comment_chars = ("//", "/*")
        else:
            comment_chars = ("#", "//", "--")

        comment_lines = sum(1 for l in lines if any(l.strip().startswith(c) for c in comment_chars))
        blank_lines = sum(1 for l in lines if not l.strip())
        code_lines = total - comment_lines - blank_lines
        comment_ratio = comment_lines / max(1, code_lines)

        return {
            "lines_of_code": code_lines,
            "comment_lines": comment_lines,
            "blank_lines": blank_lines,
            "comment_ratio": round(comment_ratio, 3),
            "total_lines": total,
        }

    # --- Module Analysis ---

    def analyze_module(self, path: str | Path, source: str, language: Optional[str] = None) -> Optional[ModuleNode]:
        """Build a ModuleNode with deep structural analysis for all supported languages."""
        path_str = str(Path(path).as_posix())
        lang = language or self.router.language_for_path(path)
        if not lang:
            lang = "unknown"

        metrics = self.compute_metrics(source, lang)
        is_entry = _is_entry_point(path_str, source)

        if lang == "python":
            structure = self.extract_python_structure(path, source)
            return ModuleNode(
                path=path_str,
                language=lang,
                imports=structure.import_strings,
                public_functions=structure.functions,
                classes=structure.classes,
                decorators=structure.decorators,
                cyclomatic_complexity=structure.complexity,
                lines_of_code=metrics["lines_of_code"],
                comment_ratio=metrics["comment_ratio"],
                is_entry_point=is_entry,
            )

        if lang == "sql":
            structure = self.extract_sql_structure(path, source)
            return ModuleNode(
                path=path_str,
                language=lang,
                cyclomatic_complexity=structure.complexity,
                lines_of_code=metrics["lines_of_code"],
                comment_ratio=metrics["comment_ratio"],
                is_entry_point=False,
                sql_statement_type=structure.statement_type,
                sql_tables_referenced=structure.tables_referenced,
                sql_tables_written=structure.tables_written,
                sql_ctes=structure.ctes,
                sql_joins=structure.joins,
                sql_has_aggregation=structure.has_aggregation,
                sql_has_window_function=structure.has_window_function,
                sql_subquery_count=structure.subqueries,
            )

        if lang == "yaml":
            structure = self.extract_yaml_structure(path, source)
            return ModuleNode(
                path=path_str,
                language=lang,
                lines_of_code=metrics["lines_of_code"],
                comment_ratio=metrics["comment_ratio"],
                is_entry_point=False,
                yaml_root_keys=structure.root_keys,
                yaml_key_paths=structure.key_paths,
                yaml_depth=structure.depth,
                yaml_list_keys=structure.list_keys,
            )

        return ModuleNode(
            path=path_str,
            language=lang,
            lines_of_code=metrics["lines_of_code"],
            comment_ratio=metrics["comment_ratio"],
            is_entry_point=is_entry,
        )


# --- Helper Functions ---


def _get_text(b: bytes, node: tree_sitter.Node) -> str:
    """Extract text from AST node."""
    return b[node.start_byte : node.end_byte].decode("utf-8", errors="replace")


def _is_entry_point(path: str, source: str) -> bool:
    """Heuristic: detect if file is an entry point (main, cli, app, etc.)."""
    path_lower = path.lower()
    if any(x in path_lower for x in ("__main__", "main.py", "cli.py", "app.py", "manage.py", "wsgi.py", "asgi.py")):
        return True
    if 'if __name__ == "__main__"' in source or "if __name__ == '__main__'" in source:
        return True
    if "@click.command" in source or "@app.route" in source or "def main(" in source:
        return True
    return False


import re

# Jinja pattern matching for dbt templates
_DBT_REF_PATTERN = re.compile(r"\{\{\s*ref\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
_DBT_SOURCE_PATTERN = re.compile(r"\{\{\s*source\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
_DBT_CONFIG_PATTERN = re.compile(r"\{\{\s*config\s*\([^)]*\)\s*\}\}")
_DBT_VAR_PATTERN = re.compile(r"\{\{\s*var\s*\(\s*['\"]([^'\"]+)['\"]\s*[^)]*\)\s*\}\}")
_JINJA_BLOCK_PATTERN = re.compile(r"\{[%#].*?[%#]\}", re.DOTALL)
_JINJA_EXPR_PATTERN = re.compile(r"\{\{.*?\}\}", re.DOTALL)


def _strip_jinja_for_sqlglot(sql: str) -> str:
    """Replace Jinja templates with SQL-parseable placeholders.
    
    This allows sqlglot to parse the SQL structure even when Jinja
    templates are present (common in dbt projects).
    """
    result = sql

    # Replace ref() calls with placeholder table names
    counter = [0]
    def ref_replacer(m: re.Match) -> str:
        counter[0] += 1
        return f"__ref_table_{counter[0]}__"
    result = _DBT_REF_PATTERN.sub(ref_replacer, result)

    # Replace source() calls with placeholder table names
    def source_replacer(m: re.Match) -> str:
        counter[0] += 1
        return f"__source_table_{counter[0]}__"
    result = _DBT_SOURCE_PATTERN.sub(source_replacer, result)

    # Remove config() blocks
    result = _DBT_CONFIG_PATTERN.sub("", result)

    # Replace var() with placeholder values
    result = _DBT_VAR_PATTERN.sub("'__var__'", result)

    # Remove Jinja block tags ({% ... %} and {# ... #})
    result = _JINJA_BLOCK_PATTERN.sub("", result)

    # Replace remaining Jinja expressions with placeholder
    result = _JINJA_EXPR_PATTERN.sub("'__jinja__'", result)

    return result
