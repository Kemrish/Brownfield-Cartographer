"""Multi-language AST parsing with tree-sitter and LanguageRouter."""

from pathlib import Path
from typing import Optional

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
    "python": {"if_statement", "elif_clause", "for_statement", "while_statement", "try_statement", "except_clause", "with_statement", "match_statement", "case_clause", "and", "or"},
    "javascript": {"if_statement", "for_statement", "while_statement", "do_statement", "switch_statement", "case", "catch_clause", "ternary_expression", "&&", "||"},
    "typescript": {"if_statement", "for_statement", "while_statement", "do_statement", "switch_statement", "case", "catch_clause", "ternary_expression", "&&", "||"},
}


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
                p = tree_sitter.Parser(lang)
                self._parsers[lang_name] = p
            except Exception:
                return None
        return self._parsers.get(lang_name)

    def language_for_path(self, path: str | Path) -> Optional[str]:
        """Return language name for path, or None."""
        ext = Path(path).suffix.lower()
        return EXT_TO_LANG.get(ext)


class TreeSitterAnalyzer:
    """Extract structure from source files using tree-sitter AST."""

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

    def extract_python_structure(self, path: str | Path, source: str) -> dict:
        """Extract imports, public functions, classes, decorators from Python source."""
        b = source.encode("utf-8")
        tree = self.parse_file(path, b)
        if tree is None:
            return {"imports": [], "public_functions": [], "classes": [], "decorators": [], "complexity": 1}

        root = tree.root_node()
        imports: list[str] = []
        public_functions: list[str] = []
        classes: list[str] = []
        decorators: list[str] = []
        complexity = 1  # base complexity

        def visit(node: tree_sitter.Node) -> None:
            nonlocal complexity
            if node.type == "import_statement":
                for child in node.children:
                    if child.type == "dotted_name":
                        imports.append(_get_text(b, child))
                return
            if node.type == "import_from_statement":
                module_name_node = node.child_by_field_name("module_name")
                if module_name_node:
                    imports.append(_get_text(b, module_name_node))
                return
            if node.type == "decorator":
                dec_text = _get_text(b, node).strip().lstrip("@").split("(")[0]
                if dec_text and dec_text not in decorators:
                    decorators.append(dec_text)
            if node.type == "function_definition":
                name_node = node.child_by_field_name("name")
                if name_node and not _get_text(b, name_node).startswith("_"):
                    public_functions.append(_get_text(b, name_node))
            if node.type == "class_definition":
                name_node = node.child_by_field_name("name")
                if name_node and not _get_text(b, name_node).startswith("_"):
                    classes.append(_get_text(b, name_node))
            if node.type in COMPLEXITY_NODES.get("python", set()):
                complexity += 1
            for i in range(node.child_count):
                visit(node.child(i))

        visit(root)
        return {
            "imports": _normalize_imports(imports),
            "public_functions": public_functions,
            "classes": classes,
            "decorators": decorators,
            "complexity": complexity,
        }

    def compute_metrics(self, source: str) -> dict:
        """Compute LOC and comment ratio."""
        lines = source.split("\n")
        total = len(lines)
        comment_lines = sum(1 for l in lines if l.strip().startswith("#") or l.strip().startswith("//") or l.strip().startswith("--"))
        blank_lines = sum(1 for l in lines if not l.strip())
        code_lines = total - comment_lines - blank_lines
        comment_ratio = comment_lines / max(1, code_lines)
        return {
            "lines_of_code": code_lines,
            "comment_ratio": round(comment_ratio, 3),
            "total_lines": total,
        }

    def extract_sql_tables(self, path: str | Path, source: str) -> list[str]:
        """Extract table names from SQL (simple tree-sitter pass). Defer full lineage to sqlglot."""
        b = source.encode("utf-8")
        tree = self.parse_file(path, b)
        if tree is None:
            return []
        tables: list[str] = []
        root = tree.root_node()

        def visit(node: tree_sitter.Node) -> None:
            if node.type in ("identifier", "qualified_identifier", "table_identifier"):
                tables.append(_get_text(b, node).strip("`\"[]"))
            for i in range(node.child_count):
                visit(node.child(i))

        visit(root)
        return list(dict.fromkeys(tables))

    def analyze_module(self, path: str | Path, source: str, language: Optional[str] = None) -> Optional[ModuleNode]:
        """Build a ModuleNode for the file. Python gets full structure; others get minimal."""
        path_str = str(Path(path).as_posix())
        lang = language or self.router.language_for_path(path)
        if not lang:
            lang = "unknown"

        metrics = self.compute_metrics(source)
        is_entry = _is_entry_point(path_str, source)

        if lang == "python":
            structure = self.extract_python_structure(path, source)
            return ModuleNode(
                path=path_str,
                language=lang,
                imports=structure["imports"],
                public_functions=structure["public_functions"],
                classes=structure["classes"],
                decorators=structure.get("decorators", []),
                cyclomatic_complexity=structure.get("complexity", 1),
                lines_of_code=metrics["lines_of_code"],
                comment_ratio=metrics["comment_ratio"],
                is_entry_point=is_entry,
            )
        return ModuleNode(
            path=path_str,
            language=lang,
            lines_of_code=metrics["lines_of_code"],
            comment_ratio=metrics["comment_ratio"],
            is_entry_point=is_entry,
        )


def _get_text(b: bytes, node: tree_sitter.Node) -> str:
    return b[node.start_byte : node.end_byte].decode("utf-8", errors="replace")


def _normalize_imports(imports: list[str]) -> list[str]:
    """Dedupe and clean import names."""
    seen: set[str] = set()
    out: list[str] = []
    for imp in imports:
        imp = imp.strip()
        if imp and imp not in seen:
            seen.add(imp)
            out.append(imp)
    return out


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
