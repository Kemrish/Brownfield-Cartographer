"""Unified analyzer service providing a clean interface for all language analyzers.

This module provides a service layer that orchestrates tree-sitter analyzers,
SQL lineage extraction, and YAML config parsing through a consistent API.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from src.analyzers.tree_sitter_analyzer import (
    TreeSitterAnalyzer,
    PythonStructure,
    SQLStructure,
    YAMLStructure,
    EXT_TO_LANG,
)
from src.models.schemas import ModuleNode


@dataclass
class AnalysisResult:
    """Unified result from analyzing a source file."""

    path: str
    language: str
    module_node: Optional[ModuleNode] = None
    python_structure: Optional[PythonStructure] = None
    sql_structure: Optional[SQLStructure] = None
    yaml_structure: Optional[YAMLStructure] = None
    metrics: dict = None
    parse_success: bool = False
    error: Optional[str] = None

    def __post_init__(self):
        if self.metrics is None:
            self.metrics = {}


class AnalyzerService:
    """Unified service for analyzing source files across languages.
    
    Provides a clean, reusable interface for:
    - Python: imports, functions, classes, decorators, complexity
    - SQL: statement type, tables, CTEs, joins, aggregations, window functions
    - YAML: hierarchical keys, nesting depth, list detection
    
    Usage:
        service = AnalyzerService()
        result = service.analyze_file("models/staging/stg_customers.sql")
        if result.parse_success:
            print(f"Tables referenced: {result.sql_structure.tables_referenced}")
    """

    def __init__(self) -> None:
        self._ts_analyzer = TreeSitterAnalyzer()

    @property
    def supported_extensions(self) -> list[str]:
        """Return list of supported file extensions."""
        return list(EXT_TO_LANG.keys())

    def is_supported(self, path: str | Path) -> bool:
        """Check if file type is supported for analysis."""
        ext = Path(path).suffix.lower()
        return ext in EXT_TO_LANG

    def get_language(self, path: str | Path) -> Optional[str]:
        """Get language name for file, or None if unsupported."""
        return self._ts_analyzer.router.language_for_path(path)

    def analyze_file(self, path: str | Path, source: Optional[str] = None) -> AnalysisResult:
        """Analyze a source file and return comprehensive results.
        
        Args:
            path: Path to the source file
            source: Source code content. If None, reads from path.
            
        Returns:
            AnalysisResult with language-specific structures and metrics.
        """
        path_str = str(Path(path).as_posix())
        language = self.get_language(path) or "unknown"

        # Read source if not provided
        if source is None:
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as f:
                    source = f.read()
            except Exception as e:
                return AnalysisResult(
                    path=path_str,
                    language=language,
                    parse_success=False,
                    error=str(e),
                )

        result = AnalysisResult(
            path=path_str,
            language=language,
        )

        try:
            # Get metrics for any language
            result.metrics = self._ts_analyzer.compute_metrics(source, language)

            # Language-specific deep analysis
            if language == "python":
                result.python_structure = self._ts_analyzer.extract_python_structure(path, source)
                result.parse_success = True
            elif language == "sql":
                result.sql_structure = self._ts_analyzer.extract_sql_structure(path, source)
                result.parse_success = True
            elif language == "yaml":
                result.yaml_structure = self._ts_analyzer.extract_yaml_structure(path, source)
                result.parse_success = True
            elif language in ("javascript", "typescript"):
                # Basic parsing support
                result.parse_success = True
            else:
                result.parse_success = False
                result.error = f"Unsupported language: {language}"

            # Build ModuleNode with all extracted data
            result.module_node = self._ts_analyzer.analyze_module(path, source, language)

        except Exception as e:
            result.parse_success = False
            result.error = str(e)

        return result

    def analyze_python(self, path: str | Path, source: str) -> PythonStructure:
        """Analyze Python source and return structure.
        
        Extracts:
        - imports (with aliases, relative levels, imported names)
        - star imports (from x import *)
        - conditional imports (inside if/try blocks)
        - lazy imports (__import__(), importlib.import_module())
        - public functions
        - classes
        - decorators
        - global constants
        - cyclomatic complexity
        
        The returned PythonStructure has convenience properties:
        - star_imports: modules with "from x import *"
        - conditional_imports: imports inside if/try blocks
        - lazy_imports: modules loaded via __import__() or importlib
        - relative_imports: imports with dot prefixes
        """
        return self._ts_analyzer.extract_python_structure(path, source)

    def analyze_sql(self, path: str | Path, source: str) -> SQLStructure:
        """Analyze SQL source and return structure.
        
        Extracts:
        - statement type (SELECT, INSERT, CREATE, etc.)
        - tables referenced (FROM, JOIN)
        - tables written (INSERT, UPDATE, CREATE)
        - CTEs (WITH clause)
        - joins with types
        - columns selected
        - aggregation detection
        - window function detection
        - subquery count
        - complexity
        """
        return self._ts_analyzer.extract_sql_structure(path, source)

    def analyze_yaml(self, path: str | Path, source: str) -> YAMLStructure:
        """Analyze YAML source and return structure.
        
        Extracts:
        - root keys
        - nested key structure
        - full key paths (e.g., "models.customers.columns")
        - list keys (keys with array values)
        - nesting depth
        - scalar count
        """
        return self._ts_analyzer.extract_yaml_structure(path, source)

    def get_module_node(self, path: str | Path, source: str) -> Optional[ModuleNode]:
        """Build a ModuleNode for the file with language-appropriate data."""
        return self._ts_analyzer.analyze_module(path, source)

    def compute_metrics(self, source: str, language: str = "") -> dict:
        """Compute source code metrics."""
        return self._ts_analyzer.compute_metrics(source, language)


# Singleton instance for convenience
_default_service: Optional[AnalyzerService] = None


def get_analyzer_service() -> AnalyzerService:
    """Get the default analyzer service instance."""
    global _default_service
    if _default_service is None:
        _default_service = AnalyzerService()
    return _default_service
