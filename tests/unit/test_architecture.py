"""The dependency rules of the codebase, checked on every module's imports.

Application logic (adapter, consumer) reaches Snowflake, Kafka and PostgreSQL
only through the ports it declares; each integration lives in one package and
is the only code that imports its client library; the composition roots wire
everything together.
"""

import ast
from pathlib import Path

from tests.helpers import REPO_ROOT

PACKAGE_DIR = REPO_ROOT / "src" / "switch_pipeline"
ROOT = "switch_pipeline"

ERRORS = f"{ROOT}.errors"
SETTINGS = f"{ROOT}.settings"
OBSERVABILITY = f"{ROOT}.observability"
RETRY = f"{ROOT}.retry"
LIFECYCLE = f"{ROOT}.lifecycle"
DOMAIN = f"{ROOT}.domain"
QUALITY = f"{ROOT}.quality"
ADAPTER_PORTS = (f"{ROOT}.adapter.ports", f"{ROOT}.adapter.cursor")

# Module (or package) prefix -> the project modules it may import. The longest
# matching prefix applies.
LAYERS: dict[str, tuple[str, ...]] = {
    # Shared kernel.
    ERRORS: (),
    SETTINGS: (),
    RETRY: (),
    LIFECYCLE: (),
    OBSERVABILITY: (SETTINGS,),
    # Domain: the change event, quarantine and log vocabulary, and the rules.
    DOMAIN: (DOMAIN,),
    QUALITY: (DOMAIN, QUALITY),
    # Application logic.
    f"{ROOT}.adapter": (
        *ADAPTER_PORTS,
        f"{ROOT}.adapter.mapper",
        DOMAIN,
        ERRORS,
        OBSERVABILITY,
        RETRY,
        LIFECYCLE,
    ),
    f"{ROOT}.consumer": (
        f"{ROOT}.consumer.ports",
        f"{ROOT}.consumer.processor",
        f"{ROOT}.transport.codec",  # the wire format, a pure module
        DOMAIN,
        QUALITY,
        ERRORS,
        OBSERVABILITY,
        RETRY,
        LIFECYCLE,
        SETTINGS,
    ),
    # Integrations, which implement the ports.
    f"{ROOT}.adapter.snowflake": (*ADAPTER_PORTS, ERRORS, OBSERVABILITY, SETTINGS),
    f"{ROOT}.transport": (f"{ROOT}.transport", DOMAIN, ERRORS, OBSERVABILITY, RETRY, SETTINGS),
    f"{ROOT}.sink": (f"{ROOT}.sink", *ADAPTER_PORTS, DOMAIN, ERRORS, OBSERVABILITY, SETTINGS),
    f"{ROOT}.api": (
        f"{ROOT}.api",
        f"{ROOT}.sink.reads",
        f"{ROOT}.transport.lag",
        DOMAIN,
        ERRORS,
        OBSERVABILITY,
        SETTINGS,
    ),
}

# Wiring and operator entry points may import anything.
COMPOSITION_ROOTS = (
    f"{ROOT}.__main__",
    f"{ROOT}.cli",
    f"{ROOT}.healthcheck",
    f"{ROOT}.adapter.main",
    f"{ROOT}.consumer.main",
    f"{ROOT}.api.main",
    f"{ROOT}.tools",
)

# Client library -> the only code allowed to import it.
LIBRARY_OWNERS: dict[str, tuple[str, ...]] = {
    "snowflake": (f"{ROOT}.adapter.snowflake", f"{ROOT}.tools"),
    "confluent_kafka": (f"{ROOT}.transport",),
    "psycopg": (f"{ROOT}.sink",),
    "psycopg_pool": (f"{ROOT}.sink",),
    "fastapi": (f"{ROOT}.api",),
    "starlette": (f"{ROOT}.api",),
    "uvicorn": (f"{ROOT}.api",),
}


def module_name(path: Path) -> str:
    parts = path.relative_to(PACKAGE_DIR.parent).with_suffix("").parts
    return ".".join(parts[:-1] if parts[-1] == "__init__" else parts)


def is_module(name: str) -> bool:
    relative = Path(*name.split("."))
    return (PACKAGE_DIR.parent / relative).with_suffix(".py").exists() or (
        PACKAGE_DIR.parent / relative / "__init__.py"
    ).exists()


def imports_of(path: Path) -> set[str]:
    """Imported modules; ``from package import module`` counts as the module."""
    found: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, f"{path}: use absolute imports"
            assert node.module is not None
            submodules = {
                f"{node.module}.{alias.name}"
                for alias in node.names
                if is_module(f"{node.module}.{alias.name}")
            }
            found.update(submodules or {node.module})
    return found


def within(name: str, prefix: str) -> bool:
    return name == prefix or name.startswith(f"{prefix}.")


def layer_of(module: str) -> str | None:
    matches = [prefix for prefix in [*LAYERS, *COMPOSITION_ROOTS] if within(module, prefix)]
    return max(matches, key=len) if matches else None


MODULES = {module_name(path): imports_of(path) for path in sorted(PACKAGE_DIR.rglob("*.py"))}


def test_every_module_belongs_to_a_layer() -> None:
    unclassified = [module for module in MODULES if module != ROOT and layer_of(module) is None]
    assert not unclassified, f"add these modules to LAYERS or COMPOSITION_ROOTS: {unclassified}"


def test_modules_import_only_what_their_layer_allows() -> None:
    violations = []
    for module, imported in MODULES.items():
        layer = layer_of(module)
        if layer is None or layer in COMPOSITION_ROOTS:
            continue
        for name in sorted(imported):
            # The package root only carries __version__.
            if within(name, ROOT) and name != ROOT:
                allowed = LAYERS[layer]
                if not any(within(name, prefix) for prefix in allowed):
                    violations.append(f"{module} imports {name}")
    assert not violations, "\n".join(violations)


def test_each_client_library_is_imported_only_by_its_integration() -> None:
    violations = []
    for module, imported in MODULES.items():
        for name in sorted(imported):
            library = name.partition(".")[0]
            owners = LIBRARY_OWNERS.get(library)
            if owners is not None and not any(within(module, owner) for owner in owners):
                violations.append(f"{module} imports {name}")
    assert not violations, "\n".join(violations)
