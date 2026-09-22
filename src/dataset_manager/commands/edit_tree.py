from __future__ import annotations

from pathlib import Path

import questionary
from nutree import Tree

from ..app import app
from ..log import log
from ..metadata import BOOLEAN_METADATA_FIELDS, METADATA_FIELDS, METADATA_KEY, format_metadata
from ..tree import PathData


def _prompt_field(key: str, label: str, current: object) -> object:
    """Prompt for a single metadata field, defaulting to its current value.

    Boolean fields render as a yes/no confirmation, everything else as a text
    prompt. A `None` answer means the prompt was interrupted, so abort.
    """
    default_text = "" if current is None else str(current)
    if key in BOOLEAN_METADATA_FIELDS:
        answer = questionary.confirm(label, default=bool(current)).ask()
    else:
        answer = questionary.text(label, default=default_text).ask()
    if answer is None:
        raise SystemExit(1)
    return answer


@app.command
def edit_tree(path: Path, /) -> None:
    """Interactively edit the provenance metadata stored in a saved tree.

    Every field is prompted for with its current value as the default. Afterwards
    the edited metadata is shown and you choose whether to overwrite the loaded
    tree (default no); answering no prompts for a new file name instead.

    Args:
        path (Path): Path to the saved tree (expects a json file).
    """
    file_meta: dict = {}
    tree: Tree = Tree.load(path, mapper=PathData.deserialize_mapper, file_meta=file_meta)
    current = dict(file_meta.get(METADATA_KEY) or {})
    if not current:
        log.warning(f"{path} has no metadata; a new header will be created.")

    edited = dict(current)
    for key, label in METADATA_FIELDS:
        answer = _prompt_field(key, label, current.get(key))
        if answer == "":
            # An emptied text field is removed rather than stored as "".
            edited.pop(key, None)
        else:
            edited[key] = answer

    print()
    print(format_metadata(edited) or "No metadata fields set.")
    print()

    if questionary.confirm(f"Overwrite {path}?", default=False).ask():
        target = path
    else:
        name = questionary.text("New tree name:", default=f"{path.stem}-edited").ask()
        if not name:
            raise SystemExit(1)
        target = path.with_name(name) if Path(name).parent == Path(".") else Path(name)
        if target.suffix != ".json":
            target = target.with_suffix(".json")
        if target.exists() and not questionary.confirm(f"{target} already exists. Overwrite?", default=False).ask():
            raise SystemExit(1)

    tree.save(target, mapper=PathData.serialize_mapper, compression=True, meta={METADATA_KEY: edited})
    log.info(f"Saved tree with edited metadata to {target}")
