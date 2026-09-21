from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

from natsort import natsort_key
from nutree import SkipBranch, Tree

from ..app import app
from ..sizes import _bytes_to_str
from ..tree import PathData

# Map Unicode box-drawing tree connector characters (╰──, ├──, │, …) to their
# HTML numeric character references, so trees can be pasted directly inside a
# <details> tag without relying on the HTML file's encoding.
_BOX_DRAWING_HTML_TABLE = str.maketrans(
    {
        "\u2500": "&#x2500;",  # ─
        "\u2502": "&#x2502;",  # │
        "\u250c": "&#x250c;",  # ┌
        "\u2510": "&#x2510;",  # ┐
        "\u2514": "&#x2514;",  # └
        "\u2518": "&#x2518;",  # ┘
        "\u251c": "&#x251c;",  # ├
        "\u2570": "&#x2570;",  # ╰
    }
)


@app.command
def show_tree(
    path: Path,
    /,
    full: bool = False,
    s3_prefix: str | None = None,
    html: bool = False,
) -> None:
    """Print out (zip) tree given it's path

    Args:
        path (Path): Path to saved tree (expects a json file).
        full (bool, optional): If true, also print contents of zip files.
        s3_prefix (str, optional): S3 key prefix for generating download links.
            When provided, zip file names are rendered as hyperlinks
            pointing to https://web.s3.wisc.edu/public-datasets/<s3_prefix>/<zip_path>.
        html (bool, optional): If true, render links as HTML <a> tags instead of
            markdown. Useful when the output is used inside a <details> tag in
            a README where markdown links are not rendered.
    """
    tree: Tree = Tree.load(path, mapper=PathData.deserialize_mapper)
    tree.name = path.name

    def format_node(node) -> str:
        """Format a node, optionally with a link for zip files."""
        if s3_prefix and isinstance(node.data, PathData) and node.data.is_zip:
            # Build the S3 key by collecting path components from root to this node
            parts = []
            current = node
            while current is not None:
                if isinstance(current.data, PathData):
                    parts.append(current.data.path.name)
                try:
                    current = current.up()
                except ValueError:
                    current = None
            parts.reverse()
            # Join all path components to form the relative S3 key, then
            # percent-encode it (keeping "/" separators) so names containing
            # spaces, "#", "?" etc. produce valid, clickable URLs.
            zip_rel_path = "/".join(parts)
            url = "https://web.s3.wisc.edu/public-datasets/" + quote(f"{s3_prefix}/{zip_rel_path}", safe="/")
            icon = "💾"
            size = _bytes_to_str(node.data.size or 0)
            size += f" {(node.data.size or 0) / node.data.zip_size:.1f}x" if node.data.zip_size else ""
            if html:
                return f'{icon} <a href="{url}">{node.data.path.name}</a> ({size})'
            return f"{icon} [{node.data.path.name}]({url}) ({size})"
        return str(node.data)

    if not full:
        slim_tree = tree.filtered(lambda n: SkipBranch(and_self=False) if n.data.is_zip else True)
        slim_tree.name = tree.name
        slim_tree.sort(key=lambda n: natsort_key(n.name), deep=True)
        output = slim_tree.format(repr="{node.data}" if not s3_prefix else format_node)
    else:
        tree.sort(key=lambda n: natsort_key(n.name), deep=True)
        output = tree.format(repr="{node.data}" if not s3_prefix else format_node)

    if html:
        # HTML-escape the Unicode box-drawing tree connector characters (╰──,
        # ├──, │) so they display correctly when the output is pasted inside a
        # <details> tag. The <a href> links rendered by `format_node` are left
        # untouched so they stay clickable.
        output = output.translate(_BOX_DRAWING_HTML_TABLE)
        # Wrap in a <pre> block to preserve line breaks and monospace layout;
        # inside a <details> tag, markdown code blocks are not rendered.
        output = f"<pre>\n{output}\n</pre>"

    print(output)
