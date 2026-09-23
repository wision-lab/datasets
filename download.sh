#!/usr/bin/env bash
#
# Download a dataset from the public S3 bucket and unpack its archives in place.
#
# Usage: download.sh [DATASET_PREFIX] [DOWNLOAD_DIR]
#
# Unpack on local disk: extracting onto an SMB/Windows share silently corrupts
# names containing `:` (e.g. `mean_frames/0:512.jpg`), leaving zero-byte files.
#
set -euo pipefail

DATASET_PREFIX="${1:-visionsim/visionsim50/frames}"
DOWNLOAD_DIR="${2:-downloads}"
BUCKET="${BUCKET:-public-datasets}"
ENDPOINT="${ENDPOINT:-https://web.s3.wisc.edu}"

for tool in aws 7z; do
  command -v "$tool" >/dev/null || { echo "Missing required tool: $tool" >&2; exit 1; }
done

case "$(realpath -m "$DOWNLOAD_DIR")" in
  /run/user/*/gvfs/* | */smb-share:* | *gvfs*)
    echo "Refusing to unpack on a gvfs/SMB mount ($DOWNLOAD_DIR): ':' in file names would be corrupted." >&2
    echo "Choose a local DOWNLOAD_DIR." >&2
    exit 1
    ;;
esac

# Clone all data from S3
aws s3 sync "s3://$BUCKET/$DATASET_PREFIX" "$DOWNLOAD_DIR" --endpoint="$ENDPOINT" --no-sign-request

# Extract every archive in its own directory. 7z handles the LZMA-compressed
# archives that `unzip` cannot, and `-print0`/`read -d ''` keep names with spaces
# intact.
while IFS= read -r -d '' archive; do
  7z x -y -o"$(dirname "$archive")" "$archive" >/dev/null
  rm -f "$archive"
done < <(
  find "$DOWNLOAD_DIR" -type f \
    \( -name '*.zip' -o -name '*.tar' -o -name '*.tar.gz' -o -name '*.tgz' \
    -o -name '*.tar.bz2' -o -name '*.tar.xz' -o -name '*.7z' \) -print0
)
