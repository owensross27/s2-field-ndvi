# Source this before any Spark invocation. Spark 3.5 supports Java 8/11/17 only.
# Prefers a system JDK 17; falls back to Homebrew's keg-only openjdk@17 (no sudo).
export JAVA_HOME=$(/usr/libexec/java_home -v 17 2>/dev/null)
if [ -z "$JAVA_HOME" ] && command -v brew >/dev/null; then
  _BREW17="$(brew --prefix openjdk@17 2>/dev/null)/libexec/openjdk.jdk/Contents/Home"
  [ -x "$_BREW17/bin/java" ] && export JAVA_HOME="$_BREW17"
fi
if [ -z "$JAVA_HOME" ]; then
  echo "ERROR: JDK 17 not found. brew install openjdk@17" >&2
  exit 1
fi
export PATH="$JAVA_HOME/bin:$PATH"
# macOS: driver binds to the (often unresolvable) hostname without this
export SPARK_LOCAL_IP=127.0.0.1
# workers must use the same interpreter as the driver (system python is 3.13)
_REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)"
export PYSPARK_PYTHON="$_REPO_DIR/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$_REPO_DIR/.venv/bin/python"

