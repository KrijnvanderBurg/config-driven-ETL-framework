import logging
import subprocess
import argparse
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_changed_files() -> list[Path]:
    result = subprocess.run(
        ["git", "diff", "--name-only", "HEAD", "origin/main"],
        capture_output=True,
        text=True,
    )
    
    files: list[Path] = []
    for file in result.stdout.strip().splitlines():
        files.append(Path(file))
    return files

def find_test_files(changed_files: list[Path], tests_path: Path) -> list[Path]:
    logger.info(f"Changed files: {changed_files}")
    test_files: list[Path] = []
    
    for file in changed_files:
        if file.suffix == ".py":  # Only check Python files
            # Remove the monorepo structure from the file path, aka <repo>/src/<package>.
            full_path = Path(tests_path, *file.parts[3:])

            # Check for test_ prefix and _test suffix
            test_prefix = Path(full_path.parent, f"test_{full_path.stem}").with_suffix(".py")
            test_suffix = Path(full_path.parent, f"{full_path.stem}_test").with_suffix(".py")

            if test_prefix.exists():
                logger.info(f"Found: {str(test_prefix)}")
                test_files.append(test_prefix)

            if test_suffix.exists():
                logger.info(f"Found: {str(test_suffix)}")
                test_files.append(test_suffix)

    return test_files

def main(tests_path: Path) -> None:
    changed_files = get_changed_files()
    test_files = find_test_files(changed_files=changed_files, tests_path=tests_path)
    logger.info(f"##vso[task.setvariable variable=testFiles]{" ".join(map(str, test_files))}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find relevant test files based on `git diff`.")
    parser.add_argument('--tests-path', required=True, help="Path to the tests folder in the repository.")
    args = parser.parse_args()

    tests_path = Path(args.tests_path)

    main(tests_path=tests_path)
