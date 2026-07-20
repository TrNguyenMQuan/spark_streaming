from pathlib import Path

REPO = Path("target-repo")

# Count total file (use recursion, ignore .git)
# rglob is recursive entire folder return all file match
all_py_files = [file for file in REPO.rglob("*.py") if ".git" not in file.parts]
print(f"Total python files without .git in repo is: {len(all_py_files)}")

# filter test / setup / auto-generated
def is_excluded(p: Path) -> bool:
    name = p.name
    # files test
    if name.startswith("test_") or name.endswith("_test.py"):
        return True

    # folder test
    if any(part in ("tests", "test") for part in p.parts):
        return True

    # config file
    if name in ("setup.py", "conftest.py"):
        return True

    # __init__.py
    if name == "__init__.py":
        return True

    return False

filtered = [file for file in all_py_files if not is_excluded(file)]
excluded = [file for file in all_py_files if is_excluded(file)]

print(f"After filter repo have: {len(filtered)} file")
print(f"Remove: {len(excluded)} file")
