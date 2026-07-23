"""Master Test Runner for Task 6 & Pipeline Verification

Executes all 5 modular testcases sequentially, followed by ground-truth audit,
and prints a unified test report summary.
"""

import sys
from pathlib import Path

# Ensure scripts/tests is in python path
TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TESTS_DIR))

from test_tc1_add_function import run_testcase_1
from test_tc2_add_class import run_testcase_2
from test_tc3_line_shift import run_testcase_3
from test_tc4_exact_replay import run_testcase_4
from test_tc5_call_graph import run_testcase_5
from test_audit_accuracy import run_audit_accuracy


def main():
    print("==========================================================================")
    print("      TASK 6 IDEMPOTENT REPLAY & CODE MUTATION MODULAR TEST SUITE       ")
    print("==========================================================================")

    results = {}

    results["TC1: Add Function"] = run_testcase_1()
    results["TC2: Add Class"] = run_testcase_2()
    results["TC3: Line Shift"] = run_testcase_3()
    results["TC4: Exact Replay"] = run_testcase_4()
    results["TC5: Call Graph"] = run_testcase_5()

    print("\n--------------------------------------------------------------------------")
    print("                     MODULAR TEST SUITE SUMMARY                           ")
    print("--------------------------------------------------------------------------")
    all_passed = True
    for name, passed in results.items():
        status = "PASSED [SUCCESS]" if passed else "FAILED [ERROR]"
        if not passed:
            all_passed = False
        print(f"  {name:<30}: {status}")

    print("--------------------------------------------------------------------------")
    if all_passed:
        print("  ALL 5 TASK 6 TESTCASES PASSED 100% SUCCESSFULLY!\n")
    else:
        print("  SOME TESTCASES FAILED. PLEASE REVIEW LOGS ABOVE.\n")

    print("\nRunning Ground-Truth Accuracy Audit...")
    run_audit_accuracy()


if __name__ == "__main__":
    main()
