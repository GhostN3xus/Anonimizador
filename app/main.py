import sys
import logging
from app.config import Config
from app.logging import setup_logging
from app.db import DatabaseConnector
from app.discovery import SensitiveDiscovery
from app.anonymization import Anonymizer
from app.simulation import SimulationEngine
from app.execution import ExecutionEngine

def main():
    setup_logging()
    logger = logging.getLogger("Main")

    print("\n=========================================")
    print("   SENSITIVE DATA ANONYMIZER (LGPD/PCI)")
    print("=========================================")

    # 1. Config & Connect
    print("\n[INIT] Validating configuration...")
    if not Config.validate():
        sys.exit(1)

    db = DatabaseConnector()
    try:
        db.connect()
    except Exception as e:
        logger.critical(f"Connection failed: {e}")
        print(f"Error: {e}")
        sys.exit(1)

    # 2. Discovery
    print("\n[PHASE 1] Discovery & Classification...")
    discovery = SensitiveDiscovery(db)
    sensitive_cols = discovery.scan()

    if not sensitive_cols:
        print("No sensitive columns detected.")
        sys.exit(0)

    print(f"\n[RESULT] Detected {len(sensitive_cols)} sensitive columns:")
    for c in sensitive_cols:
        full_table = f"{c['schema']}.{c['table']}" if c['schema'] else c['table']
        print(f"  - {full_table:<20} | {c['column']:<15} | Type: {c['sensitive_type']:<10} | Conf: {c['confidence']:.2f}")

    # 3. Anonymization Setup & Validation
    print("\n[PHASE 2] Anonymization Logic Validation...")
    anonymizer = Anonymizer()

    # "Exibir as primeiras 100 linhas da tabela DE → PARA"
    print("\n--- SAMPLE MAPPINGS (Existing DE -> PARA) ---")
    mappings = anonymizer.get_mappings(100)
    if mappings:
        print(f"{'ORIGINAL':<30} | {'TYPE':<10} | {'FAKE':<30}")
        print("-" * 75)
        for m in mappings:
            # truncate for display
            orig = (m[0][:27] + '...') if len(m[0]) > 27 else m[0]
            fake = (m[2][:27] + '...') if len(m[2]) > 27 else m[2]
            print(f"{orig:<30} | {m[1]:<10} | {fake:<30}")
    else:
        print("  (No mappings exist yet. They will be generated consistently during execution.)")

    confirm = input("\n[?] Is the sample satisfactory? Abort if not. [y/N]: ")
    if confirm.lower() != 'y':
        print("Aborted by user.")
        sys.exit(0)

    # 4. Simulation
    print("\n[PHASE 3] Simulation (Impact Preview)...")
    simulator = SimulationEngine(db, anonymizer)
    simulator.simulate(sensitive_cols)

    print("\n[WARNING] You are about to PERMANENTLY modify the database.")
    confirm = input("[?] CONFIRM EXECUTION? [y/N]: ")
    if confirm.lower() != 'y':
        print("Aborted by user.")
        sys.exit(0)

    # 5. Execution
    print("\n[PHASE 4] Execution (Applying Changes)...")
    executor = ExecutionEngine(db, anonymizer)
    try:
        executor.execute(sensitive_cols)
        print("\n[SUCCESS] Anonymization completed.")
        print("Check 'audit.log' and 'rollback.csv' for details.")
    except Exception as e:
        print(f"\n[ERROR] Execution failed: {e}")

    anonymizer.close()
    db.close()

if __name__ == "__main__":
    main()
