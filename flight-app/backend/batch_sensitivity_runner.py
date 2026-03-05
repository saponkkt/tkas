"""
Batch runner for sensitivity_tow_pipeline.py (format like run_all_pipelines.py)

Simple approach: Define all commands in the COMMANDS list, then run them sequentially.

Usage:
    python batch_sensitivity_runner.py

Just edit the COMMANDS list below with your sensitivity runs!
"""

import subprocess
import sys
import os
import time
from pathlib import Path

# Configure your commands here (like run_all_pipelines.py)
# Format: ['python', 'sensitivity_tow_pipeline.py', input_folder, output_folder, deviation_percent, aircraft_type]
COMMANDS = [    
    # -10% sensitivity - DMK-HDY
    ['python', 'sensitivity_tow_pipeline.py', r'C:\Users\User\Desktop\Validation_TAS\CSV\Sensitivity\DMK-HDY', r'C:\Users\User\Desktop\Validation_TAS\TAS\Sensitivity\DMK-HDY\-10%', '-10', '737'],

    # -10% sensitivity - DMK-PHS
    ['python', 'sensitivity_tow_pipeline.py', r'C:\Users\User\Desktop\Validation_TAS\CSV\Sensitivity\DMK-PHS', r'C:\Users\User\Desktop\Validation_TAS\TAS\Sensitivity\DMK-PHS\-10%', '-10', '737'],

    # -10% sensitivity - HDY-DMK
    ['python', 'sensitivity_tow_pipeline.py', r'C:\Users\User\Desktop\Validation_TAS\CSV\Sensitivity\HDY-DMK', r'C:\Users\User\Desktop\Validation_TAS\TAS\Sensitivity\HDY-DMK\-10%', '-10', '737'],

    # -10% sensitivity - PHS-DMK
    ['python', 'sensitivity_tow_pipeline.py', r'C:\Users\User\Desktop\Validation_TAS\CSV\Sensitivity\PHS-DMK', r'C:\Users\User\Desktop\Validation_TAS\TAS\Sensitivity\PHS-DMK\-10%', '-10', '737'],

    # -10% sensitivity - HDY-UTH
    ['python', 'sensitivity_tow_pipeline.py', r'C:\Users\User\Desktop\Validation_TAS\CSV\Sensitivity\HDY-UTH', r'C:\Users\User\Desktop\Validation_TAS\TAS\Sensitivity\HDY-UTH\-10%', '-10', '737'],

    # -10% sensitivity - UTH-HDY
    ['python', 'sensitivity_tow_pipeline.py', r'C:\Users\User\Desktop\Validation_TAS\CSV\Sensitivity\UTH-HDY', r'C:\Users\User\Desktop\Validation_TAS\TAS\Sensitivity\UTH-HDY\-10%', '-10', '737'],
]


def shutdown_pc(delay_seconds: int = 60):
    """Shutdown the PC after specified delay (in seconds).
    
    Args:
        delay_seconds: Seconds to wait before shutdown. Default 60 seconds.
    """
    print(f"\n{'=' * 80}")
    print(f"🔴 SHUTTING DOWN PC IN {delay_seconds} SECONDS")
    print(f"{'=' * 80}")
    print(f"Press Ctrl+C to cancel shutdown...")
    
    try:
        for i in range(delay_seconds, 0, -1):
            print(f"\rShutting down in {i:3d} seconds... ", end='', flush=True)
            time.sleep(1)
        
        print(f"\n\n⏹️  EXECUTING SHUTDOWN...")
        
        # Windows shutdown command (delay already handled by our countdown)
        os.system('shutdown /s /t 0')
        
    except KeyboardInterrupt:
        print("\n\n✅ Shutdown cancelled by user")
        sys.exit(0)


def main():
    """Run all commands sequentially."""
    
    if not COMMANDS:
        print("╔" + "═" * 78 + "╗")
        print("║ ERROR: No commands configured in COMMANDS list" + " " * 31 + "║")
        print("║ Edit batch_sensitivity_runner.py and add your sensitivity_tow_pipeline.py" + " " * 3 + "║")
        print("║ commands in the COMMANDS list" + " " * 49 + "║")
        print("╚" + "═" * 78 + "╝")
        sys.exit(1)
    
    print("╔" + "═" * 78 + "╗")
    print("║ BATCH SENSITIVITY RUNNER" + " " * 54 + "║")
    print("╠" + "═" * 78 + "╣")
    print(f"║ Total commands to run: {len(COMMANDS):<51}║")
    print("╚" + "═" * 78 + "╝")
    
    successful = []
    failed = []
    
    for i, cmd in enumerate(COMMANDS, 1):
        print(f"\n{'=' * 80}")
        print(f"Running command {i}/{len(COMMANDS)}...")
        print(f"{'=' * 80}")
        
        # Extract parameters from command for display
        if len(cmd) >= 4:
            input_path = Path(cmd[2])
            output_path = Path(cmd[3])
            deviation = cmd[4] if len(cmd) > 4 else "0"
            aircraft = cmd[5] if len(cmd) > 5 else "737"
            
            print(f"Input:     {input_path}")
            print(f"Output:    {output_path}")
            print(f"Deviation: {deviation}%")
            print(f"Aircraft:  {aircraft}")
        
        print(f"\nCommand: {' '.join(cmd)}\n")
        
        try:
            result = subprocess.run(cmd, check=True, cwd=Path(__file__).parent)
            
            status = f"✅ Command {i} completed successfully"
            print(f"\n{status}")
            successful.append(i)
            
        except subprocess.CalledProcessError as e:
            status = f"❌ Command {i} failed with exit code {e.returncode}"
            print(f"\n{status}")
            failed.append((i, f"Exit code {e.returncode}"))
            
            # Option: Stop on first error (like run_all_pipelines.py)
            # Uncomment the line below to stop on error
            # sys.exit(1)
            
        except Exception as e:
            status = f"❌ Command {i} error: {e}"
            print(f"\n{status}")
            failed.append((i, str(e)))
    
    # Summary
    print(f"\n{'=' * 80}")
    print(f"BATCH PROCESSING COMPLETE")
    print(f"{'=' * 80}")
    print(f"✅ Successful: {len(successful)}/{len(COMMANDS)}")
    for cmd_num in successful:
        print(f"   ✓ Command {cmd_num}")
    
    if failed:
        print(f"\n❌ Failed: {len(failed)}/{len(COMMANDS)}")
        for cmd_num, reason in failed:
            print(f"   ✗ Command {cmd_num}: {reason}")
    
    print(f"{'=' * 80}\n")
    
    # Shutdown PC if all commands completed successfully
    if not failed:
        print("\n✅ All commands completed successfully!")
        print("🖥️  Preparing to shutdown PC...")
        shutdown_pc(delay_seconds=60)  # 60 second countdown before shutdown
    else:
        print(f"\n⚠️  Some commands failed. PC shutdown cancelled.")
        sys.exit(1)


if __name__ == "__main__":
    main()
