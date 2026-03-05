"""
Batch runner for sensitivity_TAS=GS.py

This script runs multiple TAS=GS sensitivity analyses sequentially.
Edit the COMMANDS list below to configure your sensitivity runs.

Usage:
    python batch_sensitivity_TAS=GS_runner.py

TAS=GS sensitivity mode:
    - Uses ground_speed directly as TAS_kt instead of computing from ERA5
    - Still computes weather data (Temperature, wind) from ERA5
    - Allows comparison of pipeline output using different TAS definitions

Command format:
    ['python', 'sensitivity_TAS=GS.py', input_folder, output_folder, aircraft_type]
"""

import subprocess
import sys
import os
import time
from pathlib import Path

# Configure your TAS=GS sensitivity commands here
# Format: ['python', 'sensitivity_TAS=GS.py', input_folder, output_folder, aircraft_type]
COMMANDS = [
    # Example: Process HDY-UTH with 737 using ground_speed as TAS_kt
    ['python', 'sensitivity_TAS=GS.py',
     r'C:\Users\User\Desktop\Validation_TAS\CSV\Sent Temp\HDY-UTH', 
     r'C:\Users\User\Desktop\Validation_TAS\TAS\Sent Temp\HDY-UTH\TAS_GS_sensitivity', 
     '737'],

    # Example: Process UTH-HDY with 737 using ground_speed as TAS_kt
    ['python', 'sensitivity_TAS=GS.py',
     r'C:\Users\User\Desktop\Validation_TAS\CSV\Sent Temp\UTH-HDY', 
     r'C:\Users\User\Desktop\Validation_TAS\TAS\Sent Temp\UTH-HDY\TAS_GS_sensitivity', 
     '737'],
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
        time.sleep(delay_seconds)
        os.system(f"shutdown /s /t 0")
    except KeyboardInterrupt:
        print("\n✓ Shutdown cancelled")


def run_commands():
    """Run all configured commands sequentially."""
    print(f"\n{'=' * 80}")
    print(f"TAS=GS Sensitivity Batch Runner")
    print(f"{'=' * 80}")
    print(f"Total commands to run: {len(COMMANDS)}\n")
    
    successful = 0
    failed = 0
    failed_commands = []
    
    for idx, cmd in enumerate(COMMANDS, 1):
        try:
            print(f"\n[{idx}/{len(COMMANDS)}] Running command:")
            print(f"  {' '.join(cmd)}")
            print(f"{'-' * 80}")
            
            start_time = time.time()
            result = subprocess.run(cmd, check=True)
            elapsed = time.time() - start_time
            
            print(f"{'-' * 80}")
            print(f"✓ Command {idx} completed successfully in {elapsed:.1f}s\n")
            successful += 1
            
        except subprocess.CalledProcessError as e:
            print(f"\n✗ Command {idx} failed with return code {e.returncode}")
            failed += 1
            failed_commands.append((idx, cmd))
        except Exception as e:
            print(f"\n✗ Command {idx} failed with error: {e}")
            failed += 1
            failed_commands.append((idx, cmd))
    
    # Print summary
    print(f"\n{'=' * 80}")
    print(f"SUMMARY")
    print(f"{'=' * 80}")
    print(f"Total: {len(COMMANDS)}")
    print(f"✓ Successful: {successful}")
    print(f"✗ Failed: {failed}")
    
    if failed_commands:
        print(f"\nFailed commands:")
        for idx, cmd in failed_commands:
            print(f"  [{idx}] {' '.join(cmd)}")
    
    print(f"{'=' * 80}\n")
    
    return failed == 0


if __name__ == "__main__":
    try:
        all_success = run_commands()
        
        # Optional: shutdown PC on completion
        # Uncomment the line below to enable auto-shutdown after running all commands
        # shutdown_pc(delay_seconds=60)
        
        sys.exit(0 if all_success else 1)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Batch execution cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        sys.exit(1)
