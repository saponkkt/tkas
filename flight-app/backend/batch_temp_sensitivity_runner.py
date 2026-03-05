"""
Batch runner for temp_sensitivity_pipeline.py

This script runs multiple temperature sensitivity analyses sequentially.
Edit the COMMANDS list below to configure your temperature sensitivity runs.

Usage:
    python batch_temp_sensitivity_runner.py

Temperature sensitivity values:
    - Positive values: Warmer baseline temperature (e.g., +5 means 278.15 K)
    - Negative values: Cooler baseline temperature (e.g., -5 means 268.15 K)
    - The first N rows (N = abs(sensitivity_value)) use constant temperature
    - Remaining rows apply ISA lapse rate: T = T0 - 0.0065 * altitude_m

Command format:
    ['python', 'temp_sensitivity_pipeline.py', input_folder, output_folder, sensitivity_value, aircraft_type]
"""

import subprocess
import sys
import os
import time
from pathlib import Path

# Configure your temperature sensitivity commands here
# Format: ['python', 'temp_sensitivity_pipeline.py', input_folder, output_folder, sensitivity_value, aircraft_type]
COMMANDS = [
    # Example temperature sensitivity runs
    # Set base temperature to 273.15 + 10 = 283.15 K
    ['python', 'temp_sensitivity_pipeline.py',
     r'C:\Users\User\Desktop\Validation_TAS\CSV\Sent Temp\DMK-HDY', 
     r'C:\Users\User\Desktop\Validation_TAS\TAS\Sent Temp\DMK-HDY\temp_sens_+10', 
     '10', '737'],

    # Set base temperature to 273.15 + 15 = 288.15 K
    ['python', 'temp_sensitivity_pipeline.py', 
     r'C:\Users\User\Desktop\Validation_TAS\CSV\Sent Temp\DMK-HDY', 
     r'C:\Users\User\Desktop\Validation_TAS\TAS\Sent Temp\DMK-HDY\temp_sens_+15', 
     '15', '737'],

    # Set base temperature to 273.15 + 20 = 293.15 K
    ['python', 'temp_sensitivity_pipeline.py', 
     r'C:\Users\User\Desktop\Validation_TAS\CSV\Sent Temp\DMK-HDY', 
     r'C:\Users\User\Desktop\Validation_TAS\TAS\Sent Temp\DMK-HDY\temp_sens_+20', 
     '20', '737'],

    # Set base temperature to 273.15 + 25 = 298.15 K
    ['python', 'temp_sensitivity_pipeline.py', 
     r'C:\Users\User\Desktop\Validation_TAS\CSV\Sent Temp\DMK-HDY', 
     r'C:\Users\User\Desktop\Validation_TAS\TAS\Sent Temp\DMK-HDY\temp_sens_+25', 
     '25', '737'],

    # Set base temperature to 273.15 + 5 = 278.15 K
    ['python', 'temp_sensitivity_pipeline.py', 
     r'C:\Users\User\Desktop\Validation_TAS\CSV\Sent Temp\DMK-PHS', 
     r'C:\Users\User\Desktop\Validation_TAS\TAS\Sent Temp\DMK-PHS\temp_sens_+5', 
     '5', '737'],

    # Set base temperature to 273.15 + 10 = 283.15 K
    ['python', 'temp_sensitivity_pipeline.py',
     r'C:\Users\User\Desktop\Validation_TAS\CSV\Sent Temp\DMK-PHS', 
     r'C:\Users\User\Desktop\Validation_TAS\TAS\Sent Temp\DMK-PHS\temp_sens_+10', 
     '10', '737'],

    # Set base temperature to 273.15 + 15 = 288.15 K
    ['python', 'temp_sensitivity_pipeline.py', 
     r'C:\Users\User\Desktop\Validation_TAS\CSV\Sent Temp\DMK-PHS', 
     r'C:\Users\User\Desktop\Validation_TAS\TAS\Sent Temp\DMK-PHS\temp_sens_+15', 
     '15', '737'],

    # Set base temperature to 273.15 + 20 = 293.15 K
    ['python', 'temp_sensitivity_pipeline.py', 
     r'C:\Users\User\Desktop\Validation_TAS\CSV\Sent Temp\DMK-PHS', 
     r'C:\Users\User\Desktop\Validation_TAS\TAS\Sent Temp\DMK-PHS\temp_sens_+20', 
     '20', '737'],

    # Set base temperature to 273.15 + 25 = 298.15 K
    ['python', 'temp_sensitivity_pipeline.py', 
     r'C:\Users\User\Desktop\Validation_TAS\CSV\Sent Temp\DMK-PHS', 
     r'C:\Users\User\Desktop\Validation_TAS\TAS\Sent Temp\DMK-PHS\temp_sens_+25', 
     '25', '737'],

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
    print(f"Temperature Sensitivity Batch Runner")
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
