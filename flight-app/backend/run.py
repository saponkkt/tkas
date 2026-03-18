import subprocess
import sys

commands = [
    ['python', 'cleaning.py', r'C:\Users\User\Desktop\Uni\SKATS\Test\SL506_3c655f43', r'C:\Users\User\Desktop\Uni\SKATS\Test\cleaning_SL506_3c655f43'],
    
]
for i, cmd in enumerate(commands, 1):
    print(f"\n{'='*80}")
    print(f"Running process {i}/{len(commands)}...")
    print(f"{'='*80}")
    print(f"Command: {' '.join(cmd)}")
    
    result = subprocess.run(cmd)
    
    if result.returncode != 0:
        print(f"\n❌ Error in process {i}. Stopping.")
        sys.exit(1)
    
    print(f"✅ Process {i} completed successfully.")

print(f"\n{'='*80}")
print("✅ All processes completed successfully!")
print(f"{'='*80}")