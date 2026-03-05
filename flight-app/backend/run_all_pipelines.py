import subprocess
import sys

commands = [
    ['python', 'process_adsb_pipeline.py', r'C:\Users\User\Desktop\Validation_TAS\CSV\Lion Air\HS-LGI\วันที่ 1 กันยายน 2568', r'C:\Users\User\Desktop\Validation_TAS\TAS\Lion Air\15000_25000\0.75\HS-LGI\วันที่ 1 กันยายน 2568', '737'],
    ['python', 'process_adsb_pipeline.py', r'C:\Users\User\Desktop\Validation_TAS\CSV\Lion Air\HS-LGI\วันที่ 2 กันยายน 2568', r'C:\Users\User\Desktop\Validation_TAS\TAS\Lion Air\15000_25000\0.75\HS-LGI\วันที่ 2 กันยายน 2568', '737'],
    ['python', 'process_adsb_pipeline.py', r'C:\Users\User\Desktop\Validation_TAS\CSV\Lion Air\HS-LGS\วันที่ 1 กันยายน 2568', r'C:\Users\User\Desktop\Validation_TAS\TAS\Lion Air\15000_25000\0.75\HS-LGS\วันที่ 1 กันยายน 2568', '737'],
    ['python', 'process_adsb_pipeline.py', r'C:\Users\User\Desktop\Validation_TAS\CSV\Lion Air\HS-LGS\วันที่ 2 กันยายน 2568', r'C:\Users\User\Desktop\Validation_TAS\TAS\Lion Air\15000_25000\0.75\HS-LGS\วันที่ 2 กันยายน 2568', '737'],
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
