#!/usr/bin/env python3
"""Test the SL792 fixes"""

import pandas as pd
from flight_phase import detect_flight_phase

# Load the file
df = pd.read_excel('SL792_3c010e9c_Boeing_Cubic_Cleaned_mistake.xlsx')

# Run detection
result = detect_flight_phase(df)

# Check the specific problematic rows
print('Testing fixes...')
print('=' * 70)

# Issue 1: Row 1667
print('\n[ISSUE 1] Row 1667 (Excel row): altitude = 33075, ROCD = 3.925')
print(f'  Before: Climb')
print(f'  After:  {result.iloc[1666]["flight_phase"]}')
print(f'  Status: {"✓ FIXED" if result.iloc[1666]["flight_phase"] == "Cruise" else "✗ STILL BROKEN"}')

# Issue 2: Rows 1669-1702
print('\n[ISSUE 2] Rows 1669-1702: altitude from 33073.44 to 33047.66, ROCD = -0.238')
subset = result.iloc[1668:1702]
descent_count = (subset['flight_phase'] == 'Descent').sum()
cruise_count = (subset['flight_phase'] == 'Cruise').sum()
print(f'  Total rows: 34')
print(f'  Descent: {descent_count}')
print(f'  Cruise:  {cruise_count}')
print(f'  Status: {"✓ FIXED" if cruise_count == 34 else "✗ STILL BROKEN" if descent_count > 0 else "✓ PARTIALLY FIXED"}')

# Show sample data
print('\n  Sample rows (1668-1675):')
print('  Row | Altitude | ROCD | Phase')
print('  ' + '-' * 40)
for i in range(1668, 1676):
    alt = result.iloc[i]['altitude']
    rocd = result.iloc[i]['ROCD_m/s']
    phase = result.iloc[i]['flight_phase']
    print(f'  {i+1:4d} | {alt:8.2f} | {rocd:7.4f} | {phase}')

print('\n  Sample rows (1698-1705):')
print('  Row | Altitude | ROCD | Phase')
print('  ' + '-' * 40)
for i in range(1698, 1706):
    alt = result.iloc[i]['altitude']
    rocd = result.iloc[i]['ROCD_m/s']
    phase = result.iloc[i]['flight_phase']
    print(f'  {i+1:4d} | {alt:8.2f} | {rocd:7.4f} | {phase}')
