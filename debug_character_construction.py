#!/usr/bin/env python3

import unicodedata

# Compare the two ways of creating the character
method1 = f"cafe{'e' + chr(0x0301)}.txt"  # Method used in fixture
method2 = f'cafe{chr(0x0301)}.txt'  # Method used in simple test

print(f'Method 1: {method1!r} (len={len(method1)})')
print(f'Method 2: {method2!r} (len={len(method2)})')
print(f'Equal: {method1 == method2}')
print()

print('Method 1 normalized:', unicodedata.normalize('NFC', method1))
print('Method 2 normalized:', unicodedata.normalize('NFC', method2))
print()

# Check if method1 is already NFC
print('Method 1 is NFC:', method1 == unicodedata.normalize('NFC', method1))
print('Method 2 is NFC:', method2 == unicodedata.normalize('NFC', method2))
print()

# Let's see the actual Unicode code points
print('Method 1 code points:', [ord(c) for c in method1])
print('Method 2 code points:', [ord(c) for c in method2])