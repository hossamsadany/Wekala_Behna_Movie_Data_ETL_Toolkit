import os

# List of required packages
required_packages = [
    'pandas',
    'numpy',
    'pyarabic',
    'sqlalchemy',
    'PyQt5',
    'logging'
    # Add any other required packages here
]

# Write the requirements to a file
with open('requirements.txt', 'w') as f:
    f.write('\n'.join(required_packages))

# Install the packages
for package in required_packages:
    os.system(f'pip install {package}')

print('Packages installed successfully.')
