from setuptools import setup, find_packages
import os, sys, glob, fnmatch

entry_names = ['dataplugins', 'transforms', 'tx', 'filters']
sub_entry_names = ['pollingservices', 'parsers']
entrypoints = [os.path.join(os.path.dirname(__file__), 'factorytx', 'components', x, x + '.ini') \
               for x in entry_names]
entrypoints += [os.path.join(os.path.dirname(__file__), 'factorytx/components/dataplugins', x, x + '.ini') \
               for x in sub_entry_names]

entry_points = '[console_scripts]\nfactorytx=factorytx.cli:main\n'
for entry in entrypoints:
    with open(entry) as fp:
        entry_points += fp.read()

folders_and_files = []
ff = []
for entry in entry_names:
    ff += glob.glob(os.path.join("factorytx/components", entry, "*/schemas/*.schema"))
for f in ff:
    if f[0:11] == "factorytx/components/":
        folders_and_files.append(f[22:])
    else:
        folders_and_files.append(f)

package_data = {
    'factorytx': folders_and_files
}

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name="factorytx",
    version='0.1.0',
    # download_url='https://github.com/sightmachine/factorytx/tarball/master',
    description = 'The factorytx framework allows easy intake, processing, and forwarding of unstructured data.',
    long_description = 'The factorytx framework allows easy intake, processing, and forwarding of unstructured data.',
    # classifiers=[ ],
    # keywords='',
    author='Sight Machine',
    author_email='support@sightmachine.com',
    url='https://github.com/sightmachine/factorytx',
    license='',
    packages=find_packages(exclude=["tests.*", "tests"]),
    zip_safe=False,
    package_data=package_data,
    # data_files=[ ],
    entry_points=entry_points,
    install_requires=required,
    tests_require=['pytest'],
    test_suite='tests'
)
