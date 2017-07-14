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

with open('requirements.txt') as f:
    required = f.read().splitlines()

root = os.path.join(os.path.dirname(__file__), "factorytx")
folders_and_files = []
for dirpath, _, filenames in os.walk(root):
    for filename in filenames:
        if filename.endswith(".schema") or filename.endswith(".conf"):
            abspath = os.path.join(dirpath, filename)
            relpath = os.path.relpath(abspath, root)
            folders_and_files.append(relpath)


print("Appending the files %s to be listed", folders_and_files)
package_data = {'factorytx': folders_and_files}

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
    package_data=package_data,
    zip_safe=False,
    # data_files=[ ],
    entry_points=entry_points,
    install_requires=required,
    tests_require=['pytest'],
    test_suite='tests'
)
