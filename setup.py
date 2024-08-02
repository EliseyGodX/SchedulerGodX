from setuptools import setup, find_packages
import os
import re

def read_version():
    with open(os.path.join('schedulergodx', '__init__.py')) as f:
        content = f.read()
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", content, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string")

setup(
    name='schedulergodx',
    version=read_version(),
    packages=find_packages(),
    install_requires=[
        'dill==0.3.8',
        'pika==1.3.2',
        'SQLAlchemy==2.0.31',
        'ulid==1.1'
    ],
    author='EliseyGodX',
    description='A simple task manager to run functions',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/EliseyGodX/SchedulerGodX',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.11',
)