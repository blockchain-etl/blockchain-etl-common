import os

from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


long_description = read('README.md') if os.path.isfile("README.md") else ""

setup(
    name='blockchain-etl-common',
    version='1.4.0',
    author='Evgeny Medvedev',
    author_email='evge.medvedev@gmail.com',
    description='Common utils for Blockchain ETL',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/blockchain-etl/blockchain-etl-common',
    packages=find_packages(exclude=['schemas', 'tests']),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ],
    keywords='blockchain ethereum bitcoin etl',
    python_requires='>=3.5.3,<4',
    install_requires=[
    ],
    extras_require={
        'streaming': [
            'timeout-decorator==0.4.1',
            'google-cloud-pubsub==0.39.1'
        ]
    },
    project_urls={
        'Bug Reports': 'https://github.com/blockchain-etl/blockchain-etl-common/issues',
        'Chat': 'https://gitter.im/ethereum-etl/Lobby',
        'Source': 'https://github.com/blockchain-etl/blockchain-etl-common',
    },
)
