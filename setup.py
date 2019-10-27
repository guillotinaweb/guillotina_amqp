from setuptools import find_packages
from setuptools import setup


try:
    README = open('README.rst').read()
except IOError:
    README = None

setup(
    name='guillotina_amqp',
    version='5.0.4',
    description='Integrate amqp into guillotina',
    long_description=README,
    install_requires=[
        'guillotina>=5.0.0.dev0,<6',
        'aioamqp',
        'lru-dict',
        'aioredis'
    ],
    author='Nathan Van Gheem',
    author_email='vangheem@gmail.com',
    url='https://github.com/guillotinaweb/guillotina_amqp',
    packages=find_packages(exclude=['demo']),
    include_package_data=True,
    tests_require=[
        'pytest'
    ],
    extras_require={
        'test': [
            'pytest>=4.1.1',
            'docker',
            'backoff',
            'psycopg2-binary',
            'pytest-asyncio>=0.8.0',
            'pytest-cov>=2.6.1',
            'coverage>=4.4',
            'pytest-docker-fixtures[rabbitmq]==1.3.5',
        ]
    },
    license='BSD',
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Internet :: WWW/HTTP',
        'Intended Audience :: Developers',
    ],
    entry_points={
    }
)
