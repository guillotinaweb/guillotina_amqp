from setuptools import find_packages
from setuptools import setup


try:
    README = open("README.rst").read()
except IOError:
    README = None

setup(
    name="guillotina_amqp",
    version="5.0.26",
    description="Integrate amqp into guillotina",
    long_description=README,
    install_requires=[
        "guillotina>=5.3.55.dev0,<6",
        "aioamqp",
        "lru-dict",
        "aioredis",
        "backoff",
        "typing_extensions",
    ],
    author="Nathan Van Gheem",
    author_email="vangheem@gmail.com",
    url="https://github.com/guillotinaweb/guillotina_amqp",
    packages=find_packages(exclude=["demo"]),
    include_package_data=True,
    package_data={"": ["*.txt", "*.rst"], "guillotina_amqp": ["py.typed"]},
    tests_require=["pytest"],
    extras_require={
        "test": [
            "asynctest==0.13.0",
            "pytest>=4.6.0,<6.1.0",
            "docker",
            "psycopg2-binary",
            "pytest-asyncio==0.10.0",
            "pytest-cov>=2.6.1,<=2.9.0",
            "coverage>=4.4",
            "pytest-docker-fixtures[rabbitmq]==1.3.5",
            "prometheus-client>=0.8.0",
        ]
    },
    license="BSD",
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3.7",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Internet :: WWW/HTTP",
        "Intended Audience :: Developers",
    ],
    entry_points={},
)
