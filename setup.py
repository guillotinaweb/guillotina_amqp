from setuptools import find_packages
from setuptools import setup


try:
    README = open("README.rst").read()
except IOError:
    README = None

setup(
    name="guillotina_amqp",
    version="6.0.0",
    description="Integrate amqp into guillotina",
    long_description=README,
    install_requires=[
        "guillotina>=6.0.0<7",
        "aiohttp>=3.6.0,<4.0.0",
        "aioamqp",
        "lru-dict",
        "aioredis",
        "backoff",
        "mypy-extensions",
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
            "asynctestL=0.13.0",
            "pytest>=3.8.0<=5.0.0",
            "docker",
            "psycopg2-binary",
            "pytest-asyncio==0.10.0",
            "pytest-cov>=2.6.1",
            "pluggy<=0.12",
            "coverage>=4.4",
            "pytest-docker-fixtures[rabbitmq]==1.3.5",
            "async_asgi_testclient==1.4.4",
            "prometheus-client",
        ]
    },
    license="BSD",
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Internet :: WWW/HTTP",
        "Intended Audience :: Developers",
    ],
    entry_points={},
)
