import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sqlitemanager",
    version="0.5.2",
    author="Michael-Yongshi",
    author_email="4registration@outlook.com",
    description="Handler object for easy sqlite manipulation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Michael-Yongshi/SQLiteManager",
    packages=setuptools.find_packages(),
    install_requires=[
        'sqlite3',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.5',
)