import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cdsspy",                     # This is the name of the package
    version="1.2.71",                        # The initial release version
    author="Angus Watters",                     # Full name of the author
    description="Provides Python functions for discovering and requesting data from the CDSS REST API.",
    long_description=long_description,      # Long description read from the the readme file
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    # packages=setuptools.find_packages(exclude=["cdsspy.egg_info"]),    # List of all python modules to be installed
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],                                      # Information to filter the project on PyPi website
    python_requires='>=3.6',                # Minimum version requirement of the package
    # py_modules=["cdsspy"],             # Name of the python package
    # package_dir={'':'cdsspy/cdsspy'},     # Directory of the source code of the package
    install_requires=['pandas', 'datetime', 'requests', 'geopandas', 'shapely', 'pyproj']
)