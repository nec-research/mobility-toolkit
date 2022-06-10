import setuptools

setuptools.setup(
    name="mtk_common",
    version="0.0.1",
    author="Jonathan Fuerst",
    author_email="jonathan.fuerst@neclab.eu",
    description="Mobility Toolkit Common Library",
    url="https://github.com/nec-research/mobility-toolkit",
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)
