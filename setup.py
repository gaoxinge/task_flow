import pkg_resources
from setuptools import find_packages, setup

with open("requirements.txt") as f:
    install_requires = [str(install_require) for install_require in pkg_resources.parse_requirements(f.read())]

setup(
    name="task_flow",
    author="gaoxinge",
    version="0.0.1",
    description="task_flow",
    url="https://github.com/gaoxinge/task_flow",
    packages=find_packages(),
    install_requires=install_requires,
    extras_require={},
    include_package_data=True,
)
