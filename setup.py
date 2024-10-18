from setuptools import setup

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

PROJECT_NAME = "queue-processor"

setup(
    name=PROJECT_NAME,
    packages=["queue_processor"],
    package_dir={"": "src"},
    version="0.8",
    url="https://github.com/huridocs/queue-processor",
    author="HURIDOCS",
    description="Manage queues on Uwazi services",
    install_requires=requirements,
    setup_requieres=requirements,
)
