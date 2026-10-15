from setuptools import setup, find_packages

setup(
    name="mayacardio-pipeline",
    version="1.0.0",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "pipeline = pipeline_app.cli:main",
            "biosignal-mock = biosignal_mock.server:main",
            "check_ground_truth = biosignal_mock.ground_truth:main",
        ],
    },
)
