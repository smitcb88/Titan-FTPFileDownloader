from setuptools import setup


setup(
    name="FTP File Downloader",
    version="0.1",
    author="Adam Cunnington",
    author_email="adam.cunnington@wmglobal.com",
    license="MIT",
    py_modules=["ftpfiledownloader"],
    install_requires=["click"],
    entry_points={"console_scripts": ["ftpfiledownloader = ftpfiledownloader:main"]}
)
