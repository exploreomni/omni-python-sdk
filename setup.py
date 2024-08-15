from setuptools import setup, find_packages

setup(
	name='omni_python_sdk',
	version='0.1.0',
	description='A Python SDK for Omni API',
	long_description=open('README.md').read(),
	long_description_content_type='text/markdown',
	author='Jamie Davidson',
	author_email='jamie@omni.co',
	url='https://github.com/yourusername/omni-python-sdk',
	packages=find_packages(),
	install_requires=[
		'requests',
		'pyarrow',
		'ndjson',
		'pandas',
		'matplotlib',
		'statsmodels'
	],
	classifiers=[
		'Programming Language :: Python :: 3',
		'License :: OSI Approved :: MIT License',
		'Operating System :: OS Independent',
	],
	python_requires='>=3.6',
)