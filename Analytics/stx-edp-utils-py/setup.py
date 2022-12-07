from setuptools import setup

setup(
    name='edputils',
    packages=['edputils'],
	install_requires=[
          'requests-auth',
		  'requests',
		  'pyhive',
		  'cloudpathlib[s3]',
		  'pyarrow',
		  'boto3',
		  'pandas'
      ],
    description='download table from s3',
    version='2.4',
    keywords=['pip','edputils']
    )
