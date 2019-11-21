from setuptools import setup

setup(
    name='py-tools',
    version='1.0.0',
    packages=['py_base'],
    url='https://github.com/wobeng/flask-base',
    license='',
    author='wobeng',
    author_email='wobeng@yblew.com',
    description='python tools',
    install_requires=[
        'pytz',
        'simplejson',
        'pynamodb @ git+https://github.com/pynamodb/PynamoDB#egg=pynamodb'
    ]
)
